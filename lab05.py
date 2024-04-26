import sqlalchemy
import pandas as pd
import datetime
import redis
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.hooks.base_hook import BaseHook


# Get the connection object
ch_conn = BaseHook.get_connection('clickhouse')
pg_conn = BaseHook.get_connection('postgres')
redis_conn = BaseHook.get_connection('redis')


DEFAULT_ARGS = {"owner": "newprolab"}


@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",
    start_date=pendulum.datetime(2020, 11, 22),
    catchup=False,
)
def lab05():
    @task
    def metrics_count():
        context = get_current_context()
        now, start, end = context["logical_date"], context["data_interval_start"], context["data_interval_end"]
        start_ts = datetime.datetime.fromisoformat(str(start)) \
                .strftime('%Y-%m-%d %H:%M:%S')
        end_ts = datetime.datetime.fromisoformat(str(end)) \
                .strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"start with '{start_ts}'")
        print(f"end with '{end_ts}'")
        # DB connections 
        engine_ch = sqlalchemy.create_engine(f'clickhouse+native://{ch_conn.login}:@{ch_conn.host}:{ch_conn.port}/{ch_conn.schema}')
        engine_pg = sqlalchemy.create_engine(f'postgresql://{pg_conn.login}:{pg_conn.password}@{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}')

        clickstream_query = f"""
            select 
                fromUnixTimestamp(toInt64(`timestamp`)) as time_dt,
                toStartOfHour(time_dt) as time_hour,
                toUUID(splitByChar(':', userId)[2]) as user_id,
                toUUID(splitByChar(':', itemId)[2]) as sku_id,
                ROW_NUMBER() over() as row_num,
                    *
            from evgenii_luchinskii_lab05_insert elli
            where time_dt between '{str(start_ts)}' and '{str(end_ts)}'
            FORMAT TabSeparatedWithNamesAndTypes;
        """
        
        sku_info_query = """
            with clear_cat as (
            select id,
                cat,
                parent_id,
                lvl
            from (
                select id,
                    cat,
                    parent_id,
                    row_number() over (partition by cat order by id desc) as rn_cat,
                    case when cat = 0 then 0 else LENGTH(CAST(cat AS VARCHAR)) end as lvl
                from public.category_tree ct 
            ) as sq
            where rn_cat = 1),
            cat_tree as (select lvl_1.cat as cat_lvl_1,
                lvl_2.cat as cat_lvl_2,
                lvl_3.cat as cat_lvl_3,
                lvl_4.cat as cat_lvl_4
            from clear_cat as lvl_4
            left join (select *
            from clear_cat
            where lvl=3) as lvl_3
            on lvl_4.parent_id = lvl_3.id
            left join (select *
            from clear_cat
            where lvl=2) as lvl_2
            on lvl_3.parent_id = lvl_2.id
            left join (select *
            from clear_cat
            where lvl=1) as lvl_1
            on lvl_2.parent_id = lvl_1.id
            where lvl_4.lvl = 4)
            select distinct id,
                cat,
                ctr.cat_lvl_2,
                ctr.cat_lvl_3,
                sku_id
            from sku_cat sc 
            left join cat_tree as ctr
            on (sc.cat = ctr.cat_lvl_3 or sc.cat = ctr.cat_lvl_4);
        """
        
        clickstream_df = pd.read_sql_query(clickstream_query, engine_ch)
        sku_info_df = pd.read_sql_query(sku_info_query, engine_pg)
    
        cs_sku_df = clickstream_df.merge(sku_info_df, 
                                         how='left', 
                                         on=['sku_id']) \
                        .sort_values('timestamp')
    
        add_cat2_df = cs_sku_df[cs_sku_df.action == 'favAdd'] \
            .groupby(['time_hour', 'cat_lvl_2'], as_index=False) \
            .agg(added_qty=('user_id', 'count'),
                 first_add_ts=('timestamp', 'min')) \
            .sort_values(['time_hour', 'added_qty', 'first_add_ts'], 
                         ascending=[True, False, True])
        add_cat2_df.time_hour = pd.to_datetime(add_cat2_df.time_hour) 
        hours_list = add_cat2_df.time_hour.dt.strftime('%Y-%m-%d %H:%M:%S').unique().tolist()
        
        # Connect to Redis
        redis_client = redis.Redis(host=redis_conn.host, 
                                   port=redis_conn.port, 
                                   db=0)
        
        # Define the set name and elements to add
        for hour in hours_list:
            rel_hour_df = add_cat2_df[add_cat2_df.time_hour == hour].head(5)
            cat_top_list = rel_hour_df.cat_lvl_2.astype(str).tolist()
            set_name = f'fav:level2:{datetime.datetime.strptime(hour, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d:%H")}h:top5'
            elements = cat_top_list
            # Add elements to the set in Redis
            redis_client.sadd(set_name, *elements)
        
        print("Non personalized metrics added to the set in Redis successfully.")
        
        ## Part 2
        ### Favourite
        
        fav_act_df = cs_sku_df[cs_sku_df.action.isin(['favAdd', 'favRemove'])]
        
        def score_setter(action_list):
            score = 0
            if 'favAdd' in action_list:
                first_add_pos = action_list.index('favAdd')
                for i in range(len(action_list)):
                    if action_list[i] == 'favAdd':
                        score += 1
                    elif action_list[i] == 'favRemove' and i > first_add_pos:
                        score += -1
            return score
        
        add_ts_df = fav_act_df[fav_act_df.action == 'favAdd'] \
            .groupby(['time_hour', 'user_id', 'cat_lvl_3'], as_index=False) \
            .agg(add_ts=('timestamp', 'min'))
        
        agg_fav_df = fav_act_df \
            .groupby(['time_hour', 'user_id', 'cat_lvl_3']) \
            .agg({'action': list, 'timestamp': 'min', 'row_num':'min'}) \
            .reset_index()
        
        agg_fav_df = agg_fav_df.merge(add_ts_df, 
                                      how='left', 
                                      on=['time_hour', 'user_id', 'cat_lvl_3'])
        
        agg_fav_df['score'] = agg_fav_df['action'].apply(lambda x: score_setter(x))
        
        agg_fav_df = agg_fav_df[(agg_fav_df.score >= 0) 
                                & (agg_fav_df.add_ts.notna())] \
            .sort_values(['score', 'add_ts', 'row_num'], 
                         ascending=[False, True, True])
        
        final_fav_df = agg_fav_df.groupby(['time_hour', 'user_id']) \
            .agg({'cat_lvl_3': list}).reset_index()
        
        ### itemView
        view_act_df = cs_sku_df[cs_sku_df.action == 'itemView']
        view_agg_df = view_act_df.groupby(['time_hour', 'user_id', 'cat_lvl_3'], 
                                          as_index=False) \
            .agg(views_qty=('sku_id', 'count'), 
                 first_ts=('timestamp', 'min'), 
                 row_num=('row_num', 'min')) \
            .sort_values(['time_hour', 'user_id', 'views_qty', 'first_ts', 'row_num'], 
                         ascending=[True, True, False, True, True])
        final_view_df = view_agg_df.groupby(['time_hour', 'user_id']) \
            .agg({'cat_lvl_3': list}).reset_index()
        
        for index, row in final_fav_df.iterrows():
            # Example data
            redis_key = f"user:{row['user_id']}:fav:level3:{row['time_hour'].strftime('%Y-%m-%d:%H')}h:top5"
            data = [str(num) for num in row['cat_lvl_3'][:5]]
            # Use ZADD command to add data to the sorted set
            score_value_pairs = {item: i for i, item in enumerate(data)}
        
            # Use ZADD command to add data to the sorted set
            redis_client.zadd(redis_key, score_value_pairs)
        
        print("Favourites personalized metrcisadded to the set in Redis successfully.")
        
        for index, row in final_view_df.iterrows():
            # Example data
            redis_key = f"user:{row['user_id']}:view:level3:{row['time_hour'].strftime('%Y-%m-%d:%H')}h:top10"
            data = [str(num) for num in row['cat_lvl_3'][:10]]
    
            # Use ZADD command to add data to the sorted set
            score_value_pairs = {item: i for i, item in enumerate(data)}
        
            # Use ZADD command to add data to the sorted set
            redis_client.zadd(redis_key, score_value_pairs)
        
        print("Views personalized metrics added to the set in Redis successfully.")
        
    metrics_count()
    
actual_dag = lab05()
