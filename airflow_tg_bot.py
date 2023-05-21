import pandas as pd
import pandahouse
from datetime import date, timedelta, datetime
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import date, timedelta, datetime
from tokens import my_token
from con_password import my_password
chat_id = 812007021
token = my_token
bot = telegram.Bot(token=token)
default_args = {
    'owner': 'v.bogatov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 4, 21),
}
schedule_interval = '55 22 * * *'
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': my_password,
    'user': 'student',
    'database': 'simulator_20230320'}

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)    
def dag_lesson_6_bogatov():
    @task()
    def extract_1():
        query_1 = """SELECT 
                   user_id,
                   countIf(action = 'like') as likes,
                   countIf(action = 'view') as views,
                   min(age) as age,
                   min(gender) as gender,
                   min(os) as os
                   FROM simulator_20230320.feed_actions 
                   where 
                   toDate(time) = today() - 2
                   group by user_id 
                   """ #запрос 1
        df_likes_views = pandahouse.read_clickhouse(query_1, connection=connection)
        return df_likes_views
    @task
    def extract_2():
        query_2 = """select greatest(u_id, reciever_id) as user_id, messages_sent, 
    messages_received, users_sent, users_received from
    (SELECT 
                   user_id as u_id,
                   count(reciever_id) as messages_sent,
                   count(distinct reciever_id) as users_sent
                   from simulator_20230320.message_actions 
                   where 
                   toDate(time) = today() - 2
                   group by u_id) t1
                   full outer JOIN
                   (SELECT
                   reciever_id,
                   count(user_id) as messages_received,
                   count(distinct user_id) as users_received
                   from simulator_20230320.message_actions 
                   where 
                   toDate(time) = today() - 2
                   group by reciever_id) t2
                   on u_id = reciever_id""" #запрос 2
        df_sent_received = pandahouse.read_clickhouse(query_2, connection=connection)
        return df_sent_received
    
    @task
    def merge_task(df_likes_views, df_sent_received):
        merged_df = df_likes_views.merge(df_sent_received, how='outer')
        return merged_df #трансформ объединение
    
    @task
    def os_task(merged_df):
        df_os = merged_df.groupby(['os']).agg({'likes': 'sum', 'views': 'sum', 'messages_sent': 'sum', 'messages_received': 'sum', 'users_sent': 'sum', 'users_received': 'sum'})
        df_os['event_date'] = date.today() - timedelta(days=1)
        df_os['dimension'] = 'os'
        return df_os    
    
    @task
    def gender_task(merged_df):
        df_gender = merged_df.groupby(['gender']).agg({'likes': 'sum', 'views': 'sum', 'messages_sent': 'sum', 'messages_received': 'sum', 'users_sent': 'sum', 'users_received': 'sum'})
        df_gender['event_date'] = date.today() - timedelta(days=1)
        df_gender['dimension'] = 'gender'
        return df_gender
    
    @task
    def age_task(merged_df):
        df_age = merged_df.groupby(['age']).agg({'likes': 'sum', 'views': 'sum', 'messages_sent': 'sum', 'messages_received': 'sum', 'users_sent': 'sum', 'users_received': 'sum'})
        df_age['event_date'] = date.today() - timedelta(days=1)
        df_age['dimension'] = 'age'
        return df_age
    
    @task
    def final_concat(df_os, df_gender, df_age):
        general_df = pd.concat([df_os, df_gender, df_age])
        general_df.reset_index(inplace=True )
        general_df.rename(columns = {'index':'dimension_value'}, inplace=True)
        general_df.astype({'likes': 'int64', 'views': 'int64',
                  'messages_sent': 'int64', 'messages_received': 'int64',
                  'users_sent': 'int64', 'users_received': 'int64'})
        final_df = general_df.reindex(columns=['event_date', 'dimension', 'dimension_value',
                                      'views', 'likes', 'messages_received',
                                      'messages_sent', 'users_received', 'users_sent'])
        return final_df
    
    @task
    def sending(final_df):        
        file_object = io.StringIO()
        final_df.to_csv(file_object)
        file_object.name = 'test_file.csv'
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object)
    
    df_likes_views = extract_1()
    df_sent_received = extract_2()
    merged_df = merge_task(df_likes_views, df_sent_received)              
    df_os = os_task(merged_df)
    df_gender = gender_task(merged_df)
    df_age = age_task(merged_df)
    final_df = final_concat(df_os, df_gender, df_age)
    sending(final_df)
dag_lesson_6_bogatov = dag_lesson_6_bogatov()