import requests
import json 
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago 
from airflow.providers.mysql.hooks.mysql import MySqlHook

mysql = MySqlHook(mysql_conn_id='mysql_db')
conn = mysql.get_conn()
cursor = conn.cursor() 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['emak.90@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

@dag(
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
)  

def taskflow_api_etl():
    @task
    def extract_task():
        context = get_current_context
        params = {
            'access_key': '3db82e8bd9684f0f48af4ea3918e5cfd',
            'limit' : 100,
            'flight_status' : 'active',
            'departure' : {'airport','timezone'}
            }
        
        api_result = requests.get('http://api.aviationstack.com/v1/flights', params)
        
        api_response = api_result.json()
            
        return api_response
    
    @task
    def transform_task(json_df):
            
        for flight in json_df['data']:
            if flight["departure"]["timezone"] == None:
                flight["departure"]["timezone"] = ''
            else:
                flight["departure"]["timezone"]=flight["departure"]["timezone"].replace("/"," - ")
            if flight["arrival"]["terminal"] == None:
                flight["arrival"]["terminal"] = ''
            else:
                flight["arrival"]["terminal"]=flight["departure"]["timezone"].replace("/"," - ")
                
            sql_query = ("""INSERT INTO test_fligoo.testdata (flight_date, flight_status, departure_airport, departure_timezone, 
                            arrival_airport, arrival_timezone, arrival_terminal, airline_name, flight_number) 
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s); """)
            sql_values = (flight["flight_date"],flight["flight_status"],flight["departure"]["airport"],flight["departure"]["timezone"],
                             flight["arrival"]["airport"],flight["arrival"]["timezone"],flight["arrival"]["terminal"],flight["airline"]["name"],flight["flight"]["number"])
   
            res = cursor.execute(sql_query,sql_values)
            print(res)     
            conn.commit()     
        
    df = extract_task()
    trf_df = transform_task(df) 
    
taskflow_api_etl_dag = taskflow_api_etl()