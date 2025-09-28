from airflow import DAG
from datetime import datetime
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
# from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

import requests
import json


# Latitude and longitude for the desired location (Bangkok in this case)
# https://api.open-meteo.com/v1/forecast?latitude=13.75398&longitude=100.50144&hourly=temperature_2m
# https://api.open-meteo.com/v1/forecast?latitude=13.75398&longitude=100.50144&current_weather=true
LATITUDE = '13.75398'
LONGITUDE = '100.50144'
API_CONN_ID='open_meteo_api'
MYSQL_CONN_ID='my_mysql_conn'

default_args={
    'owner':'wrl_airflow',
    #'start_date':days_ago(1) #datetime(2025,9,1)
    'start_date': datetime.now() - timedelta(days=1)
}

## DAG
with DAG(dag_id='api-mysql-dag',
         default_args=default_args,
         # schedule_interval="@daily",
         schedule="@daily",
         catchup=False) as dags:
    
    @task()
    def extract_data():

        http_hook=HttpHook(http_conn_id=API_CONN_ID,method='GET')

        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        
        response=http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_data(weather_data):

        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data

    @task()
    def load_data(transformed_data):

        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()

    ## DAG Pipeline
    weather_data= extract_data()
    transformed_data=transform_data(weather_data)
    load_data(transformed_data)



