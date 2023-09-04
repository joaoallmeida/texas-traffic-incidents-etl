from airflow.models import Variable
from pyspark.sql import SparkSession
from datetime import datetime

import clickhouse_connect as ch

HOST = Variable.get('clickhouse_host')

def ch_client():

    client = ch.get_client(
        host=HOST
        ,username='devUser'
        ,password='dev@2023'
        ,port=8123
    )

    return client

def load(spark:SparkSession):

    BUCKET= Variable.get('bucket')

    database_qry = "CREATE DATABASE IF NOT EXISTS incidents"
    select_qry = "SELECT COUNT(*) FROM incidents.traffic"
    create_qry = """
        CREATE TABLE incidents.traffic
        (
            traffic_report_id String ,
            published_date DateTime,
            issue_reported String,
            latitude Float64,
            longitude Float64,
            address String,
            traffic_report_status String,
            traffic_report_status_date_time DateTime,
            year UInt32,
            month String
        )
        ENGINE = MergeTree()
        PRIMARY KEY (traffic_report_id)"""
    
    spark = (SparkSession 
                .builder 
                .appName("myApp") 
                .master("spark://spark-incidents:7077")
                .getOrCreate()
    )

    client = ch_client()

    # Create Database
    client.command("DROP DATABASE IF EXISTS incidents")
    client.command(database_qry)

    # Create Table
    try:
        client.command('SHOW TABLE incidents.traffic')
    except Exception as e:
        if "doesn't exist" in str(e):
            client.command(create_qry)
        else:
            raise e
        
    df = spark.read.parquet(f's3a://{BUCKET}/refined/{datetime.now().strftime("%Y%m%d")}/incidents.parquet')
    print(df.columns)

    data = [list(row) for row in df.collect()]

    ic = client.create_insert_context('incidents.traffic', data=data)
    client.insert(context=ic)

    result = client.command(select_qry)
    print(result)

if __name__=="__main__":
    load()