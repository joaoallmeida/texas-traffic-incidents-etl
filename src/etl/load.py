from airflow.models import Variable
from pyspark.sql import SparkSession
from datetime import datetime

import clickhouse_connect as ch


def ch_client():
    HOST = Variable.get('clickhouse_host')

    client = ch.get_client(
        host=HOST
        ,username='devUser'
        ,password='dev@2023'
        ,port=8123
    )

    return client

def load():

    BUCKET= Variable.get('bucket')
    TABLE_NAME = 'incidents.traffic_incidents_report'

    select_qry = f"SELECT COUNT(*) FROM {TABLE_NAME}"
    check_db = "SELECT COUNT(*) FROM system.databases WHERE name = 'incidents';"
    
    spark = (SparkSession 
                .builder 
                .appName("myApp") 
                .master("spark://spark-incidents:7077")
                .getOrCreate()
    )


    # Create Database
    try:
        client = ch_client()
        
        with open('etl/SQL/ddl.sql','r') as file:
            statemant = file.read().split(';')
            for query in statemant:
                if len(query) > 0:
                    print(query)
                    client.command(query)

    except Exception as e:
        raise e
        
    df = spark.read.parquet(f's3a://{BUCKET}/refined/{datetime.now().strftime("%Y%m%d")}/incidents.parquet')
    data = [list(row) for row in df.collect()]

    ic = client.create_insert_context(TABLE_NAME, data=data)
    client.insert(context=ic)

    result = client.command(select_qry)
    print(result)

if __name__=="__main__":
    load()