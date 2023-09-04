from airflow.models import Variable 
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from datetime import datetime

from etl.extract import extract_job

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s',datefmt='%y-%m-%d %H:%M:%S')
logConf = logging.getLogger()
logConf.setLevel(logging.INFO)


TIMEOUT = "10000000"
ACCESS_KEY = Variable.get('acces_key')
SECRET_KEY = Variable.get('secret_key')
ENDPOINT = "minio:4666"
BUCKET= Variable.get('bucket')

spark_conf = {
    "spark.hadoop.fs.s3a.endpoint": f"http://{ENDPOINT}",
    "spark.hadoop.fs.s3a.access.key": ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": SECRET_KEY,
    "spark.hadoop.fs.s3a.connection.timeout": TIMEOUT,
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": True,
    "spark.hadoop.fs.s3a.path.style.access": True
}


@dag(
    dag_id='traffic-incidents-pipeline',
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
)
def pipeline():
    
    extractTask = PythonOperator(
        task_id='extract_task',
        python_callable=extract_job
    )

    refinedTask = SparkSubmitOperator(
        application='./etl/refined.py',
        task_id = 'refined_task',
        verbose=True,
        packages='org.apache.hadoop:hadoop-aws:3.3.4',
        conf=spark_conf
    )

    validateTask = SparkSubmitOperator(
        application='./etl/validate.py',
        task_id = 'validate_task',
        verbose=True,
        packages='org.apache.hadoop:hadoop-aws:3.3.4',
        application_args=['spark'],
        conf=spark_conf
    )

    loadTask = SparkSubmitOperator(
        application='./etl/load.py',
        task_id = 'load_task',
        verbose=True,
        packages='org.apache.hadoop:hadoop-aws:3.3.4',
        application_args=['spark'],
        conf=spark_conf
    )

    extractTask >> refinedTask >> validateTask >> loadTask

pipeline()