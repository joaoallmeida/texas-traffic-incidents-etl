from airflow.models import Variable
from pyspark.sql import SparkSession
from datetime import datetime

import pyspark.sql.functions as f
import pyspark.sql.types as t

BUCKET= Variable.get('bucket')

def refined_job():
       
    spark = (SparkSession 
                .builder 
                .appName("myApp") 
                .master("spark://spark-incidents:7077")
                .getOrCreate()
    )   

    df = spark.read.format('json').load(f's3a://{BUCKET}/raw/{datetime.now().strftime("%Y%m%d")}/incidents.json')

    longColumns = ['latitude','longitude']
    stringColumns = ['address','issue_reported','traffic_report_status']
    timestampColumns = ['published_date','traffic_report_status_date_time']
    dropColumns = ['location']

    for col in longColumns:
        df = df.withColumn(col, f.col(col).cast(t.FloatType()))

    for col in stringColumns:
        df = df.withColumn(col, f.initcap(f.col(col)))

    for col in timestampColumns:
        df = df.withColumn(col, f.to_timestamp(f.col(col)))

    df = df.drop(dropColumns[0])
    df = (
        df.withColumn('year', f.year(f.col('published_date')) )
        .withColumn('month',  f.concat(f.month(f.col("published_date")), f.lit(".") ,f.date_format("published_date", "MMMM")))
        # .withColumn('month_num',  f.month(f.col("published_date")))
        .withColumn('traffic_report_status', f.when(f.col('traffic_report_status').isNull(),'Undefine').otherwise(f.col('traffic_report_status')) )
        .withColumn('latitude', f.when(f.col('latitude').isNull(),0).otherwise(f.col('latitude')) )
        .withColumn('longitude', f.when(f.col('longitude').isNull(),0).otherwise(f.col('longitude')) )
        # .withColumn('id', f.monotonically_increasing_id())
    )

    df = df.select("traffic_report_id","published_date","issue_reported","latitude", "longitude","address", "traffic_report_status", "traffic_report_status_date_time", "year", "month")

    df.write.mode('overwrite').parquet(f's3a://{BUCKET}/refined/{datetime.now().strftime("%Y%m%d")}/incidents.parquet')

if __name__=="__main__":
    refined_job()