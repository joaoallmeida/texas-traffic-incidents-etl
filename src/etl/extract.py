from airflow.models import Variable
from io import BytesIO
from datetime import datetime
from minio import Minio

import logging
import requests
import json

URL_BASE = "https://data.austintexas.gov/resource/dx9v-zd7x.json?$limit=500000"
ACCESS_KEY = Variable.get('acces_key')
SECRET_KEY = Variable.get('secret_key')
ENDPOINT = "minio:4666"
BUCKET= Variable.get('bucket')
TIMEOUT = "10000000"

def get_data() -> any:
   
    try:      
        logging.info('Getting data from source!')
        
        response = requests.get( URL_BASE )
        response.raise_for_status()
        statusCode = response.status_code
        
        if statusCode == 200:
            logging.info('Data is available for producer!')
            dataMessage = json.dumps(response.json()).encode('utf-8')

        return dataMessage

    except Exception as e:
        logging.error(f'Failed to capture API data!')
        raise e

def minio_config(bucket:str):

    client = Minio(
        endpoint=ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )

    found = client.bucket_exists(bucket)

    if not found:
        client.make_bucket(bucket)
    else:
        logging.info(f"Bucket '{bucket}' already exists")

    return client
    

def extract_job():

    client = minio_config(BUCKET)
    data = get_data()
    
    logging.info('Uploading raw data to Bucket ')
    client.put_object(
        BUCKET,
        f'raw/{datetime.now().strftime("%Y%m%d")}/incidents.json',
        data=BytesIO(data),
        length=len(data),
        content_type='application/json'
    )
    logging.info('Uploaded completed!')

