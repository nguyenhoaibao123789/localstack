""" python file to set up airflow for push data in s3localstack to s3real.
"""
import os
from airflow.models import Variable
import glob
import json
from datetime import datetime, timedelta
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


######################################################

ENDPOINT_URL=os.environ.get('ENDPOINT_URL')
REGION_NAME=os.environ.get('REGION_NAME')

DESTINATION_PATH_LOCAL=os.environ.get('DESTINATION_PATH_LOCAL')
BUCKET_NAME_LOCALSTACK=os.environ.get('BUCKET_NAME_LOCALSTACK')
BUCKET_NAME_REAL=os.environ.get('BUCKET_NAME_REAL')
FOLDER_NAME_REAL=os.environ.get('FOLDER_NAME_REAL')

AWS_ACCESS_KEY_ID=Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=Variable.get('AWS_SECRET_ACCESS_KEY')

#################################################################

s3_localstack= boto3.client('s3',
                            endpoint_url=ENDPOINT_URL,
                            region_name=REGION_NAME,
                            aws_access_key_id='temp',
                            aws_secret_access_key='temp')

s3_real = boto3.resource('s3',
                         aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                         region_name=REGION_NAME)

#Utils
def from_localstack_to_local(bucket_name, destination):
    """A funtion to download data from s3 localstack to local
    Args:
        bucket_name (string): a name of the s3 bucket in localstack.
        destination (string): a path where store data in local after download.
    """
    list_key = [obj['Key'] for obj in s3_localstack.list_objects(Bucket=bucket_name)['Contents']]
    for key in list_key:
        data=json.loads(s3_localstack.get_object(Bucket=bucket_name,Key=key)['Body'].read())
        file_name = destination + key.replace('/','-')+'.json'
        with open(file_name, 'w') as json_file:
            json.dump(data, json_file)
        s3_localstack.delete_object(Bucket=bucket_name, Key=key)
def from_local_to_real(bucket_name,source_place):
    """A function to upload data from local to s3 real.

    Args:
        bucket_name (string): a name of the bucket you want to push the data in.
        source_place (string): a path for get data.
    """
    local_objects=glob.glob(source_place+'*')
    for local_object in local_objects:
        s3_real.Bucket(bucket_name).upload_file(local_object,
                                                FOLDER_NAME_REAL+os.path.basename(local_object))
def remove_in_local(source_place):
    """A function to remove all file in local after pushing data.

    Args:
        source_place (string): a path of folder where store data.
    """
    local_objects=glob.glob(source_place+'*')
    for local_object in local_objects:
        os.remove(local_object)

 ##########################################################

default_args = {
    "start_date": datetime(2022, 5, 11),
    "catchup":False
}

with DAG(
    dag_id='localstack_to_real_s3',
    default_args=default_args,
    schedule_interval=timedelta(minutes=30)

) as dag :
    #dag for download
    download_from_s3 = PythonOperator(
        task_id='download_from_s3localstack',
        python_callable=from_localstack_to_local,
        op_kwargs={'bucket_name': BUCKET_NAME_LOCALSTACK, 'destination': DESTINATION_PATH_LOCAL},
        dag=dag)
    #dag for upload
    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=from_local_to_real,
        op_kwargs={'bucket_name': BUCKET_NAME_REAL,'source_place': DESTINATION_PATH_LOCAL},
        dag=dag)
    #dag remove after upload
    remove_place = PythonOperator(
        task_id='remove_place',
        python_callable=remove_in_local,
        op_kwargs={"source_place":DESTINATION_PATH_LOCAL},
        dag=dag)
#Set the order for dags
download_from_s3>>upload_to_s3>>remove_place
