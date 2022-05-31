"""
F20: get data from OpenWeather API and pust into Kinesis stream.
"""
import logging
import urllib3
import boto3
import botocore
logging.basicConfig(filename='Resources/push_data_kinesis.log',level=logging.INFO)
def get_city_list(buckets,key):
    """ function to get namelist of cities.
    Returns:
        cities(list): the namelist of cities.
    """
    try:
        #Call the s3 service
        #Provide information about the bucket and the key - city.txt is stored.
        s3_client = boto3.client('s3', endpoint_url="http://host.docker.internal:4566")
        #Read txt file contain names of 78 citys from s3 bucket
        object_=s3_client.get_object(Bucket=buckets, Key=key)
        data = object_['Body'].read().splitlines()
        cities=[]
        for line in data:
            line=line.decode("utf-8")
            line=line.split("'")[1]
            cities.append(line) #write them to a list
    except (botocore.exceptions.ConnectionClosedError,botocore.exceptions.ClientError) as error:
        logging.exception(error)
        return "error"
    return [cities,len(cities)]

def get_data_from_openweathermap_api(cities):
    """
    Function to get data from api of OpenWeather website.
    Get current weather of 70 citys by citys.txt.
    Returns:
        byte_array: Data from API which is converted to bytes.
    """
    #Get current weather data for each city in the list name
    list_data=[] #create a list to store data
    base_url="https://api.openweathermap.org/data/2.5/weather?"
    app_id="f43c15a214ecdde2cdf065132443eab0"
    for city in cities:
        url=base_url+ "q="+city +"&appid="+app_id
        http = urllib3.PoolManager()
        response=http.request("GET", url)
        if response.status == 200:
            data_bytes=response.data
            list_data.append(data_bytes) #write result to a list
        else:
            logging.exception(response.status)
    byte_array=b''.join(list_data) #convert to bytes type to push to kinesis
    return [byte_array,"succeed"]

def push_to_kinesis(stream_name):
    """function to push data to kinesis stream
    """
    try:
        kinesis=boto3.client('kinesis',endpoint_url="http://host.docker.internal:4566")
        buckets="csv"
        key="city.txt"
        cities=get_city_list(buckets,key)[0]
        data_to_push=get_data_from_openweathermap_api(cities)[0]
        kinesis.put_record(
            StreamName=stream_name,
            Data=data_to_push,
            PartitionKey='test2',
        )
    except (botocore.exceptions.ClientError,
            botocore.exceptions.ConnectionClosedError) as error:
        print(error)
        logging.exception(error)
        return "error"
    return "succeed"

def main(event,context):
    # pylint: disable=unused-argument
    """ Function to push data into kinesis.
    """
    #Call the Kinesis service.
    #Provide the name of the kinesis stream that you want to include the data in.
    stream_name='my_kinesis_stream'
    # push data to kinesis stream
    push_to_kinesis(stream_name)
    return "succeed"
