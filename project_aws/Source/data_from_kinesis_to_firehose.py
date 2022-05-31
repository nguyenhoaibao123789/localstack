"""
F21: get data from Kinesis stream.
"""
import logging
import base64
import datetime
import json
import ast
import boto3
import botocore

logging.basicConfig(filename='Resources/data_from_kinesis_to_firehose.log',level=logging.INFO)
def flatten(dictionary):
    """ Function to flatten data
    flattern a dictionary, json variable.

    Args:
        dictionary: a dictionary contain data.
    """
    try:
        unix_timestamp=dictionary["dt"]
        date=datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H')
        data={
            "Id_city":dictionary["id"],
            "city_name":dictionary["name"],
            "latitude":dictionary["coord"]["lat"],
            "longtitude":dictionary["coord"]["lon"],
            "country":dictionary["sys"]["country"],
            "timezone":dictionary["timezone"],
            "Id_weather":dictionary["weather"][0]["id"],
            "weather_description":dictionary["weather"][0]["description"],
            "weather_type":dictionary["weather"][0]["main"],
            "weather_icon":dictionary["weather"][0]["icon"],
            "date":date,
            "sunset":dictionary["sys"]["sunset"],
            "sunrise":dictionary["sys"]["sunrise"],
            "feels_like":dictionary["main"]["feels_like"],
            "humidity":dictionary["main"]["humidity"],
            "pressure":dictionary["main"]["pressure"],
            "temp":dictionary["main"]["temp"],
            "temp_max":dictionary["main"]["temp_max"],
            "temp_min":dictionary["main"]["temp_min"],
            "visibility":dictionary["visibility"],
            "wind_speed":dictionary["wind"]["speed"],
            "wind_direction":dictionary["wind"]["deg"]
        }
    except (KeyError,TypeError) as error:
        logging.exception(error)
        return "error"
    return data

def push_to_firehose(stream_name,data):
    """ Function to push data
    Push data to a kinesis firehose stream

    Args:
        stream_name: Name of the stream to push data to.
        data: data pf byte type to push.
    """
    try:
        firehose = boto3.client("firehose",endpoint_url="http://host.docker.internal:4566")
        firehose.put_record(
            DeliveryStreamName = stream_name,
            Record={
                "Data":json.dumps(data)
            }
        )
    except (TypeError,KeyError,
            botocore.exceptions.ClientError,
            botocore.exceptions.ConnectionClosedError) as error:
        logging.exception(error)
        return "error"
    return "succeed"

def transform_data(data_decode):
    """ Function to transform data
    transform a string to dictionary
    Args:
        data_decode: a string contain all data.
    """
    try:
        dataraw = data_decode.split(',"cod":200}')[:-1]
        list_data=[]
        for record in dataraw:
            data_in_dictionary = ast.literal_eval(record + "}")
            data=flatten(data_in_dictionary)
            list_data.append(data)
    except (AttributeError,SyntaxError) as error:
        logging.exception(error)
        return "error"
    return list_data

def main(event, context):
    """ Function to catch push event from kinesis stream
        Extract data from event and processing.

    Args:
        event: the data that's passed to the function upon execution.
        context: context object contains all data and methods related to lambda function.
    """
    # pylint: disable=unused-argument
    #s3_client = boto3.client('s3', endpoint_url="http://host.docker.internal:4566")
    datadecode_from_kinesis_stream = base64.b64decode(
        event['Records'][0]["kinesis"]["data"]).decode("utf-8")
    print("running read data from kinesis")
    list_data=transform_data(datadecode_from_kinesis_stream)
    firehose_stream_name ='firehosestream'
    push_to_firehose(firehose_stream_name,list_data)
    return "succeed"
    