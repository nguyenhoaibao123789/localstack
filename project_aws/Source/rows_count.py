"""
F11: count file rows in s3 bucket and write result to dynamoDB.
"""
import logging
import boto3
import botocore
logging.basicConfig(filename='Resources/rows_count.log', level=logging.INFO)
s3 = boto3.client('s3', endpoint_url="http://host.docker.internal:4566")
dynamodb = boto3.resource(
    'dynamodb',
    endpoint_url="http://host.docker.internal:4566")


def count_rows(bucket, key):
    """Function to count the number of records in .csv file in s3.
    Args:
        bucket (str): a name of bucket in s3.
        key (str): a key of the bucket.
    Returns:
        int: The number of records in .csv file.
    """
    try:
        object_ = s3.get_object(Bucket=bucket, Key=key)
        data = object_['Body'].read().splitlines()
        row_count = len(data) - 1
    except botocore.exceptions.ClientError as error:
        logging.exception(error)
        return "error"
    return row_count


def push(table_name, value):
    """Function to push count result of .csv fileS to table in DynamoDB.
    Args:
        table_name (str): a name of the table that stores the count result.
        value (int): countable value.
    Raises:
        e: error.
    """
    try:
        client = boto3.client('dynamodb',
                              endpoint_url="http://host.docker.internal:4566")
        describe_table = client.describe_table(
            TableName=table_name
        )
        key = describe_table["Table"]["KeySchema"][0]["AttributeName"]
        table = dynamodb.Table(table_name)
        table.put_item(
            Item={
                key: value
            }
        )
    except (botocore.exceptions.ClientError, botocore.exceptions.ConnectionClosedError) as error:
        logging.exception(error)
        return "error"
    return "succeed"


def main(event, context):
    # pylint: disable=unused-argument
    """Function to catch events from S3 to count the number of records.
    Args:
        event: the data that's passed to the function upon execution.
        context: context object contains all data and methods related to lambda function.

    Raises:
        e: error.

    Returns:
        rows(int): count results from count_rows function.
    """
    table_name = 'count_record_table'
    buckets = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    rows = count_rows(buckets, object_key)
    push(table_name, rows)
    return "succeed"
