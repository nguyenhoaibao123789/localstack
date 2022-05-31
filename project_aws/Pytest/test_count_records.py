"""
Test with pytest for functions in F11.
"""
# sys.path.append("C:/Users/BaoNH6/Desktop/FAHCM.HCM22_FR_Data_01/project_aws")
# sys.path.append("E:/FAHCM.HCM22_FR_Data_01-HanhDTH9_BaoNH6/project_aws")

import sys
import boto3
import pytest
sys.path.append('C:/Users/FAHCM.HCM22_FR_Data_01/project_aws')
from Source.rows_count import count_rows, push, main


@pytest.fixture(autouse=True, scope="module")
def prepare_s3():
    """Set up S3 for test: create bucket and push data into s3 bucket.
    """
    s3_client = boto3.client(
        's3', endpoint_url="http://host.docker.internal:4566")
    test_bucket_name = 'testbucket'
    test_data = b'col_1,col_2\n1,2\n3,4\n'
    test_key = "test.csv"
    s3_client.create_bucket(Bucket=test_bucket_name)
    s3_client.put_object(Body=test_data, Bucket=test_bucket_name, Key=test_key)

# mock s3 event
test_s3_event = {
    "Records": [{
        "s3": {
            'bucket': {'name': 'testbucket'},
            'object': {
                'key': 'test.csv'
            }
        }
    }]}

@pytest.mark.parametrize("bucket,key,expected_result",
                         [["testbucket", "test.csv", 2], ["testbucket22", "test.csv", "error"]])
def test_count_rows(bucket, key, expected_result):
    """A function to test the count_rows function.

    Args:
        bucket (string): a name of bucket.
        key (string): a key of a object in bucket.
        expected_result (int or "error"): the number of rows in the object.
    """
    rows = count_rows(bucket, key)
    assert rows == expected_result

@pytest.mark.parametrize("table_name,value,expected_result",
                         [["count_record_table", 3, "succeed"], ["not_exist_table", 3, "error"]])
def test_push(table_name, value, expected_result):
    """A function to test the push function.

    Args:
        table_name (string): a name of a table in DynamoDB.
        value (int): a value to store in DynamoDB.
        expected_result (string): an expected status ("succeed";"error")
                                    after executing push function for each object.
    """
    try_push = push(table_name, value)
    assert try_push == expected_result

@pytest.mark.parametrize("event,context,expected_result", [[test_s3_event, {}, "succeed"]])
def test_main(event, context, expected_result, mocker):
    """A  function to test main function.

    Args:
        event: the data that's passed to the function upon execution.
        context: context object contains all data and methods related to lambda function.
        expected_result (string): an expected status.
        mocker (_type_): _description_
    """
    mocker.patch("Source.rows_count.count_rows", return_value="3")
    mocker.patch("Source.rows_count.push", return_value="error")
    response = main(event, context)
    assert response == expected_result
