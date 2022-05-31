"""Test with pytest for functions in push_data_kinesis.py
"""
import sys
import pytest
sys.path.append("C:/Users/FAHCM.HCM22_FR_Data_01/project_aws")
from Source.push_data_kinesis import main, push_to_kinesis
from Source.push_data_kinesis import get_city_list, get_data_from_openweathermap_api


@pytest.mark.parametrize("bucket,key,expected_result",
                         [["csv", "city.txt", ["bytes", 77]], ["fake_bucket", "fake_key", "error"]])
def test_get_city_list(bucket, key, expected_result):
    """A function to test the get_city_list function.

    Args:
        bucket (string): a name of bucket.
        key (string): a key of a object in bucket.
        expected_result (int or 'error'): the number of rows in the object.
    """
    num_cities = get_city_list(bucket, key)[1]
    assert num_cities == expected_result[1]


@pytest.mark.parametrize("cities,expected_result",
                         [[["fake_city"], ["data_return", "succeed"]],
                          [["ho chi minh"], ["data_return", "succeed"]]])
def test_get_data_from_openweathermap_api(cities, expected_result):
    """A function to test the get_data_from_openweathermap_api function.

    Args:
        cities (list): a name city list.
        expected_result (int): a number of cities.
    """
    len_data = get_data_from_openweathermap_api(cities)[1]
    assert len_data == expected_result[1]


@pytest.mark.parametrize("stream_name,expected_result",
                         [["my_kinesis_stream", "succeed"], ["mystream1", "error"], ["not_exist", "error"]])
def test_push_to_kinesis(stream_name, expected_result, mocker):
    """A function to test the push_to_kinesis function.

    Args:
        stream_name (string): _description_
        expected_result (string): an expected status ("succeed" or "error").
        mocker (_type_): _description_
    """
    mocker.patch("Source.push_data_kinesis.get_data_from_openweathermap_api",
                 return_value=[b'S\x00t\x00a', 77])
    # mocker.patch("Source.push_data_kinesis.push_to_kinesis.data_to_push",return_value=77)
    true_result = push_to_kinesis(stream_name)
    assert true_result == expected_result


@pytest.mark.parametrize("event,context,expected_result", [[{}, {}, "succeed"]])
def test_main(event, context, expected_result, mocker):
    """A function to test the main function.

    Args:
        event: the data that's passed to the function upon execution.
        context: context object contains all data and methods related to lambda function.
        expected_result (string): an expected status.
        mocker (_type_): _description_
    """
    mocker.patch("Source.push_data_kinesis.push_to_kinesis",
                 return_value="succeed")
    result = main(event, context)
    assert result == expected_result
