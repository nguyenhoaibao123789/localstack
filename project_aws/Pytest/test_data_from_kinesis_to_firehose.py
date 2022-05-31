"""Test with pytest for functions in data_from_kinesis_to_firehose.py
"""
import sys
import pytest
sys.path.append("C:/Users/FAHCM.HCM22_FR_Data_01/project_aws")
from Source.data_from_kinesis_to_firehose import flatten, push_to_firehose, transform_data, main
# sys.path.append("E:/FAHCM.HCM22_FR_Data_01-HanhDTH9_BaoNH6/project_aws")

true_dict = {
    "coord": {
        "lon": -122.08,
        "lat": 37.39
    },
    "weather": [
        {
            "id": 800,
            "main": "Clear",
            "description": "clear sky",
            "icon": "01d"
        }
    ],
    "base": "stations",
    "main": {
        "temp": 282.55,
        "feels_like": 281.86,
        "temp_min": 280.37,
        "temp_max": 284.26,
        "pressure": 1023,
        "humidity": 100
    },
    "visibility": 10000,
    "wind": {
        "speed": 1.5,
        "deg": 350
    },
    "clouds": {
        "all": 1
    },
    "dt": 1560350645,
    "sys": {
        "type": 1,
        "id": 5122,
        "message": 0.0139,
        "country": "US",
        "sunrise": 1560343627,
        "sunset": 1560396563
    },
    "timezone": -25200,
    "id": 420006353,
    "name": "Mountain View",
    "cod": 200
}

true_output = {'Id_city': 420006353, 'city_name': 'Mountain View', 'latitude': 37.39,
               'longtitude': -122.08, 'country': 'US', 'timezone': -25200, 'Id_weather': 800,
               'weather_description': 'clear sky', 'weather_type': 'Clear',
               'weather_icon': '01d', 'date': '2019-06-12 14', 'sunset': 1560396563,
               'sunrise': 1560343627, 'feels_like': 281.86, 'humidity': 100, 'pressure': 1023,
               'temp': 282.55, 'temp_max': 284.26, 'temp_min': 280.37, 'visibility': 10000,
               'wind_speed': 1.5, 'wind_direction': 350}

fake_dict = {}

fake_kinesis_event = {
    "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49590338271490256608559692538361571095921575989136588898",
                "data": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                "approximateArrivalTimestamp": 1545084650.987
            }
        }
    ]
}


@pytest.mark.parametrize("_dict,expected_result", [[true_dict, true_output], [fake_dict, "error"]])
def test_flatten(_dict, expected_result):
    """A function to test the flatten function.

    Args:
        _dict (dict): Data in dict format need to flatten.
        expected_result (dict or "error"): Data in dict format after flatten or "error".
    """
    data = flatten(_dict)
    assert data == expected_result


@pytest.mark.parametrize("stream_name,data,expected_result",
                         [["firehosestream", "mock_data", "succeed"],
                          ["fake_firehose_stream", "mock_data", "error"]])
def test_push_to_firehose(stream_name, data, expected_result):
    """A function to test the push_to_firehose function.

    Args:
        stream_name (string): a name of firehose kinesis stream.
        data (dict): data in dict format.
        expected_result (string): a expected status - "succeed" or "error".
    """
    result = push_to_firehose(stream_name, data)
    assert result == expected_result


@pytest.mark.parametrize("data,expected_result",
                         [["mock_data", []],
                          ['{"mock":2,"cod":200}{"mock2":3,"cod":200}',
                              ["mock", "mock"]],
                          [fake_dict, "error"]])
def test_transform_data(data, expected_result, mocker):
    """A function to test the transform_data function.

    Args:
        data (dict): data in dict format.
        expected_result (_type_): a expected value after transforming.
        mocker.
    """
    mocker.patch("Source.data_from_kinesis_to_firehose.flatten",
                 return_value="mock")
    list_data = transform_data(data)
    assert list_data == expected_result


@pytest.mark.parametrize("event,context,expected_result", [[fake_kinesis_event, {}, "succeed"]])
def test_main(event, context, expected_result, mocker):
    """A function to test the main function.

    Args:
        event: the data that's passed to the function upon execution.
        context: context object contains all data and methods related to lambda function.
        expected_result (string): an expected status.
        mocker.
    """
    mocker.patch(
        "Source.data_from_kinesis_to_firehose.transform_data", return_value="mock")
    mocker.patch(
        "Source.data_from_kinesis_to_firehose.push_to_firehose", return_value="succeed")
    result = main(event, context)
    assert result == expected_result
