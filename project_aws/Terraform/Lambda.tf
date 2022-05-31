data "archive_file" "rows_count" {
    output_path = "${path.module}/../Source/rows_count.zip"
    source_file = "${path.module}/../Source/rows_count.py"
    #excludes = ["__pycache__", "*.pyc"]
    type = "zip"
}

data "archive_file" "push_data_kinesis" {
    output_path = "${path.module}/../Source/push_data_kinesis.zip"
    source_file = "${path.module}/../Source/push_data_kinesis.py"
    #excludes = ["__pycache__", "*.pyc"]
    type = "zip"
}
data "archive_file" "data_from_kinesis_to_firehose" {
    output_path = "${path.module}/../Source/data_from_kinesis_to_firehose.zip"
    source_file = "${path.module}/../Source/data_from_kinesis_to_firehose.py"
    #excludes = ["__pycache__", "*.pyc"]
    type = "zip"
}

resource "aws_lambda_function" "count" {
    function_name = "countrecords"
    filename = data.archive_file.rows_count.output_path
    handler = "rows_count.main"
    role = aws_iam_role.lambda_role.arn
    runtime = "python3.8"
    timeout       = 180
}

resource "aws_lambda_function" "put_kinesis" {
    function_name = "put_kinesis"
    filename =data.archive_file.push_data_kinesis.output_path
    handler = "push_data_kinesis.main"
    role = aws_iam_role.lambda_role.arn
    runtime = "python3.8"
    timeout = 180
}

resource "aws_lambda_function" "get_kinesis" {
    function_name = "get_kinesis_data"
    filename = data.archive_file.data_from_kinesis_to_firehose.output_path
    handler = "data_from_kinesis_to_firehose.main"
    role = aws_iam_role.lambda_role.arn
    runtime = "python3.8"
    timeout = 180
}