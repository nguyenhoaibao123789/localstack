resource "aws_lambda_permission" "allow_bucket" {
    statement_id = "AllowExecutionFromS3Bucket"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.count.arn
    principal = "s3.amazonaws.com"
    source_arn = aws_s3_bucket.csv.arn
}

resource "aws_s3_bucket_notification" "bucket_notification" {
    bucket = aws_s3_bucket.csv.id

    lambda_function {
        lambda_function_arn = aws_lambda_function.count.arn
        events = ["s3:ObjectCreated:*"]
    }

    depends_on = [aws_lambda_permission.allow_bucket]
}

# Schedule
resource "aws_cloudwatch_event_rule" "scheduled_lambda_schedule" {
  schedule_expression ="rate(59 minutes)"
  name = "scheduled_lambda_schedule"
  description = "Schedule to trigger put_kinesis function."
  is_enabled = true
}

resource "aws_cloudwatch_event_target" "scheduled_lambda_event_target" {
    rule = aws_cloudwatch_event_rule.scheduled_lambda_schedule.name
    target_id = "InvokeLambda"
    arn = aws_lambda_function.put_kinesis.arn
}

resource "aws_lambda_permission" "scheduled_lambda_cloudwatch_permission" {
    statement_id = "AllowExecutionFromCloudWatch"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.put_kinesis.arn
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.scheduled_lambda_schedule.arn
}

# Trigger event for kinesis
resource "aws_lambda_event_source_mapping" "trigger" {
    event_source_arn = "${aws_kinesis_stream.my_kinesis_stream.arn}"
    function_name = "${aws_lambda_function.get_kinesis.arn}"
    starting_position = "LATEST"
}
