resource "aws_kinesis_firehose_delivery_stream" "firehosestream" {
  name        = "firehosestream"
  destination = "s3"

  s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = aws_s3_bucket.firehosebucket.arn
  }
}

