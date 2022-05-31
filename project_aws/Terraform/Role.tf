#Role
resource "aws_iam_role" "lambda_role" {
  name               = "lamdbdaRole"
  assume_role_policy = <<EOF
{
 "Version": "2012-10-17",
 "Statement": [
   {
     "Action": "sts:AssumeRole",
     "Principal": {
       "Service": "lambda.amazonaws.com"
     },
     "Effect": "Allow",
     "Sid": ""
   }
 ]
}
EOF
}

resource "aws_iam_role" "firehose_role" {
  name = "firehose_test_role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "firehose.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

#Policy iam
resource "aws_iam_policy" "iam_policy" {

  name   = "iamPolicy"
  policy = <<EOF
{
 "Version": "2012-10-17",
 "Statement": [
   {
     "Action": [
       "logs:CreateLogGroup",
       "logs:CreateLogStream",
       "logs:PutLogEvents"
     ],
     "Resource": "*",
     "Effect": "Allow"
   }
 ]
}
EOF
}


resource "aws_iam_policy" "dynamodb_policy" {
  name   = "dynamoPolicy"
  policy = <<EOF
{  
  "Version": "2012-10-17",
  "Statement":[
    {
    "Effect": "Allow",
    "Action": [
     "dynamodb:BatchGetItem",
     "dynamodb:GetItem",
     "dynamodb:Query",
     "dynamodb:Scan",
     "dynamodb:BatchWriteItem",
     "dynamodb:PutItem",
     "dynamodb:UpdateItem"
    ],
    "Resource": "*"
    }
  ]
}
EOF
}

resource "aws_iam_policy" "s3_policy" {

  name   = "s3_policy"
  policy = <<EOF
{
 "Version": "2012-10-17",
 "Statement": [
   {
     "Action": [
       "s3:PutObject",
       "s3:GetObject",
       "s3:ListBucket"
     ],
     "Resource": "*",
     "Effect": "Allow"
   }
 ]
}
EOF
}

# Attachment policies with role
resource "aws_iam_role_policy_attachment" "attach_iam_policy_to_iam_role" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.iam_policy.arn
}

resource "aws_iam_role_policy_attachment" "attach_dynamo_policy_to_iam_role" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.dynamodb_policy.arn
}

resource "aws_iam_role_policy_attachment" "attach_s3_policy_to_iam_role" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

