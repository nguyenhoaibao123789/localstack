provider "aws" {
    region = "us-east-2"
    access_key = "fake"
    secret_key = "fake"
    skip_credentials_validation = true
    skip_metadata_api_check = true
    skip_requesting_account_id = true
    s3_force_path_style = true

    endpoints {
        dynamodb = "http://host.docker.internal:4566"
        lambda = "http://localhost:4566"
        kinesis = "http://host.docker.internal:4566"
        s3 = "http://host.docker.internal:4566"
        iam = "http://localhost:4566"
        firehose = "http://host.docker.internal:4566"
        cloudwatch = "http://localhost:4566"
        cloudwatchevents = "http://localhost:4566"
        cloudwatchlogs = "http://localhost:4566"
    }
}
