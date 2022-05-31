resource "aws_kinesis_stream" "my_kinesis_stream" {
    name = "my_kinesis_stream"
    shard_count = 1
    retention_period = 30

    shard_level_metrics = [
        "IncomingBytes",
        "IncomingRecords",
        "OutgoingBytes",
        "OutgoingRecords"
    ]
}
