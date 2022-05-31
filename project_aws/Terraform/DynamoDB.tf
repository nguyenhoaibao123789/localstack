// DYNAMODB TABLES
resource "aws_dynamodb_table" "count_record_table" {
    name = "count_record_table"
    read_capacity = "20"
    write_capacity = "20"
    hash_key = "rows"

    attribute {
        name = "rows"
        type = "N"
    }
}