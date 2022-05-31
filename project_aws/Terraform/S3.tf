resource "aws_s3_bucket" "csv" {
    bucket = "csv"
}

resource "aws_s3_bucket" "fact" {
    bucket = "fact-weather-hourly"
}

resource "aws_s3_bucket" "firehosebucket" {
  bucket = "firehosebucket"
}

resource "aws_s3_bucket_object" "city_name" {
    bucket = aws_s3_bucket.csv.id
    key = "city.txt"
    source = "${path.module}/../Resources/city.txt"
    etag = filemd5("${path.module}/../Resources/city.txt")
}
