### HOW TO RUN
- Cd project_aws.
- Run `docker-compose up`.
- Cd Terraform.
- Run `terraform init` - ` terraform plan` - `terraform apply -auto-approve` one by one from left to right.

### WHAT WE COVER IN THIS PROJECT
#### FEATURE 1: AUTO COUNT ROWS OF FILE   
- Whenever a new file is pushed to specific bucket (`csv` by default), auto count the rows of that file.
- Write result to a dynamodb table (`count_record_table` by default).
- Run `aws --endpoint-url=http://localhost:4566 s3 cp Resources/city.txt s3://csv` to copy the city.txt file to s3 bucket name `csv`. 
- Run `aws --endpoint-url=http://localhost:4566 dynamodb scan --table-name count_record_table` to check result

#### FEATURE 2: THIS FEATURE CONTAIN 3 SMALLER FEATURES
##### FEATURE 2.0: AUTO INGESTING DATA FROM API AND PUSH INTO KINESIS DATA STREAM 
- SCHEDULED TO AUTO RUN ONCE EVERY HOUR.
- Scheduled to automatic run once every hour.
- However, you can manually run to test result using `aws --endpoint-url=http://localhost:4566 lambda invoke --function-name put_kinesis ../Resources/out.txt` where `out.txt` should contain `succeed` which mean the execution is succeed.

##### FEATURE 2.1: AUTO PARSING AND FLATTING DATA THEN SEND TO KINESIS FIREHOSE STREAM
- Auto catch event in kinesis data stream (whenever there is new data pushed to stream).
- Get data from event and decode.

##### FEATURE 2.2: AUTO STORING DATA INTO DATA LAKE-DATA WAREHOUSE
- Send processed data in feature 2.1 to kinesis firehose.
- Data is pushed to s3 bucket (`firehosebucket` by default).
- Run `aws --endpoint-url=http://localhost:4566 s3api list-objects --bucket firehosebucket` to get key for each record.
- Run `aws --endpoint-url=http://localhost:4566 s3api get-object --bucket firehosebucket --key ... ../Resources/data_test.json` where data_test.json contain all the data and `...` is the key from above command.

# THE END
