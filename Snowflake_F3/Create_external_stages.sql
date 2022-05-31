
USE S3JSON
#######################################################################################################

//do not run again
// Create storage integration object
create or replace storage integration s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::796919976836:role/snowflake-access-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflakehanhbao/')
   COMMENT = 'This an optional comment' 
   
#######################################################################################################
  
// See storage integration properties to fetch external_id so we can update it in S3
DESC integration s3_int;

#######################################################################################################

// Create file format object
CREATE OR REPLACE file format S3JSON.file_format.json_fileformat
    type = json
    strip_outer_array = true;

#######################################################################################################

// Create stage to get data from json_folder in S3 bucket
CREATE OR REPLACE stage S3JSON.external_stages.json_folder
    URL = 's3://snowflakehanhbao/json/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = S3JSON.file_format.json_fileformat

    
#######################################################################################################    

// Check
SELECT * FROM @S3JSON.external_stages.json_folder
