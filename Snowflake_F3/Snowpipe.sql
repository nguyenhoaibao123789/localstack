USE S3JSON;
ALTER SESSION SET TIMESTAMP_NTZ_OUTPUT_FORMAT ='MM-DD-YYYY HH24:mi';
#######################################################################################################   
SELECT * FROM @S3JSON.external_stages.json_folder;
//Create the snow pip to catch data and copy them into dim_city table
CREATE OR REPLACE PIPE S3JSON.PIPE.s3_dim_city auto_ingest=true AS
    COPY INTO S3JSON.PUBLIC.dim_city 
    FROM (SELECT $1:"Id_city"::int,
          $1:"city_name"::STRING, 
          $1:"latitude"::FLOAT, 
          $1:"longtitude"::FLOAT,
          $1:"country"::STRING, 
          $1:"timezone"::INT
        FROM @S3JSON.external_stages.json_folder);

####################################################################################################### 
//Create the snow pip to catch data and copy them into dim_weathertype table
CREATE OR REPLACE PIPE S3JSON.PIPE.s3_dim_weathertype auto_ingest=true AS
COPY INTO S3JSON.PUBLIC.dim_weathertype 
  FROM (SELECT DISTINCT $1:"Id_weather"::int,
        $1:"weather_description"::STRING, 
        $1:"weather_type"::STRING
      FROM @S3JSON.external_stages.json_folder);

//#######################################################################################################
//Create the snow pip to catch data and copy them into fact table
CREATE OR REPLACE PIPE S3JSON.PIPE.s3_fact_weather auto_ingest=true AS
  COPY INTO S3JSON.PUBLIC.fact_weather1hour 
  FROM (SELECT $1:"Id_city"::int,
        $1:"Id_weather"::int,
        try_to_timestamp_ntz($1:"date"::string),
        try_to_timestamp_ntz($1:"sunset"::string), 
        try_to_timestamp_ntz($1:"sunrise"::string),
        $1:"feels_like"::float,
        $1:"humidity"::int,
        $1:"pressure"::int,
        $1:"temp"::float,
        $1:"temp_max"::float,
        $1:"temp_min"::float,
        $1:"visibility"::int,
        $1:"wind_speed"::float,
        $1:"wind_direction"::int
      FROM @S3JSON.external_stages.json_folder);

//#######################################################################################################   

//Get SQS and information about pipe      
DESCRIBE PIPE S3JSON.PIPE.s3_fact_weather;
DESCRIBE PIPE S3JSON.PIPE.s3_dim_weathertype;
#######################################################################################################   

//Check pipe status
select system$pipe_status('S3JSON.PIPE.s3_dim_weathertype');
select system$pipe_status('S3JSON.PIPE.s3_fact_weather');
select system$pipe_status('S3JSON.PIPE.s3_dim_city');

select * from table(information_schema.copy_history(table_name=>'S3JSON.PUBLIC.dim_city',
                                                    start_time=>dateadd(hours,-1,current_timestamp())));  
                                                    
