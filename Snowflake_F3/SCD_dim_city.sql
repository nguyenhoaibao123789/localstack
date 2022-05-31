
USE S3JSON

//####################################################################################################### 

truncate table dim_city;
truncate table dim_city_landing;
truncate table dim_city_staging;
truncate table fact_weather1hour;

//#######################################################################################################

//Create stream
// A stream to capture changes in country_overview_raw to runtask raw_to_landing
create or replace stream dim_city_stream on table dim_city;
select * from dim_city_stream;

//Create stream
// A stream to capture changes in dim_city_landing table
create or replace stream dim_city_landing_stream on table dim_city_landing;
select * from dim_city_landing_stream;


#######################################################################################################

//create task to get data from stream
create or replace task stream_to_dimcity_landing
warehouse=compute_wh
schedule='3 minute'  
when
system$stream_has_data('dim_city_stream')
as
merge into dim_city_landing landing
using (select * from dim_city_stream) city_stream
on landing.id_city = city_stream.id_city 
when matched and (landing.City_name!=city_stream.City_name or
                  landing.Latitude!=city_stream.Latitude or
                  landing.Longtitude!=city_stream.Longtitude or
                  landing.Country!=city_stream.Country or
                  landing.Timezone!=city_stream.Timezone
                 ) 
then
update set  landing.City_name=city_stream.City_name,
            landing.Latitude=city_stream.Latitude,
            landing.Longtitude=city_stream.Longtitude,
            landing.Country=city_stream.Country,
            landing.Timezone=city_stream.Timezone
when not matched 
then
insert(id_city,
  City_name, 
  Latitude, 
  Longtitude,
  Country,
  Timezone)
values (city_stream.id_city,city_stream.City_name, city_stream.Latitude, city_stream.Longtitude, city_stream.Country,city_stream.Timezone);

#######################################################################################################

//create task to update changes in target table
create or replace task dim_city_landing_to_staging
warehouse=compute_wh
after stream_to_dimcity_landing
when
system$stream_has_data('dim_city_landing_stream')
as
merge into Dim_City_staging stage
using (select * from dim_city_landing_stream) landing_stream
on stage.id_city = landing_stream.id_city 
and stage.City_name=landing_stream.City_name
and stage.Latitude=landing_stream.Latitude
and stage.Longtitude=landing_stream.Longtitude
and stage.Country=landing_stream.Country
and stage.Timezone=landing_stream.Timezone
when matched and (landing_stream.metadata$action='DELETE')
then
update set end_time = to_timestamp(current_timestamp),
current_flag_status = 'False'
when not matched and (landing_stream.metadata$action='INSERT')
then
insert(id_city,
  City_name, 
  Latitude, 
  Longtitude,
  Country,
  Timezone, 
  start_time,
  end_time,
  current_flag_status
)
values (
  landing_stream.id_city,
  landing_stream.City_name, 
  landing_stream.Latitude, 
  landing_stream.Longtitude, 
  landing_stream.Country,
  landing_stream.Timezone, 
  to_timestamp(current_timestamp),
  NULL,
  'True'
);

#######################################################################################################
//Create task to remove data in dim_city table
create or replace task dim_city_truncate
warehouse=compute_wh
after dim_city_landing_to_staging
as
truncate table dim_city;


#######################################################################################################

SHOW TASKS;

ALTER TASK stream_to_dimcity_landing SUSPEND;
ALTER TASK DIM_CITY_LANDING_TO_STAGING SUSPEND;
ALTER TASK dim_city_truncate SUSPEND;

ALTER TASK dim_city_truncate RESUME;
ALTER TASK DIM_CITY_LANDING_TO_STAGING RESUME;
ALTER TASK stream_to_dimcity_landing RESUME;

#######################################################################################################

//check what task has run
select * from table(information_schema.task_history());

#######################################################################################################
select * 
from Fact_weather1hour
                                                                       
                                                                         