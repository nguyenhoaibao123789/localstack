USE S3JSON

####################################################################################################### 

truncate table dim_weathertype;
truncate table dim_weathertype_landing;
truncate table dim_weathertype_staging;
truncate table dim_weather_distinct;
#######################################################################################################

//Create stream
// A stream to capture changes in dim_weathertype_landing table
create or replace stream dim_weathertype_landing_stream on table dim_weathertype_landing;
select * from dim_weathertype_landing_stream;
#######################################################################################################
//Create task
//A task to create table that contain only distinct value of dim_weathertype table
create or replace task select_distinct
warehouse=compute_wh
after dim_city_truncate
as
CREATE OR REPLACE TABLE dim_weather_distinct AS SELECT DISTINCT * FROM dim_weathertype;    

#######################################################################################################

//create task to get data from table contain distinct value
create or replace task stream_to_dim_weathertype_landing
warehouse=compute_wh
after select_distinct 
as
merge into dim_weathertype_landing landing
using (select * from dim_weather_distinct) distinct_table
on landing.id_wtype = distinct_table.id_wtype 
when matched and (landing.weather_description!=distinct_table.weather_description or
                  landing.weather_type!=distinct_table.weather_type
                 ) 
then
update set  landing.weather_description=distinct_table.weather_description,
            landing.weather_type=distinct_table.weather_type
when not matched 
then
insert(id_wtype,
        weather_description,
        weather_type)
values (distinct_table.id_wtype,distinct_table.weather_description,distinct_table.weather_type);

#######################################################################################################

//create task to update changes in target table
create or replace task dim_weathertype_landing_to_staging
warehouse=compute_wh
after stream_to_dim_weathertype_landing
when
system$stream_has_data('dim_weathertype_landing_stream')
as
merge into Dim_WeatherType_staging stage
using (select * from dim_weathertype_landing_stream) landing_stream
on stage.id_wtype = landing_stream.id_wtype 
and stage.weather_description=landing_stream.weather_description
and stage.weather_type=landing_stream.weather_type
when matched and (landing_stream.metadata$action='DELETE')
then
update set end_time = to_timestamp(current_timestamp),
current_flag_status = 'False'
when not matched and (landing_stream.metadata$action='INSERT')
then
insert(id_wtype,
  weather_description, 
  weather_type, 
  start_time,
  end_time,
  current_flag_status
)
values (
  landing_stream.id_wtype,
  landing_stream.weather_description, 
  landing_stream.weather_type, 
  to_timestamp(current_timestamp),
  NULL,
  'True'
);

#######################################################################################################

//Create task to remove file in external staging
Create or replace task src_file_remove
warehouse=compute_wh
after dim_weathertype_landing_to_staging
as
remove '@S3JSON.external_stages.json_folder';
//#######################################################################################################
//Create task to remove data in dim_weathertype table
create or replace task dim_weathertype_truncate
warehouse=compute_wh
after src_file_remove
as
truncate table dim_weathertype;


#######################################################################################################

SHOW TASKS;

ALTER TASK select_distinct SUSPEND;
ALTER TASK stream_to_dim_weathertype_landing SUSPEND;
ALTER TASK dim_weathertype_landing_to_staging SUSPEND;
ALTER TASK src_file_remove SUSPEND;
ALTER TASK dim_weathertype_truncate SUSPEND;

ALTER TASK dim_weathertype_truncate RESUME;
ALTER TASK src_file_remove RESUME;
ALTER TASK dim_weathertype_landing_to_staging RESUME;
ALTER TASK stream_to_dim_weathertype_landing RESUME;
ALTER TASK select_distinct RESUME;

#######################################################################################################

//check what task has run
select * from table(information_schema.task_history()); 