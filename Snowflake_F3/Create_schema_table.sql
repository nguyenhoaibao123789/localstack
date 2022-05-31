USE S3JSON
#######################################################################################################

CREATE TABLE IF NOT EXISTS Dim_City( 
  id_city int,
  City_name STRING, 
  Latitude FLOAT, 
  Longtitude FLOAT,
  Country STRING,
  Timezone INT
);

CREATE TABLE IF NOT EXISTS Dim_City_landing( 
  id_city int,
  City_name STRING, 
  Latitude FLOAT, 
  Longtitude FLOAT,
  Country STRING,
  Timezone INT
);

CREATE TABLE IF NOT EXISTS Dim_City_staging( 
  id_city int,
  City_name STRING, 
  Latitude FLOAT, 
  Longtitude FLOAT,
  Country STRING,
  Timezone INT,
  start_time TIMESTAMP_NTZ,
  end_time TIMESTAMP_NTZ,
  current_flag_status STRING
);

#######################################################################################################
//drop table Dim_WeatherType_landing
CREATE TABLE IF NOT EXISTS Dim_WeatherType( 
  id_wtype int primary key,
  weather_description STRING,
  weather_type string
);

CREATE TABLE IF NOT EXISTS Dim_WeatherType_landing( 
  id_wtype int primary key unique,
  weather_description STRING,
  weather_type string
);

CREATE TABLE IF NOT EXISTS Dim_WeatherType_staging( 
  id_wtype int,
  weather_description STRING,
  weather_type string,
  start_time TIMESTAMP_NTZ,
  end_time TIMESTAMP_NTZ,
  current_flag_status STRING
);
#######################################################################################################

CREATE TABLE IF NOT EXISTS Fact_Weather1Hour
(
  id_city int,
  id_wtype int,
  date datetime,
  sunset datetime,
  sunrise datetime,
  feels_like float,
  humidity int,
  pressure int,
  temp float,
  temp_max float,
  temp_min float,
  visibility int,
  wind_speed float,
  wind_direction int
);