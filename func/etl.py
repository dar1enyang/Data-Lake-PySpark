import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T
import sys
sys.path.append('..')
from schema.schema import *

config = configparser.ConfigParser()
config.read('../config/dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']     = config.get("AWS", 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get("AWS", 'AWS_SECRET_ACCESS_KEY')



def create_spark_session():
    """
    The function to create Spark session
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data):
    """
    The function to process song data
    
    Parameters:
        spark  : The Spark session that will be used to execute commands.
        input_data : The file path resides files to be processed.
    """
    
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(song_id, title, artist_id, year, duration)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet("songs_table")

    # extract columns to create artists table
    artists_table = df.select(artist_id, name, location, latitude, longitude)
    
    # write artists table to parquet files
    artists_table.write.parquet("artists_table")


def process_log_data(spark, input_log_data, input_song_data):
    """
    The function to process log data and song data
    
    Parameters:
        spark  : The Spark session that will be used to execute commands.
        input_log_data : The file path resides log files to be processed.
        intput_song_data : The file path resides song files to be processed.
    """
    
    # get filepath to log data file
    log_data = input_log_data
    
    # read log data file
    df_log = spark.read.json(log_data)
    
    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    
    # create datetime columns from original timestamp column
    get_hour = F.udf(lambda x: x.hour, T.IntegerType()) 
    get_day = F.udf(lambda x: x.day, T.IntegerType()) 
    get_week = F.udf(lambda x: x.isocalendar()[1], T.IntegerType()) 
    get_month = F.udf(lambda x: x.month, T.IntegerType()) 
    get_year = F.udf(lambda x: x.year, T.IntegerType()) 
    get_weekday = F.udf(lambda x: x.weekday(), T.IntegerType()) 
    
    df_log = df_log.withColumn("hour", get_hour(df_log.timestamp))
    df_log = df_log.withColumn("day", get_day(df_log.timestamp))
    df_log = df_log.withColumn("week", get_week(df_log.timestamp))
    df_log = df_log.withColumn("month", get_month(df_log.timestamp))
    df_log = df_log.withColumn("year", get_year(df_log.timestamp))
    df_log = df_log.withColumn("weekday", get_weekday(df_log.timestamp))
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log["page"] == "NextSong")

    # extract columns for users table    
    users_table = df_log.select(user_id, first_name, last_name, gender, level)
    
    # write users table to parquet files
    users_table.write.parquet("users_table")
    
    # extract columns to create time table
    time_table = df_log.select(start_time, hour, day, week, month, year, weekday)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet("time_table")
    
    # get filepath to song data file
    song_data = input_song_data
    
    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)
    song_df = song_df.select(song_id, artist_id, name, title)
    df_log2 = df_log.select(start_time, user_id, level, session_id, locationSP, user_agent, song, artist, month, year)
    
    # extract columns from joined song and log datasets to create songplays table  
    songplays_table = song_df.join(df_log2, (song_df.artist_name == df_log2.artist) & (song_df.title == df_log2.song))
    songplays_table = songplays_table.select(start_time, user_id, level, song_id, artist_id, session_id, locationSP, user_agent, month, year)
    songplays_table = songplays_table.withColumn("songplay_id", F.monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet("songplays_table")


def main():
    spark = create_spark_session()
    input_song_data = 's3a://udacity-dend/song_data/*/*/*/*.json'
    input_log_data = 's3://udacity-dend/log_data/*/*/*.json'
    
    process_song_data(spark, input_song_data)    
    process_log_data(spark, input_log_data, input_song_data)


if __name__ == "__main__":
    main()
