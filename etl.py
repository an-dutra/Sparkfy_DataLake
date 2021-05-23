# -*- coding: UTF-8 -*-
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

clean_output_tables = False

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

save_mode = 'overwrite' if clean_output_tables else 'ignore'

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = f"{input_data}/song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df \
        .filter(df.artist_id.isNotNull()) \
        .select("song_id", "title", "artist_id", "year", "duration") \
        .distinct()

    # set filepath to output song data
    song_path = f"{output_data}/song"

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(song_path, mode=save_mode, partitionBy=('year', 'artist_id'))

    # extract columns to create artists table
    artists_table = df \
        .filter(df.artist_id.isNotNull()) \
        .select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")\
        .withColumnRenamed("artist_name","name")\
        .withColumnRenamed("artist_location","location")\
        .withColumnRenamed("artist_latitude","latitude")\
        .withColumnRenamed("artist_longitude","longitude")

    # set filepath to output artist data
    artist_path = f"{output_data}/artist/"

    # write artists table to parquet files
    artists_table.write.parquet(artist_path, mode=save_mode)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f"{input_data}/log-data/*.json"

    # read log data file
    log_dataframe = spark.read.json(log_data)

    # filter by actions for song plays
    log_dataframe = log_dataframe.filter("page == 'NextSong'").dropDuplicates()


    # extract columns for users table
    users_table = log_dataframe\
        .filter(log_dataframe.userId.isNotNull())\
        .select("userId", "firstName", "lastName", "gender", "level")\
        .distinct()\
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("firstName", "first_name")\
        .withColumnRenamed("lastName", "last_name")

    # set user table path
    users_table_path = f"{output_data}/user/"

    # write users table to parquet files
    users_table.write.parquet(users_table_path, mode=save_mode)

    # create timestamp column from original timestamp column

    get_timestamp = udf(lambda x: x // 1000)
    time_dataframe = log_dataframe.withColumn('timestamp', get_timestamp('ts')).select('timestamp')

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x))
    time_dataframe = time_dataframe.withColumn('datetime', get_datetime('timestamp'))
    time_dataframe.show()

    # extract columns to create time table
    #start_time, hour, day, week, month, year, weekday
    time_table =
#
#     # write time table to parquet files partitioned by year and month
#     time_table
#
#     # read in song data to use for songplays table
#     song_df =
#
#     # extract columns from joined song and log datasets to create songplays table
#     songplays_table =
#
#     # write songplays table to parquet files partitioned by year and month
#     songplays_table


def main():
    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    input_data = "./data/"
    output_data = "./output_data/"

    # process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()