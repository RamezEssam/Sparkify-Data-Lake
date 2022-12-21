import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark session configured to use AWS packages to read from and write to S3
    
    inputs:
        None
    Outputs:
        Spark session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the song data log files and creates the song and artist tables then writes 
    them to parquet files in S3
    
    inputs:
        spark: spark session object
        input_data: path to song data on s3
        ouptut_data: path to s3 bucket that will store the output parquet files
    outputs:
        None
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", 
                             "duration"]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.parquet(output_data+'songs/'+'songs.parquet', 
                                            partitionBy=['year','artist_id'])

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", 
                               "artist_latitude", "artist_longitude"]).dropDuplicates()
    
    # write artists table to parquet files
    artists_table = artists_table.write.parquet(output_data + 'artists/' + 
                                                'artists.parquet', partitionBy=
                                                ['artist_id'] )


def process_log_data(spark, input_data, output_data):
    """
    Process the log data log files and creates the users, time, and songplayes tables 
    then writes them to parquet files in S3
    
    inputs:
        spark: spark session object
        input_data: path to log data on s3
        ouptut_data: path to s3 bucket that will store the output parquet files
    outputs:
        None
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where('page="NextSong"')

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", 
                                 "level"]).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/' + 'users.parquet', partitionBy = 
                              ['userId'])

    # create timestamp column from original timestamp column
    df = df.withColumn('timestamp',((df.ts.cast('float')/1000).cast("timestamp")))
    
    
    # extract columns to create time table
    time_table = df.select(
                    F.col("timestamp").alias("start_time"),
                    F.hour("timestamp").alias('hour'),
                    F.dayofmonth("timestamp").alias('day'),
                    F.weekofyear("timestamp").alias('week'),
                    F.month("timestamp").alias('month'), 
                    F.year("timestamp").alias('year'), 
                    F.date_format(F.col("timestamp"), "E").alias("weekday")
                )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time/' + 'time.parquet', partitionBy=
                             ['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (log_df.song == song_df.title) & 
                                  (log_df.artist == song_df.artist_name) & 
                                  (log_df.length == song_df.duration), how='inner')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays/' + 
                                  'songplays.parquet',partitionBy=['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-analytics-bucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
