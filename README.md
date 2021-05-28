[//]: # (Image References)

[image1]: ./images/star_schema_model_songplays.png "Model"
[image2]: ./images/filepaths.png "Filepaths songdata"
[image3]: ./images/song_file_example.png "Song file example"
[image4]: ./images/filepaths_logdata.png "Filepaths logdata"
[image5]: ./images/log_file_example.png "Log file example"
[image6]: ./images/notebook.png "Notebook"
[image7]: ./images/run_notebook.png "Run notebook"
[image8]: ./images/star_schema_model_songplays.png "Star Schema"
[image9]: ./images/result_1.png "Result"
[image10]: ./images/result_null.png "Result null"
[image11]: ./images/result_count.png "Result count"
[image12]: ./images/dwh_cfg.png "config"
[image13]: ./images/ssh_windows.png "ssh_windows"
[image14]: ./images/ssh_linux.png "ssh_linux"



# Project: Data Warehouse hosted on Amazon S3
This project is part of the [Data Engineering Nanodegree Program](https://www.udacity.com/course/data-engineer-nanodegree--nd027), by [Udacity](https://www.udacity.com/).  

## Introduction  

A music streaming startup, Sparkify, has grown its user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I was tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights into what songs their users are listening to.

It is possible to test the database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Project Description  

In this project I am using Spark to read songs and logs data from AWS S3 and consolidating it in a Star Schema that helps the client with data analysis. This spark data run on a EMR cluste on AWS.

# Star Schema for Song Play Analysis
For this project, I read songs and logs data form S3 bucket and consolidate it on a Star Schema compose by one fact table and 4 dimensions, the time dimension could be a corporative table but in this case only contains data from logs source, to simplify the process and consume less storage.

Star and snowflake schemas use the concepts of **fact** and **dimension** tables to make getting value out of the data easier.  

- **Fact table** consists of measurements, metrics or facts a business process.
- **Dimension table** a structure that categorizes facts and measures in order to enable users to answer business questions.
	         
## Data modeling

For this project, I modeled a star-schema that includes one fact table and four dimension tables. See the descriptions below:

**Fact Table**  

- **songplays:** records in log data associated with song plays. The log data was filtered with page = 'NextSong'.  

	- Columns: songplay\_id, start\_time, user\_id, level, song\_id, artist\_id, session\_id, location, user\_agent.  
	- Distribution strategy: partitioned parquet files by **year and month** in table directories on S3

**Dimension Tables**  

- **users:** users in the app.  
	- Columns: user\_id, first\_name, last\_name, gender, level.
	- Distribution strategy: partitioned parquet files by **user\_id** in table directories on S3

- **songs:** songs in music database.  
	- Columns: song\_id, title, artist\_id, year, duration  
	- Distribution strategy: partitioned parquet files by **year and artist\_id** in table directories on S3

- **artists:** artists in music database.
	- Columns: artist\_id, name, location, latitude, longitude  
	- Distribution strategy: partitioned parquet files by **artist\_id** in table directories on S3

- **time:** timestamps of records in songplays broken down into specific units.
	- Columns: start\_time, hour, day, week, month, year, weekday  
	- Distribution strategy: partitioned parquet files by **year and month** in table directories on S3
	
# Project Datasets  

## Datasets

The source datasets are in the udacity S3 bucket and listed bellow:
	- Song data: s3://udacity-dend/song\_data  
	- Log data: s3://udacity-dend/log\_data  


### Song Dataset

The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

### Log Dataset  
 
The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.



# ETL pipeline

The script **etl.py** runs without any graphic interface and is organized like below:

- **etl.py file:**  
	- imports the necessary packages
	- defines functions to:
        - creates a spark session
		- read the data files (log\_data and song\_data)
		- extract columns to create tables
		- write the tables on S3
    - main function:
        - Execs all other functions in order.
	

# Getting Started
## Files included in this repository  
The project includes six files:  

 1.  **etl.py:** reads and processes files from song\_data and log\_data and loads them into your tables. 
 2. **README.md:** provides discussion about this project. 
 3. ***dl.cfg*** configuration files that provides credentials to read s3 bucket.

## Preparing the environment to run the project

**1.** On [AWS Console] creates a EMR Cluster

- On EMR you will need install
	+ pip3 install pandas  
	+ pip3 install pyspark
- Set Python 3 for pyspark
    + sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3'

# Running the project  

To run this project:  

- Run the etl.py in your EMR environment by writing the command bellow:
	+ python3 etl.py