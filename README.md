# Sparkify <img src='https://s3.amazonaws.com/video.udacity-data.com/topher/2018/May/5b06cfa8_3-4-p-query-a-digital-music-store-database1/3-4-p-query-a-digital-music-store-database1.jpg' align="right" height="140" />

This is the fifth project of Udacitys Data Engineering Nanodegree. In this project, [Apache Airflow](https://airflow.apache.org/) is introduced to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.

Here are other **Sparkify** projects built on different database:

[Data Modeling: PostgreSQL](https://github.com/kevinkevin556/Sparkify-Postgres)

[Data Warehouse: Redshift](https://github.com/kevinkevin556/Sparkify-Redshift)

## Overview

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

In this project,

- Data warehousing is implemented with Redshift and S3.
- Automatically run the ETL pipeline controlled by [Apache Airflow](https://airflow.apache.org/)

![Working DAG with correct task dependencies](https://github.com/kevinkevin556/Sparkify-Airflow/blob/master/image/example-dag.png?raw=true)

### Dimension Tables

- users: users in the app.
- songs: songs in the music database.
- artists: artists in the music database.
- time: timestamps of records in `songplays` broken down into specific units.

### Fact Table

- songplays: records in event data associated with song plays i.e. records with page `NextSong`

### ETL Pipeline

- transfers data from [AWS S3 bucket](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/) into the tables using Redshift
  - Song data: `s3://udacity-dend/song_data`
  - Log data: `s3://udacity-dend/log_data` which should be formatted by following the `s3://udacity-dend/log_json_path.json` json format

## Getting Started

### Prerequisites

Apache Airflow should be installed first.

```console
foo@bar:~$ pip install apache-airflow
```

You can find more information from the [official document](https://airflow.apache.org/docs/stable/installation.html).

### Perpare dag and Run Airflow

1. Initialize airflow database and download dags

Open the terminal window and type the following commands:

```console
foo@bar:~$ airflow initdb
DB: sqlite:////home/user/airflow/airflow.db
...
```

You can find the path of airflow home directory. Open configuration file `airflow.cfg` in the directory, check out the dag directory (e.g. `/home/user/airflow/dag/`)
Download all contents in the dag folder from this repo into you dag directory. Also, download the `plugin` folder and put it under airflow home directory.

2. Turn on the webserver

```
foo@bar:~$ airflow webserver -p 8080
```

3. Start another terminal to run the scheduler:

```console
foo@bar:~$ airflow scheduler
```

4. Open your browser window and visit `localhost:8080`to access airflow UI


### Set up AWS credentials and a Redshift cluster

Feel free to use your own setting. If you are not sure about how to create AWS credentials or Redshift cluster, [this link](https://github.com/kevinkevin556/Sparkify-Redshift#start-an-redshift-cluster) may be helpful.If you are not familiar with the set-up procedures, you can follow the steps below to complete your configuration. 

1. Click on the **Admin** tab and select **Connections**.
2. Under **Connections**, select **Create**.
3. On the create connection page, enter the following values:
    - **Conn Id**: Enter `aws_credentials`.
    - **Conn Type**: Enter `Amazon Web Services`.
    - **Login**: Enter your **Access key ID** from the IAM User credentials you downloaded earlier.
    - **Password**: Enter your **Secret access key** from the IAM User credentials you downloaded earlier.

    Once you've entered these values, select `Save and Add Another`.

4. On the next create connection page, enter the following values:
    - **Conn Id**: Enter `redshift`.
    - **Conn Type**: Enter `Postgres`.
    - **Host**: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your **cluster** in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to **NOT** include the port at the end of the Redshift endpoint string.
    - Schema: Enter dev. This is the Redshift database you want to connect to.
    - **Login**: Enter `awsuser`.
    - **Password**: Enter the password you created when launching your Redshift cluster.
    - Port: Enter `5439`.

    Once you've entered these values, select `Save`.