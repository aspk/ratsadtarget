# Project-Title

RATS: Realtime Ad Target System. A data pipeline that ingests real time social media comment data and processes it with real time SQL queries.   

[Presentation Link](https://docs.google.com/presentation/d/1qvnA2c4Qu6kgrOteNES9WV5V-3PNUv7GGz0yRwIK_MU/edit?usp=sharing) 

<hr/>

## How to install and get it up and running
Cluster setup donw with [pegasus](https://github.com/InsightDataScience/pegasus) on AWS.

Create a VPC

Create a public subnet within the VPC for 11 instances ( remember to increase the Elastic IP limit)

Install virtual environments on instances with python3, Tmux and kafka-python


Cluster specs :

1. Kafka cluster - 4 nodes m4.2xlarge

2. Spark cluster - 1 master, 4 slaves m4.2xlarge

3. PostgreSQL instance m4.xlarge

4. Instance for running producer and consumer m4.4xlarge

<hr/>

## Introduction

Real time ad bidding industry requires high throughput pipelines that can process social media or user web session data with ultra low latencies. In this project I designed a pipeline to process real time streams of social media comment data to figure out the best place to post ads.

## Architecture
Data is ingested through AWS S3 by a python producer into a  Kafka topic. This data is then fed to  Spark Structured Streaming for aggregation using Real time Spark SQL queries. The data is ingested into a new Kafka topic and then python consumers write the data to a PostgreSQL database.

![alt_text](https://i.imgur.com/NWmIh8p.png)

## Dataset
Reddit.com comment dataset downloaded from [Pushshift](https://files.pushshift.io/reddit/comments/)

Each month dataset is ~ 8 GB compressed and 150 GB uncompressed. 

Each comment is stored in json format with the multiple keys of which following are used:
1. post
2. subreddit
3. body
4. timestamp
5. author

A wildcard is used to filter for certain words on the body of the comment. 

## Engineering challenges

Increasing producer throughput
Each producer sends data to kafka at the rate of 700 messages/s (acks = 1, as I dont want to loose any messages). 
To increase the throughput (messages/s) I increased the the number of producers parallely writing to multiple kafka partitions.

![alt text](https://imgur.com/uEljLI4.png)

Increasing consumer throughput
Each consumer can write to postgres at a rate of ~1000 messages/s. To increase the write speed I used multiple kafka partitions and multiple consumers in a consumer group.

![alt text](https://imgur.com/sFtM2y4.png)


[//]:# (## Trade-offs)
