# Data Processing

Spark Structures Streaming

spark_kafka_streaming.py corresponds to complete mode in spark structured streaming. In this mode more data is transfered to kafka at each trigger and this can slow down the real time system.

sssl_low_latency.py file for counting comments per post with travel related words. This uses append mode, reduced amount of data transfer to kafka.

sssl_low_latency_agg.py file for counting the number of page views with append mode. watermarking used to discard old data.
