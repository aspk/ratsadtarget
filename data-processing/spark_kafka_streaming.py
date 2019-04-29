from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, window, countDistinct
from pyspark.sql.functions import split, from_json, col, to_json, struct
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
import time


def main():
    # define spark session
    print('define session')
    spark = (SparkSession
             .builder
             .appName("Number of comments")
             .getOrCreate()
             )
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "awsAccessKeyId")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "awsSecretAccessKey")
    # source
    print('source')
    schema = StructType()\
        .add('post', StringType())\
        .add('subreddit', StringType())\
        .add('timestamp', TimestampType())\
        .add('body', StringType())\
        .add('author', StringType())
    lines = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "10.0.0.5:9092")
        .option("subscribe", "reddit-stream-topic")
        .option("failOnDataLoss", "false")
        .option("startingOffsets", "latest")
        .load()
    )
    lines = lines.selectExpr("CAST(value AS STRING) as json").select(
        from_json(col("json"), schema=schema).alias('data')).select('data.*')
    lines.printSchema()

    print('source selected')
    print('query')
    lines.createOrReplaceTempView("updates")
    lines = spark.sql(
        "SELECT * FROM updates WHERE body LIKE '%vacation%'or body LIKE '%holiday%' ")

    rowCounts = lines.groupBy('post', window('timestamp', '1 seconds',
                                             '1 second')).count().orderBy('count', ascending=0)

    # sink
    print('sink')
    query = (rowCounts
             .select(to_json(struct("post", "window", "count")).cast("string").alias("value"))
             .writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", "10.0.0.5:9092")
             .option("topic", "reddit-spark-topic")
             .option("checkpointLocation", "s3n://aspk-reddit-posts/checkpoint/")
             .outputMode("complete")
             .start()
             )

    query.awaitTermination()
    return


if __name__ == '__main__':
    main()
