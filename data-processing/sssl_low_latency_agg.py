from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, window, countDistinct
from pyspark.sql.functions import split, from_json, col, to_json, struct
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
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
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3n.awsSecretAccessKey", "awsSecretAccessKey")
    spark.conf.set("spark.sql.shuffle.partitions", "6")
    # source
    print('source')
    schema = StructType()\
        .add('post', StringType())\
        .add('subreddit', StringType())\
        .add('timestamp', TimestampType())\
        .add('body', StringType())\
        .add('author', StringType())\
        .add('views', IntegerType())
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
    #
    lines = lines.selectExpr("CAST(value AS STRING) as json").select(
        from_json(col("json"), schema=schema).alias('data')).select('data.*')
    lines.printSchema()

    print('source selected')
    # query
    print('query')
    lines.createOrReplaceTempView("updates")
    lines_filtered = spark.sql("SELECT post FROM updates WHERE body LIKE '%vacation%'\
                      OR body LIKE '%holiday%'\
                      OR body LIKE '%beach%'\
                      OR body LIKE '%urope%'\
                      OR body LIKE '%trip%'\
                      OR body LIKE '%tired%'\
                      OR body LIKE '%work%'\
                      OR body LIKE '%fatigue%'\
                      OR body LIKE '%overwork%'\
                      OR body LIKE '%party%'\
                      OR body LIKE '%fun%'\
                      OR body LIKE '%weekend%'\
                      OR body LIKE '%ecember%'\
                      OR body LIKE '%ummer%'\
                      OR body LIKE '%ingapore%'\
                      OR body LIKE '%alaysia%'\
                      OR body LIKE '%hailand%'\
                      OR body LIKE '%affari%'\
                      OR body LIKE '%kids%'\
                      OR body LIKE '%lions%'\
                      OR body LIKE '%event%'\
                      OR body LIKE '%ingapore%'\
                      OR body LIKE '%bored%'\
                      OR body LIKE '%happy%'\
                      OR body LIKE '%excited%'\
                      OR body LIKE '%sad%'\
                      OR body LIKE '%breakup%'\
                      OR body LIKE '%wedding%'\
                      OR body LIKE '%visit%'\
                      OR body LIKE '%no time%'\
                      OR body LIKE '%car%'\
                      OR body LIKE '%road%'\
                      OR body LIKE '%bonus%'\
                      OR body LIKE '%tan%'\
                      OR body LIKE '%road-trip%'\
                      OR body LIKE '%girl friend%'\
                      OR body LIKE '%bus%'\
                      OR body LIKE '%train%'\
                      OR body LIKE '%motel%'\
                      OR body LIKE '%visit%'\
                      OR body LIKE '%mother%'\
                      OR body LIKE '%father%'\
                      OR body LIKE '%parents%'\
                      OR body LIKE '%thanks giving%'\
                      OR body LIKE '%long week%'")

    rowCounts = lines.withWatermark("timestamp", "100 milliseconds").groupBy('post', window('timestamp', '1000 milliseconds',
                                                                                            '1000 milliseconds')).agg(F.sum(lines.views).alias('count'))  # .orderBy('count', ascending=0)
    # join dataframes , inner join
    rowCounts.join(lines_filtered, 'post')

    print('sink')
    query = (rowCounts
             .select(to_json(struct("post", "window", "count")).cast("string").alias("value"))
             .writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", "10.0.0.5:9092")
             .option("topic", "reddit-spark-topic")
             .option("checkpointLocation", "s3n://aspk-reddit-posts/checkpoint/")
             .outputMode("append")
             .start()
             )

    query.awaitTermination()
    return


if __name__ == '__main__':
    main()
