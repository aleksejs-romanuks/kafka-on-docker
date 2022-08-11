from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from pyspark.sql.functions import when

spark = SparkSession.builder.appName("air-quality-data-partitioning").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# rdd = spark.sparkContext.parallelize(["""
# {'product': 'civillight', 'init': '2022081106', 'dataseries': [{'date': 20220811, 'weather': 'mcloudy', 'temp2m': {'max': 21, 'min': 17}, 'wind10m_max': 4}, {'date': 20220812, 'weather': 'clear', 'temp2m': {'max': 22, 'min': 16}, 'wind10m_max': 3}, {'date': 20220813, 'weather': 'clear', 'temp2m': {'max': 20, 'min': 14}, 'wind10m_max': 2}, {'date': 20220814, 'weather': 'clear', 'temp2m': {'max': 22, 'min': 16}, 'wind10m_max': 2}, {'date': 20220815, 'weather': 'cloudy', 'temp2m': {'max': 23, 'min': 17}, 'wind10m_max': 3}, {'date': 20220816, 'weather': 'rain', 'temp2m': {'max': 22, 'min': 18}, 'wind10m_max': 3}, {'date': 20220817, 'weather': 'rain', 'temp2m': {'max': 18, 'min': 18}, 'wind10m_max': 3}]}
# """])

# bla = spark.read.json(rdd)

# bla.show()
# print(bla.schema)

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /home/ubuntu/kafka-bootcamp/spark/streaming.py

schema = StructType([StructField('dataseries', ArrayType(StructType([StructField('date', LongType(), True), StructField('temp2m', StructType([StructField('max', LongType(), True), StructField('min', LongType(), True)]), True), StructField('weather', StringType(), True), StructField('wind10m_max', LongType(), True)]), True), True), StructField('init', StringType(), True), StructField('product', StringType(), True)])

def main():
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("startingOffsets", "earliest") \
        .option("subscribe", "weather") \
        .load()

    valuesDF = df.selectExpr("CAST(value AS STRING)")
    structDF = valuesDF.select(
        from_json(col("value"), schema).alias("data")
    )
    

    resultDF = structDF \
        .withColumn("datasery", explode(col("data.dataseries"))) \
        .select(
            col("datasery.date").alias("date"),
            col("datasery.wind10m_max").alias("wind10m_max"),
        ).withColumn("category", 
            when(col("wind10m_max") == 1,"calm") 
            .when(col("wind10m_max") == 2,"light") 
            .when(col("wind10m_max") == 3, "moderate") 
            .when(col("wind10m_max") == 4, "fresh") 
            .when(col("wind10m_max") == 5, "strong") 
            .when(col("wind10m_max") == 6, "gale") 
            .when(col("wind10m_max") == 7, "storm") 
            .when(col("wind10m_max") == 8, "hurricane") 
            .otherwise("???") 
        )

    resultDF \
        .writeStream \
        .format("console") \
        .start() \
        .awaitTermination()

    

if __name__ == '__main__':
  main()