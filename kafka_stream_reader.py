from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

def get_movies_stream():
    spark = SparkSession.builder \
        .appName("MoviesAnalysis") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4") \
        .getOrCreate()

    schema = StructType([
        StructField("tmdbId", StringType(), True),
        StructField("rating_time", StringType(), True),
        StructField("genres", ArrayType(StringType()), True),
        StructField("imdbId", StringType(), True),
        StructField("rating", FloatType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("userId", IntegerType(), True),
        StructField("release_year", IntegerType(), True)
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "movies_cleaned") \
        .load()

    movies = df.selectExpr("CAST(value AS STRING)") \
               .select(from_json(col("value"), ArrayType(schema)).alias("data")) \
               .selectExpr("inline(data)")

    return movies, spark
