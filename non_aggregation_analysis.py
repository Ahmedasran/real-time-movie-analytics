from kafka_stream_reader import get_movies_stream
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
import psycopg2

movies, spark = get_movies_stream()


high_rated = movies.filter(col("rating") >= 3.5) \
                   .select("movieId", "title", "rating")


def insert_high_rated(batch_df: DataFrame, batch_id: int):

    rows = batch_df.collect()

    if len(rows) == 0:
        return  

    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="moviesdb",
        user="postgres",
        password="1234"
    )
    cur = conn.cursor()

    for row in rows:
        cur.execute("""
            INSERT INTO high_rated_movies (movieId, title, rating)
            VALUES (%s, %s, %s);
        """, (row["movieId"], row["title"], row["rating"]))

    conn.commit()
    cur.close()
    conn.close()


query = high_rated.writeStream \
    .outputMode("append") \
    .foreachBatch(insert_high_rated) \
    .option("checkpointLocation", "C:\\checkpoints") \
    .start()

query.awaitTermination()
