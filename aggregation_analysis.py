from kafka_stream_reader import get_movies_stream
from pyspark.sql.functions import explode, col, avg
from pyspark.sql import DataFrame

movies, spark = get_movies_stream()

genres_df = movies.withColumn("genre", explode(col("genres")))

genre_avg = genres_df.groupBy("genre") \
                     .agg(avg("rating").alias("avg_rating"))

def upsert_to_postgres(batch_df: DataFrame, batch_id: int):

    rows = batch_df.collect()

    import psycopg2
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
            INSERT INTO genre_avg (genre, avg_rating)
            VALUES (%s, %s)
            ON CONFLICT (genre)
            DO UPDATE SET avg_rating = EXCLUDED.avg_rating;
        """, (row["genre"], row["avg_rating"]))

    conn.commit()
    cur.close()
    conn.close()

query = genre_avg.writeStream \
    .outputMode("complete") \
    .foreachBatch(upsert_to_postgres) \
    .option("checkpointLocation", "C:\\checkpoints") \
    .start()

query.awaitTermination()
