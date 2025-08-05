import sys
import os
import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession

def create_spark_session():
    """Initialize Spark session with PostgreSQL JDBC driver."""
    return SparkSession.builder \
        .appName("SpotifyDataLoad") \
        .getOrCreate()

def create_postgres_tables():
    """Create PostgresSQL tables if they don't exist using psycopg2"""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname = "postgres",
            user = "postgres",
            password = "postgres",
            host = "localhost",
            port = "5432"
        )
        cursor = conn.cursor()

        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS master_table (
                track_id VARCHAR(50),
                track_name TEXT,
                track_popularity INTEGER,
                artist_id VARCHAR(50),
                artist_name TEXT,
                followrrs FLOAT,
                genres TEXT,
                artist_popularity INTEGER,
                danceability FLOAT,
                energy FLOAT,
                tempo FLOAT,
                related_id TEXT[]
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS recommendations_exploded (
                id VARCHAR(50)
                related_id VARCHAR(50)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS artist_track (
                    id VARCHAR (50) ,
                    artist_ id VARCHAR(50)
            ) ;
            """,
            """
            CREATE TABLE IF NOT EXISTS track_metadata (
                id VARCHAR (50) PRIMARY KEY,
                name TEXT, popularity INTEGER,
                duration ms INTEGER,
                danceability FLOAT,
                energy FLOAT,
                tempo FLOAT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS artist metadata (
                id VARCHAR(50) PRIMARY KEY,
                name TEXT,
                followers FLOAT,
                popularity INTEGER
            ) ;
            """
        ]

        for query in create_table_queries:
            cursor.execute (query)
        conn.commit ()
        print ("PostgreSQL tables created successfully")
           
    except Exception as e:
        print(f"Error creating tables: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close ()
   
def load_to_postgres (spark, input_dir):
    """Load Parquet files to PostgreSQL."""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    tables = [
        ("stage2/master_table", "master_table"),
        ("stage/recommendations_exploded", "recommendations_exploded"),
        ("stage/artist_track","artist_track"),
        ("stage/track_metadata","track_metadata"),
        ("stage/artist_metadata", "artist_metadata")
    ]

    for parquet_path, table_name in tables:
        try:
            df = spark.read.parquet(os.path.join(input_dir, parquet_path))
            mode = "append" if 'master' in parquet_path else "overwrite"
            df.write \
                .mode (mode) \
                .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            print (f"Loaded {table_name} to PostgreSQL")
        except Exception as e:
            print(f"Error loading {table_name}: {e}")

if __name__ == "__main__":
    if len (sys.argv) != 2:
        print("Usage: python load/execute.py <input_dir>")
        sys.exit (1)

    input_dir = sys.argv [1]

    if not os.path.exists (input_dir):
        print(f"Error: Input directory (input_dir) does not exist")
        sys.exit (1)

    spark = create_spark_session()
    create_postgres_tables()
    load_to_postgres (spark, input_dir)

    print("Load stage completed" )