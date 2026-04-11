import kagglehub
from kagglehub import KaggleDatasetAdapter
import shutil
from pathlib import Path
import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
import requests

# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------

def get_live_usd_to_eur():
    """
    Fetch the real-time USD to EUR exchange rate using the Frankfurter API.
    Returns a float value of the rate, or a fallback value if the request fails.
    """
    try:
        # Endpoint for the latest exchange rates from USD to EUR
        url = "https://api.frankfurter.app/latest?from=USD&to=EUR"
        response = requests.get(url)
        data = response.json()

        # Extract the EUR rate from the response payload
        rate = data['rates']['EUR']
        print(f"Successfully fetched live rate: 1 USD = {rate} EUR")
        return rate
    except Exception as e:
        # Log the error and return a fallback rate to ensure pipeline continuity
        print(f"Failed to fetch exchange rate: {e}")
        return 0.92


# -----------------------------------------------------------------------------
# Bronze ingestion
# -----------------------------------------------------------------------------

def download_to_datalake_bronze():
    """Download the dataset from Kaggle and move files into the bronze datalake."""
    # Source Kaggle dataset for GDP of European countries
    download_path = kagglehub.dataset_download("samuelcortinhas/gdp-of-european-countries")
    source_path = Path(download_path)
    datalake_path = Path("/app/datalake/bronze")
    datalake_path.mkdir(parents=True, exist_ok=True)

    for file_path in source_path.iterdir():
        if file_path.is_file():
            destination = datalake_path / file_path.name
            shutil.move(str(file_path), str(destination))
            print(f"Moved: {file_path.name}")


# -----------------------------------------------------------------------------
# Silber / warehouse transformation
# -----------------------------------------------------------------------------

def convert_csv_to_parquet_silber_postgre_warehouse():
    """Read the bronze CSV, convert USD values to EUR, write parquet, and load to Postgres."""
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Postgres-Write") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
        .getOrCreate()
    # Alternative local jar option if package download is not available:
    # .config("spark.jars", "/app/postgresql-42.7.2.jar") \

    # Define a fixed schema for the input GDP CSV file
    schema = types.StructType([
        types.StructField('year', types.LongType(), True),
        types.StructField('Belgium', types.LongType(), True),
        types.StructField('France', types.LongType(), True),
        types.StructField('Germany', types.LongType(), True),
        types.StructField('Italy', types.LongType(), True),
        types.StructField('Poland', types.LongType(), True),
        types.StructField('Spain', types.LongType(), True)
    ])

    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv('/app/datalake/bronze/GDP_table.csv')

    # Fetch the current USD to EUR conversion rate from the API
    current_rate = get_live_usd_to_eur()

    # Convert USD values to EUR for each country column
    df_euro = df \
        .withColumn('Belgium', (F.col('Belgium') * current_rate).cast('long')) \
        .withColumn('France', (F.col('France') * current_rate).cast('long')) \
        .withColumn('Germany', (F.col('Germany') * current_rate).cast('long')) \
        .withColumn('Italy', (F.col('Italy') * current_rate).cast('long')) \
        .withColumn('Poland', (F.col('Poland') * current_rate).cast('long')) \
        .withColumn('Spain', (F.col('Spain') * current_rate).cast('long'))

    # Repartition before writing to improve parallel output performance
    df_euro = df_euro.repartition(24)

    # Write transformed data into the silber parquet location
    df_euro.write \
        .mode('overwrite') \
        .parquet('/app/datalake/silber')

    # PostgreSQL connection configuration for loading into the warehouse
    url = "jdbc:postgresql://pgdatabase:5432/eu_gdp"
    properties = {
        "user": "root",
        "password": "root",
        "driver": "org.postgresql.Driver"
    }

    # Write the transformed Spark DataFrame into the Postgres table
    df_euro.write.jdbc(
        url=url,
        table="eu_gdp",
        mode="overwrite",
        properties=properties
    )


if __name__ == "__main__":
    # Execute the pipeline end-to-end when run as a script
    download_to_datalake_bronze()
    convert_csv_to_parquet_silber_postgre_warehouse()