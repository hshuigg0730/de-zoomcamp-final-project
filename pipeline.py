import kagglehub
from kagglehub import KaggleDatasetAdapter
import shutil
from pathlib import Path
import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
import requests

def get_live_usd_to_eur():
    """
    Fetches the real-time USD to EUR exchange rate using the Frankfurter API.
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


def download_to_datalake_bronze():
    # https://www.kaggle.com/datasets/samuelcortinhas/gdp-of-european-countries/data
    # Download latest version
    download_path = kagglehub.dataset_download("samuelcortinhas/gdp-of-european-countries")
    source_path = Path(download_path)
    datalake_path = Path("/app/datalake/bronze")
    datalake_path.mkdir(parents=True, exist_ok=True)

    for file_path in source_path.iterdir():
        if file_path.is_file():
            destination = datalake_path / file_path.name
            
            shutil.move(str(file_path), str(destination))
            print(f"Moved: {file_path.name}")

def convert_csv_to_parquet_silber_postgre_warehouse():
    spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Postgres-Write") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()
    # .config("spark.jars", "/app/postgresql-42.7.2.jar") \
    

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

    # 1. Fetch the dynamic exchange rate via API
    current_rate = get_live_usd_to_eur()
    
    df_euro = df \
        .withColumn('Belgium', (F.col('Belgium') * current_rate).cast('long')) \
        .withColumn('France', (F.col('France') * current_rate).cast('long')) \
        .withColumn('Germany', (F.col('Germany') * current_rate).cast('long')) \
        .withColumn('Italy', (F.col('Italy') * current_rate).cast('long')) \
        .withColumn('Poland', (F.col('Poland') * current_rate).cast('long')) \
        .withColumn('Spain', (F.col('Spain') * current_rate).cast('long'))

    df_euro = df_euro.repartition(24)

    df_euro.write \
        .mode('overwrite') \
        .parquet('/app/datalake/silber/GDP_table.csv')

    url = "jdbc:postgresql://pgdatabase:5432/eu_gdp"
    properties = {
        "user": "root",
        "password": "root",
        "driver": "org.postgresql.Driver"
    }

    df_euro.write.jdbc(
        url=url, 
        table="eu_gdp", 
        mode="overwrite", 
        properties=properties
    )
if __name__ == "__main__":
    # https://www.kaggle.com/datasets/samuelcortinhas/gdp-of-european-countries/data
    download_to_datalake_bronze()

    convert_csv_to_parquet_silber_postgre_warehouse()


# docker run -it --network de-zoomcamp-final-project_default -v $(pwd)/datalake:/app/datalake pyspark:postgre 

