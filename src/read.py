import logging
from config.logger_config import setup_logging

from pyspark.sql.functions import explode, col, to_json 

setup_logging()
logger = logging.getLogger(__name__)

def read_raw_data(spark, raw_s3_path, csv_filename, geojson_filename):
    logger.info(f"Reading raw data from {raw_s3_path}...")
    
    csv_path = f"{raw_s3_path}{csv_filename}"
    geojson_path = f"{raw_s3_path}{geojson_filename}"
    
    df_perception = spark.read.option("header", "true").csv(csv_path)
    
    logger.info("Parsing GeoJSON and building spatial features...")
    raw_geojson = spark.read.option("multiline", "true").json(geojson_path)
     
    exploded_df = raw_geojson.select(explode("features").alias("feature"))
    
    extracted_df = exploded_df.select(
        col("feature.properties.*"),
        to_json(col("feature.geometry")).alias("geom_json_string")
    )
    
    extracted_df.createOrReplaceTempView("geojson_strings")
    
    df_streets = spark.sql("""
        SELECT *, ST_GeomFromGeoJSON(geom_json_string) AS geometry
        FROM geojson_strings
    """).drop("geom_json_string")
    
    return df_perception, df_streets