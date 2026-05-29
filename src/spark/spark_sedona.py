from config.logger_config import setup_logging
import logging

from sedona.spark import SedonaContext
import os

setup_logging()
logger = logging.getLogger(__name__)

def init_spark():

    logger.info("Initializing Spark Session with Sedona and AWS configurations...")
    
    aws_region = os.getenv("AWS_REGION", "eu-central-1")

    builder = SedonaContext.builder() \
        .appName("UrbanDataPreparationLayer") \
        .config("spark.jars.packages", 
                "org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.5.1,"
                "org.datasyslab:geotools-wrapper:1.5.1-28.2,"            
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.jars.repositories", 
                "https://artifacts.unidata.ucar.edu/repository/unidata-all/,"
                "https://repo.osgeo.org/repository/release/") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
        
    spark = builder.getOrCreate()
    spark = SedonaContext.create(spark)
    
    return spark