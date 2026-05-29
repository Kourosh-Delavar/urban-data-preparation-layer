import traceback
from config.logger_config import setup_logging
import logging

import os
from dotenv import load_dotenv

from spark.spark_sedona import init_spark
from read import read_raw_data
from prepare import prepare_unify
from upload import upload_to_bronze

setup_logging()
logger = logging.getLogger(__name__)

def main():

    load_dotenv(dotenv_path=".env.aws")

    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    if access_key:
        logger.info(f"AWS credentials loaded (key ends in ...{access_key[-4:]})")
    else:
        logger.error("Error: No AWS credentials found in .env.aws")

    RAW_S3_PATH = os.getenv('RAW_S3_PATH')
    BRONZE_S3_PATH = os.getenv('BRONZE_S3_PATH')

    CSV_FILE = os.getenv('CSV_FILE')
    GEOJSON_FILE = os.getenv('GEOJSON_FILE')

    spark = init_spark()

    try:
        df_csv, df_geo = read_raw_data(spark, RAW_S3_PATH, CSV_FILE, GEOJSON_FILE)
        unified_data = prepare_unify(spark, df_csv, df_geo)
        upload_to_bronze(unified_data, BRONZE_S3_PATH)
    
    except Exception as e:
        logger.error(f"An error occured during processing: {e}")
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()