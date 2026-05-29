from config.logger_config import setup_logging
import logging

setup_logging()
logger = logging.getLogger(__name__)

def upload_to_bronze(unified_df, bronze_s3_path):
    logger.info(f"Uploading unified df to {bronze_s3_path}...")

    unified_df.write \
        .format("geoparquet") \
        .partitionBy("highway") \
        .mode("overwrite") \
        .save(bronze_s3_path)
    
    logger.info(f"Data successfully stored in the bronze layer: {bronze_s3_path}")