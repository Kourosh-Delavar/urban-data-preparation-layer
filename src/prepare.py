from config.logger_config import setup_logging
import logging

setup_logging()
logger = logging.getLogger(__name__)

def prepare_unify(spark, df_perception, df_streets):
    logger.info("Starting data preparation and verification...")
    
    csv_count = df_perception.count()
    geo_count = df_streets.count()
    logger.info(f"Raw CSV rows: {csv_count}, Raw GeoJSON features: {geo_count}")
    
    if csv_count == 0 or geo_count == 0:
        logger.error("ERROR: One of the raw datasets is completely empty. Aborting join.")
        return df_perception.limit(0)

    df_perception.createOrReplaceTempView("perception_raw")
    df_streets.createOrReplaceTempView("streets_raw")

    points_metric = spark.sql("""
        SELECT *, 
        ST_Transform(ST_Point(CAST(lon AS DOUBLE), CAST(lat AS DOUBLE)), 'EPSG:4326', 'EPSG:28992') as geom_metric 
        FROM perception_raw             
    """).filter("geom_metric IS NOT NULL")
    points_metric.createOrReplaceTempView("points_metric")

    streets_metric = spark.sql("""
        SELECT *, ST_Transform(geometry, 'EPSG:4326', 'EPSG:28992') as geom_metric 
        FROM streets_raw         
    """).filter("geom_metric IS NOT NULL")
    streets_metric.createOrReplaceTempView("streets_metric")

    logger.info("Executing Spatial Distance Join (Threshold: 50 meters)...")
    
    unified_df = spark.sql("""
        SELECT p.uuid, p.path, p.Beautiful, p.Boring, p.Depressing, 
               p.Lively, p.Safe, p.Wealthy, p.lon, p.lat, p.ca, p.image_caption, 
               p.geom_metric AS geometry,
               s.highway, 
               s.night_safety, 
               s.dag_safety, 
               s.wheelchair_score, 
               s.experience_score
        FROM points_metric p, streets_metric s
        WHERE ST_Distance(p.geom_metric, s.geom_metric) <= 50
    """)

    unified_df = unified_df.fillna({"highway": "unknown"})

    final_count = unified_df.count()
    logger.info(f"Spatial join complete. Rows to upload: {final_count}")

    return unified_df