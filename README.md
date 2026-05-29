# urban-data-preparation-layer# Urban Data Preparation Layer

## Description
The Urban Data Preparation Layer is a distributed ETL (Extract, Transform, Load) pipeline built with PySpark and Apache Sedona. It is designed to process, unify, and store large-scale urban spatial data. 

The pipeline extracts AI-generated urban perception data (CSV) and OpenStreetMap street network features (GeoJSON) from an AWS S3 raw data lake. It reprojects the coordinates to a metric system, performs a highly optimized spatial distance join, and loads the unified dataset back into a bronze data lake layer as partitioned GeoParquet files.

## Architecture and Workflow

1. **Extract (src/read.py)**: 
   - Reads perception points from a CSV file.
   - Parses the street network GeoJSON using Spark's native multiline JSON parser and explodes the feature array to extract properties and geometry strings.
2. **Transform (src/prepare.py)**:
   - Converts raw coordinate strings into Apache Sedona spatial geometries.
   - Reprojects all geometries from WGS84 (EPSG:4326) to the Dutch local metric coordinate reference system (EPSG:28992).
   - Filters out invalid or null geometries to maintain spatial index integrity.
   - Executes a spatial distance join, snapping perception points to the nearest street segment within a 50-meter radius using an R-Tree index.
   - Caches the unified dataframe in memory to prevent lazy-evaluation recalculation.
3. **Load (src/upload.py)**:
   - Writes the unified spatial dataframe back to AWS S3.
   - Outputs in the geoparquet format for optimal downstream querying.
   - Partitions the data automatically by the highway classification.

## Prerequisites

To run this pipeline locally or on a cloud cluster, the following dependencies are required:
- Python 3.10 or higher.
- OpenJDK 17 (Required for PySpark 3.4+ compatibility).
- Valid AWS IAM credentials with read/write access to the target S3 buckets.

## Project Structure

```text
urban-data-preparation-layer/
├── config/
│   ├── __init__.py
│   └── logger_config.py          # Standardized logging configuration   
├── notebooks/
│   └── exploration.ipynb         # Jupyter notebooks for data exploration
├── src/
│   ├── __init__.py
│   ├── main.py                   # Main orchestrator
│   ├── prepare.py                # Spatial transformations and distance joins
│   ├── read.py                   # S3 extraction and GeoJSON parsing
│   ├── spark/
│   │   ├── __init__.py
│   │   └── spark_sedona.py       # SedonaContext and AWS Hadoop configuration
│   └── upload.py                 # GeoParquet writing and S3 partitioning
├── LICENSE                       # Project license
├── pyproject.toml                # dependency management
└── README.md                     # Project documentation