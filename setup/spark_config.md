# Spark Configuration

This document describes the Spark setup for this project.

## Deployment Mode

We run Spark in local mode (`local[*]`) on a single EC2 instance.

The 478 GB Reddit dataset is read from S3 in partitioned chunks.

## Software Versions

| Component | Version |
|-----------|---------|
| Apache Spark | 3.5.1 |
| PySpark | 3.5.1 |
| Java | 11 (OpenJDK) |
| Hadoop AWS | 3.3.4 |
| AWS Java SDK Bundle | 1.12.262 |
| Python | 3.10+ |

## EC2 Instance

- Type: t3.large (2 vCPUs, 8 GB RAM)
- OS: Ubuntu 22.04 LTS
- Storage: 30 GB EBS (gp3)
- Region: us-east-1

## Spark Session Configuration

The Spark session is built via `get_spark()` in `utils.py` with the following configuration:

```python
spark = (
    SparkSession.builder
    .appName("CricketRedditAnalysis")
    .master("local[*]")
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.driver.memory", "6g")
    .config("spark.driver.maxResultSize", "2g")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
```

## S3 Data Source

- Bucket: `s3://dats6450-group9-reddit-data/reddit/parquet/`
- Format: Parquet
- Partitioning: Hive-style on `yyyy` (integer) and `mm` (integer)
- Subdirectories:
  - `submissions/yyyy=YYYY/mm=MM/` — Reddit posts
  - `comments/yyyy=YYYY/mm=MM/` — Reddit comments

## Partition Filtering

Partition columns are integers, so filters use integer literals:

```python
df.filter((col("yyyy") == 2024) & (col("mm") == 6))
```

## Running Spark

Start Jupyter:

```bash
source ~/spark-env/bin/activate
cd ~/cricket_project
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser
```

Access at `http://<EC2-public-IP>:8888`.

Spark UI is available at `http://<EC2-public-IP>:4040` when a session is active.
