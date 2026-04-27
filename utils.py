"""
Shared utilities for the Cricket Reddit Analysis project.

Includes:
- Spark session builder
- S3 data loaders with partition filters
- Cricket-specific constants (subreddits, players, event dates)
- Time feature helpers
- Save utilities
"""

import os
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lower, regexp_extract, when, hour, dayofweek, month, year, from_unixtime, to_timestamp


# =============================================================================
# CONSTANTS
# =============================================================================

S3_BUCKET = "s3a://dats6450-group9-reddit-data/reddit/parquet"
S3_SUBMISSIONS = f"{S3_BUCKET}/submissions"
S3_COMMENTS = f"{S3_BUCKET}/comments"

# Filtered cricket dataset (created in 01_cricket_filtering.ipynb)
S3_CRICKET_SUBMISSIONS = f"{S3_BUCKET}/cricket_filtered/submissions"
S3_CRICKET_COMMENTS = f"{S3_BUCKET}/cricket_filtered/comments"

# Cricket subreddits (cast wide net, will trim based on row counts in 01)
CRICKET_SUBREDDITS = [
    # General cricket
    "Cricket",
    "CricketShitpost",
    # Indian cricket
    "IndianCricket",
    "IndianCricketTeam",
    "bcci",
    "cricketindia",
    "indiacricket",
    # IPL general
    "ipl",
    # IPL teams (full names + short forms)
    "ChennaiSuperKings", "csk",
    "RoyalChallengers", "RCB",
    "MumbaiIndians", "mi",
    "KolkataKnightRiders", "KKR",
    "SunrisersHyderabad", "srh",
    "DelhiCapitals", "delhicapitals", "dc",
    "PunjabKingsIPL", "pbks",
    "RajasthanRoyals", "rr",
    "LucknowSuperGiants", "lsg",
    "GujaratTitans", "gt",
    # Players
    "Dhoni", "MSDhoni",
    "Kohli", "ViratKohli", "Virat",
    "RohitSharma", "Hitman",
    # Other countries (rivalry context)
    "PakCricket",
    "AusCricket", "cricketaustralia",
    "EngCricket", "englandcricket",
    "NZCricket",
    # Format / events
    "TestCricket",
    "Ashes",
    "ICC",
    "t20worldcup",
]

# Player mention regex patterns (case-insensitive)
PLAYER_PATTERNS = {
    "Dhoni": r"\b(dhoni|msd|mahi|thala|captain cool)\b",
    "Kohli": r"\b(kohli|virat|king kohli|chase master)\b",
    "Rohit": r"\b(rohit|sharma|hitman|ro|rohit sharma)\b",
}

# Key event dates for narrative analysis
EVENT_DATES = {
    "wc2023_start":      datetime(2023, 10,  5),  # ICC ODI World Cup 2023 begins
    "wc2023_final":      datetime(2023, 11, 19),  # India vs Australia final (loss)
    "wc2023_final_end":  datetime(2023, 11, 26),  # 1 week after for aftermath
    "ipl2024_start":     datetime(2024,  3, 22),  # IPL 2024 begins
    "ipl2024_end":       datetime(2024,  5, 26),  # IPL 2024 final
    "t20wc2024_start":   datetime(2024,  6,  1),  # T20 World Cup 2024 begins
    "t20wc2024_final":   datetime(2024,  6, 29),  # India vs SA final (win)
    "t20wc2024_end":     datetime(2024,  7,  6),  # 1 week after for aftermath
}

# Subreddits associated with each player (for team-fan lens)
PLAYER_TEAM_SUBS = {
    "Dhoni":  ["ChennaiSuperKings"],
    "Kohli":  ["RoyalChallengers", "RCB"],
    "Rohit":  ["MumbaiIndians"],
}


# =============================================================================
# SPARK SESSION
# =============================================================================

def get_spark(app_name="CricketRedditAnalysis"):
    """Build and return a Spark session configured for S3 + local mode."""
    spark = (
        SparkSession.builder
        .appName(app_name)
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
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =============================================================================
# S3 DATA LOADERS
# =============================================================================

def load_submissions(spark, years=None, months=None):
    """
    Load Reddit submissions from S3.

    Args:
        spark: SparkSession
        years: list of integer years to filter (e.g., [2023, 2024]) or None for all
        months: list of integer months to filter (e.g., [10, 11]) or None for all
    """
    df = spark.read.parquet(S3_SUBMISSIONS)
    if years is not None:
        df = df.filter(col("yyyy").isin(years))
    if months is not None:
        df = df.filter(col("mm").isin(months))
    return df


def load_comments(spark, years=None, months=None):
    """Load Reddit comments from S3 with optional partition filters."""
    df = spark.read.parquet(S3_COMMENTS)
    if years is not None:
        df = df.filter(col("yyyy").isin(years))
    if months is not None:
        df = df.filter(col("mm").isin(months))
    return df


def load_cricket_submissions(spark):
    """Load the pre-filtered cricket submissions (from 01_cricket_filtering.ipynb)."""
    return spark.read.parquet(S3_CRICKET_SUBMISSIONS)


def load_cricket_comments(spark):
    """Load the pre-filtered cricket comments (from 01_cricket_filtering.ipynb)."""
    return spark.read.parquet(S3_CRICKET_COMMENTS)


# =============================================================================
# FILTERING HELPERS
# =============================================================================

def filter_cricket_subreddits(df, subreddit_col="subreddit"):
    """Filter a DataFrame to only rows in our cricket subreddits list."""
    lower_subs = [s.lower() for s in CRICKET_SUBREDDITS]
    return df.filter(lower(col(subreddit_col)).isin(lower_subs))


def add_player_mentions(df, text_col):
    """
    Add boolean columns for each player mention in the given text column.
    Adds: mentions_dhoni, mentions_kohli, mentions_rohit, mentions_any_king
    """
    df = df.withColumn(
        "mentions_dhoni",
        col(text_col).rlike("(?i)" + PLAYER_PATTERNS["Dhoni"])
    )
    df = df.withColumn(
        "mentions_kohli",
        col(text_col).rlike("(?i)" + PLAYER_PATTERNS["Kohli"])
    )
    df = df.withColumn(
        "mentions_rohit",
        col(text_col).rlike("(?i)" + PLAYER_PATTERNS["Rohit"])
    )
    df = df.withColumn(
        "mentions_any_king",
        col("mentions_dhoni") | col("mentions_kohli") | col("mentions_rohit")
    )
    return df


# =============================================================================
# TIME FEATURE HELPERS
# =============================================================================

def add_time_features(df, ts_col="created_utc"):
    """
    Add time-based feature columns from a Unix timestamp column.
    Adds: created_dt, hour_of_day, day_of_week, month_num, year_num
    """
    df = df.withColumn("created_dt", to_timestamp(from_unixtime(col(ts_col))))
    df = df.withColumn("hour_of_day", hour(col("created_dt")))
    df = df.withColumn("day_of_week", dayofweek(col("created_dt")))
    df = df.withColumn("month_num", month(col("created_dt")))
    df = df.withColumn("year_num", year(col("created_dt")))
    return df


def label_event_period(df, ts_col="created_dt"):
    """
    Add an 'event_period' column labeling each row by which key cricket event
    it falls into (or 'other' for non-event periods).
    """
    return df.withColumn(
        "event_period",
        when((col(ts_col) >= EVENT_DATES["wc2023_start"]) &
             (col(ts_col) <  EVENT_DATES["wc2023_final"]), "wc2023_group_stage")
        .when((col(ts_col) >= EVENT_DATES["wc2023_final"]) &
              (col(ts_col) <= EVENT_DATES["wc2023_final_end"]), "wc2023_final_aftermath")
        .when((col(ts_col) >= EVENT_DATES["ipl2024_start"]) &
              (col(ts_col) <= EVENT_DATES["ipl2024_end"]), "ipl2024")
        .when((col(ts_col) >= EVENT_DATES["t20wc2024_start"]) &
              (col(ts_col) <  EVENT_DATES["t20wc2024_final"]), "t20wc2024_group_stage")
        .when((col(ts_col) >= EVENT_DATES["t20wc2024_final"]) &
              (col(ts_col) <= EVENT_DATES["t20wc2024_end"]), "t20wc2024_final_aftermath")
        .otherwise("other")
    )


# =============================================================================
# SAVE UTILITIES
# =============================================================================

def save_to_s3(df, s3_path, mode="overwrite", partition_by=None):
    """Save a DataFrame to S3 as parquet."""
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(s3_path)
    print(f"Saved to {s3_path}")


def save_pandas_to_local(pdf, filename, results_dir="results/tables"):
    """Save a pandas DataFrame as CSV to the local results folder."""
    os.makedirs(results_dir, exist_ok=True)
    out_path = os.path.join(results_dir, filename)
    pdf.to_csv(out_path, index=False)
    print(f"Saved to {out_path}")


def save_figure(fig, filename, results_dir="results/figures"):
    """Save a matplotlib figure as PNG to the local results folder."""
    os.makedirs(results_dir, exist_ok=True)
    out_path = os.path.join(results_dir, filename)
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved to {out_path}")
