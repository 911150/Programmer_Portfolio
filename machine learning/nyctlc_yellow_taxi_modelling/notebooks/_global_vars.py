from pyspark.sql import SparkSession

# Features retained from the original dataset
retained_features = {"VendorID",
                     "tpep_pickup_datetime",
                     "tpep_dropoff_datetime",
                     "passenger_count",
                     "trip_distance",
                     "RatecodeID",
                     "PULocationID",
                     "DOLocationID",
                     "payment_type",
                     "fare_amount",
                     "tip_amount",
                     "congestion_surcharge"}

# Features that are non continuous
integer_features = {"VendorID",
                    "RatecodeID",
                    "PULocationID",
                    "DOLocationID",
                    "payment_type"}

# Chronological features
time_features = {"tpep_pickup_datetime",
                 "tpep_dropoff_datetime"}

# Numerical features / continuous
non_categorical_features = retained_features - integer_features

# Cleaned df export folder
export_relative_dir = '../data/curated/'

# spark session
spark = (
    SparkSession.builder.appName("Dataset collation")
    .config("spark.sql.repl.eagerEval.enabled", True)
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .getOrCreate()
)