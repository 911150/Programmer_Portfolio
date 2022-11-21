
# Features retained from the original dataset
retained_features = {"VendorID", "tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance","RatecodeID","PULocationID","DOLocationID","payment_type","fare_amount","tip_amount","congestion_surcharge"}

# Features that are non continuous
integer_features = {"VendorID", "RatecodeID", "PULocationID", "DOLocationID", "payment_type"}

# Chronological features
time_features = {"tpep_pickup_datetime", "tpep_dropoff_datetime"}

# Regressable features / continuous
non_categorical_features = retained_features - integer_features
