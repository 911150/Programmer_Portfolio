from pyspark.sql import functions as F
from scripts._global_vars import *


def drop_cast_and_create_taxi(sdf):
    all_cols = set(sdf.columns)
    removed_cols = all_cols - retained_features
    sdf = sdf.drop(*removed_cols)

    # cast relevant attributes to int
    for field in integer_features:
        sdf = sdf.withColumn(
            field,
            F.col(field).cast('INT')
        )

    # Cast timestamp for time features to datetime type
    sdf = sdf.withColumn("PU_datetime",
                         F.to_date("tpep_pickup_datetime"))
    sdf = sdf.withColumn("DO_datetime",
                         F.to_date("tpep_dropoff_datetime"))

    # feature engineering : hour of day
    sdf = sdf.withColumn("PU_hourofday",
                         (F.hour("tpep_pickup_datetime")))  # -4 UTC

    sdf = sdf.withColumn("DO_hourofday",
                         (F.hour("tpep_dropoff_datetime")))

    # feature engineering : day of week
    sdf = sdf.withColumn("PU_dayofweek",
                         F.dayofweek("PU_datetime"))

    sdf = sdf.withColumn("DO_dayofweek",
                         F.dayofweek("DO_datetime"))

    # feature engineering : day of month
    sdf = sdf.withColumn("PU_dayofmonth",
                         F.dayofmonth("PU_datetime"))
    sdf = sdf.withColumn("DO_dayofmonth",
                         F.dayofmonth("DO_datetime"))

    sdf = sdf.withColumn("DO_dayofweek",
                         F.dayofmonth("DO_datetime"))

    # feature engineering : month of year
    sdf = sdf.withColumn("PU_month",
                         F.month("PU_datetime"))

    sdf = sdf.withColumn("DO_month",
                         F.month("DO_datetime"))

    # feature engineering : trip_time in minutes
    sdf = sdf.withColumn("trip_time_minutes",
                         (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast(
                             "long")) / 60)

    # feature engineering : speed mph
    sdf = sdf.withColumn("trip_speed_mph",
                         (F.col("trip_distance") / (F.col("trip_time_minutes") / 60)))

    # feature engineering : fare_amount / minute
    sdf = sdf.withColumn("fare_per_minute",
                         (F.col("fare_amount")) / (F.col("trip_time_minutes")))

    # for weather aggregation
    sdf = sdf.withColumn('hour_of_day_of_year',
                         F.date_trunc('hour', F.col('tpep_pickup_datetime')))

    return sdf


def weather_cast_drop(weather_sdf):
    # weather_sdf.columns
    weather_sdf = weather_sdf.select(['station', "valid", "tmpf", "dwpf", "relh"])

    # cast cols to appropriate types
    weather_sdf = weather_sdf.withColumn('tmpf', F.col('tmpf').cast('double'))
    weather_sdf = weather_sdf.withColumn('dwpf', F.col('dwpf').cast('double'))
    weather_sdf = weather_sdf.withColumn('relh', F.col('relh').cast('double'))

    # convert timestamp to datetime
    weather_sdf = weather_sdf.withColumn('datetime', F.to_date('valid'))

    # create hour of day of year indicator column
    weather_sdf = weather_sdf.withColumn('hour_of_day_of_year',
                                         F.date_trunc('hour', F.col('valid')))
    return weather_sdf


def weather_aggregate(weather_sdf):
    weather_sdf_agg = weather_sdf.groupBy(['hour_of_day_of_year']).mean()
    weather_sdf_agg = weather_sdf_agg \
        .withColumnRenamed("avg(tmpf)", "tmpf") \
        .withColumnRenamed("avg(dwpf)", "dwpf") \
        .withColumnRenamed("avg(relh)", "relh")
    return weather_sdf_agg


def weather_process(sdf):
    return weather_aggregate(weather_cast_drop(sdf))
