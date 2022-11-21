from pyspark.sql import functions as F
import pandas as pd
from collate import drop_cast_and_create_taxi



def remove_outliers(sdf):
    # Filter out bad records
    sdf = sdf.withColumn(
        'is_valid_record',
        F.when(
            ((F.col('trip_distance') > 0) & (F.col('trip_distance') < 312))
            & ((F.col('PULocationID') >= 1) & (F.col('PULocationID') <= 263))
            & ((F.col('DOLocationID') >= 1) & (F.col('DOLocationID') <= 263))
            & ((F.col('fare_amount') > 0) & (F.col('fare_amount') < 500))
            & ((F.col('congestion_surcharge') >= 0) & (F.col('congestion_surcharge') <= 10))
            & ((F.col('passenger_count') > 0) & (F.col('passenger_count') <= 6))
            & (F.col('tip_amount') >= 0)
            & ((F.col('VendorID') <=2) & (F.col('VendorID') >=1))
            & ((F.col('payment_type') <= 2) & (F.col('payment_type') >= 1))
            & ((F.col('RatecodeID') >= 1) & (F.col('RatecodeID') <= 6))
            & ((F.col('trip_speed_mph') >= 1) & (F.col('trip_speed_mph') <= 65))
            & ((F.col('trip_time_minutes') >= 2) & (F.col('trip_time_minutes') <= 300))
            & ((F.col('RatecodeID') == 1) & (F.col('fare_amount') >= 2.50)),
            True
        ).otherwise(False)
    )
    #sdf = sdf.filter(sdf.is_valid_record == True)

    return sdf


def get_outliers_df(sdf):

    total_records = sdf.count()

    trip_dist = sdf.filter((F.col('trip_distance') > 0) & (F.col('trip_distance') < 312)).count()
    pu_locid = sdf.filter(((F.col('PULocationID') >= 1) & (F.col('PULocationID') <= 263))).count()
    du_locid = sdf.filter(((F.col('DOLocationID') >= 1) & (F.col('DOLocationID') <= 263))).count()
    fare_amount = sdf.filter(((F.col('fare_amount') > 0) & (F.col('fare_amount') < 500))).count()
    congestion = sdf.filter(((F.col('congestion_surcharge') >= 0) & (F.col('congestion_surcharge') <= 10))).count()
    pass_count = sdf.filter(((F.col('passenger_count') > 0) & (F.col('passenger_count') <= 6))).count()
    tip_amount = sdf.filter((F.col('tip_amount') >= 0)).count()
    ven_id = sdf.filter(((F.col('VendorID') <=2) & (F.col('VendorID') >=1))).count()
    pay_type = sdf.filter(((F.col('payment_type') <= 2) & (F.col('payment_type') >= 1))).count()
    rcode_id = sdf.filter(((F.col('RatecodeID') >= 1) & (F.col('RatecodeID') <= 6))).count()
    t_speed = sdf.filter(((F.col('trip_speed_mph') >= 1) & (F.col('trip_speed_mph') <= 65))).count()
    t_time = sdf.filter(((F.col('trip_time_minutes') >= 2) & (F.col('trip_time_minutes') <= 300))).count()
    r_code_id_min_fee = sdf.filter(((F.col('RatecodeID') == 1) & (F.col('fare_amount') >= 2.50))).count()

    # yuck
    outliers_cols = ['trip_distance', 'PULocationID', 'DOLocation', 'fare_amount', 'congestion', 'passenger_count',
                     'tip_amount', 'vendorid', 'pay_type', 'rcode_id', 't_speed', 't_time', 'ratecode_min_fee']
    outlier_vals = [trip_dist, pu_locid, du_locid, fare_amount, congestion, pass_count,
                    tip_amount, ven_id, pay_type, rcode_id, t_speed, t_time, r_code_id_min_fee,]

    outliers_inv = [total_records- x for x in outlier_vals]
    outliers_prop = [x / total_records for x in outliers_inv]

    outlier_df = pd.DataFrame(outliers_inv, index=outliers_cols)
    outlier_df['prop_of_total'] = outliers_prop

    return outlier_df


def run_clean(sdf):
    sdf = drop_cast_and_create_taxi(sdf)
    sdf = remove_outliers(sdf)
    sdf = sdf.filter(sdf.is_valid_record == True)
    # TODO drop isvalidrecord col
    return sdf