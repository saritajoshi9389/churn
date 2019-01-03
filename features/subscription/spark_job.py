import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from query import get_sub_sql


def get_next_month(dt):
    date = datetime.strptime(dt, '%Y-%m-%d')
    return date + relativedelta(months=1)


def get_input_paths(s3_path, date_list):
    """Get full S3 paths from prefix and list of dates."""
    return list(set([
        os.path.join(s3_path, 'yr={}'.format(date.year), 'mo={:02d}'.format(date.month))
        for date in date_list]))


def run(spark, args):
    query_dt = args['dt']

    query_date = datetime.strptime(query_dt, '%Y-%m-%d')
    next_month_date = get_next_month(query_dt)
    # churn status might appear up to 5 days late
    churn_check_date = (next_month_date + timedelta(days=5))
    input_paths = get_input_paths(args['input_path'], [query_date, next_month_date, churn_check_date])

    # get spark DF from up to 3 input paths
    all_subs = spark.read.load(input_paths[0])
    for num in range(1, len(input_paths)):
        subs_later = spark.read.load(input_paths[num])
        columns = list(set(all_subs.columns).intersection(subs_later.columns))
        all_subs = all_subs.select(columns).union(subs_later.select(columns))

    all_subs.createOrReplaceTempView("sub_table")

    sub_sql = get_sub_sql(
        query_dt=query_dt,
        max_expiration_dt=next_month_date.strftime('%Y-%m-%d'),
        after_expiration_dt=churn_check_date.strftime('%Y-%m-%d'))
    subs = spark.sql(sub_sql)

    subs.write.parquet(args['output_path'], mode='overwrite')
