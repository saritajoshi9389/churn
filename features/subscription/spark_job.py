from datetime import datetime
from dateutil.relativedelta import relativedelta

from query import get_sub_sql


def get_next_month(dt):
    date = datetime.strptime(dt, '%Y-%m-%d')
    return date + relativedelta(months=1)


def run(spark, args):
    query_dt = args['dt']

    next_month_date = get_next_month(query_dt)
    all_subs = spark.read.load(args['input_path'])
    all_subs.createOrReplaceTempView("sub_table")

    sub_sql = get_sub_sql(
        query_dt=query_dt,
        month_later_dt=next_month_date.strftime('%Y-%m-%d'))
    subs = spark.sql(sub_sql)
    subs.write.parquet(args['output_path'], mode='overwrite')
