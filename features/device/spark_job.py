from pyspark.sql.functions import sum
from features import utils
from . import query


def run(spark, args):
    query_dt = args['dt']
    dt_start = utils.dt_start(query_dt, days_back=28)
    # Job dependent on 28 days of successful session job runs
    sessions = spark.read.load(args['input_path']).where(
        "dt between '{dt_start}' and '{query_dt}'".format(
            query_dt=query_dt, dt_start=dt_start))

    sessions = spark.read.load(args['input_path'])
    sessions.createOrReplaceTempView('sessions')

    user_device_timespent = spark.sql(query.device_sql)
    user_device_timespent_pivoted = user_device_timespent.groupby('user_id') \
        .pivot('device_code').agg(sum('device_minutes'))
    user_device_timespent_pivoted.cache()

    user_device_timespent_pivoted.write.parquet(
        utils.dt_path(args['output_path'], query_dt), mode='overwrite')
