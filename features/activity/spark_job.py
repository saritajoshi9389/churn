from features import utils
from . import query


def run(spark, args):
    query_dt = args['dt']
    dt_start = utils.dt_start(query_dt, days_back=28)
    sessions = spark.read.load(args['input_path']).where(
        "dt between '{dt_start}' and '{query_dt}'".format(
            query_dt=query_dt, dt_start=dt_start))
    sessions.createOrReplaceTempView('sessions')

    user_activity = spark.sql(query.activity_sql)
    user_activity.write.parquet(
        utils.dt_path(args['output_path'], query_dt), mode='overwrite')
