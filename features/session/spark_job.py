import json
from pyspark.sql import Row
from pyspark.sql.functions import sum

from features import utils
from . import query


def parse(row):
    value = json.loads(row.value)
    properties = value.get('properties', {})

    # explicitly parse and rename for optimization
    record = {
        'dt': row.dt,
        'user_id': properties.get('userId', None),
        'session_id': properties.get('sessionId', None),
        'device_code': properties.get('device_code', None),
        'event_timestamp': properties.get('eventTimestamp', None)
    }
    return Row(**record)


def run(spark, args):

    query_dt = args['dt']
    dt_start = utils.dt_start(query_dt, days_back=28)

    # off-the-bat 'where' for optimization
    segment = spark.read.load(args['input_path']).where(
        "dt between '{dt_start}' and '{query_dt}'".format(
            dt_start=dt_start, query_dt=query_dt))

    events = spark.createDataFrame(segment.rdd.map(parse))
    events.createOrReplaceTempView('events')
    # reusable sessions dataframe
    sessions = spark.sql(query.get_session_sql(query_dt))
    sessions.createOrReplaceTempView('sessions')

    user_activity = spark.sql(query.activity_sql)
    user_activity.write.parquet(args['output_path_activity'], mode='overwrite')

    user_device_timespent = spark.sql(query.device_sql)
    user_device_timespent_pivoted = user_device_timespent.groupby('user_id') \
        .pivot('device_code').agg(sum('device_minutes'))
    user_device_timespent_pivoted.cache()
    user_device_timespent_pivoted.write.parquet(args['output_path_device'], mode='overwrite')
