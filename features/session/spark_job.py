import json
from pyspark.sql import Row

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
    dt_start = utils.dt_start(query_dt, days_back=1)

    # off-the-bat 'where' for optimization
    segment = spark.read.load(args['input_path']).where(
        "dt in ('{dt_start}', '{query_dt}')".format(
            dt_start=dt_start, query_dt=query_dt))

    events = spark.createDataFrame(segment.rdd.map(parse), samplingRatio=.4)
    events.createOrReplaceTempView('events')
    sessions = spark.sql(query.get_session_sql(query_dt))
    # Other jobs will use daily session roll-up
    sessions.write.parquet(utils.dt_path(args['output_path'], query_dt), mode='overwrite')
