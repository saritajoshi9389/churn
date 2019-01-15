import json
import pandas as pd
from pyspark.sql import Row
from pytest import raises

import features.session as session
import features.activity as activity
import features.device as device

from tests import session_helper
from tests.utils import get_data_dict


def test_parse():
    test_dict = {
        'properties': {
            'userId': 1,
            'sessionId': 'a1',
            'device_code': 'android',
            'eventTimestamp': '2018-12-04T20:00:00.000Z'
        }
    }
    test_data = Row(value=json.dumps(test_dict), dt='2018-04-21')
    row = session.spark_job.parse(test_data)
    for col in ['user_id', 'session_id', 'device_code', 'event_timestamp']:
        assert col in row


def test_parse_missing_dt():
    test_dict = {'some': 'values'}
    test_data = Row(value=json.dumps(test_dict))
    with raises(AttributeError):
        session.spark_job.parse(test_data)
        assert 'dt' in AttributeError


def test_parse_different_schema():
    """Parse returns rows but with null values for keys not specified."""
    test_dict = {
        'properties': {
            'user_id': 1,
            'session_id': 'a1',
            'event_timestamp': '2018-12-04T20:00:00.000Z'
        }
    }
    test_data = Row(value=json.dumps(test_dict), dt='2018-04-21')
    row = session.spark_job.parse(test_data)
    for col in ['user_id', 'session_id', 'device_code', 'event_timestamp']:
        assert row[col] is None


def setup_sample(spark, args, records=None):
    """Build a Spark DataFrame out of records from seession_helper and write to parquet."""
    rows = [Row(value=json.dumps(record)) for record in records]
    data = spark.createDataFrame(rows)

    dt = args['dt']
    [yr, mo, day] = dt.split('-')
    sample_input_path = '{input_path}/yr={yr}/mo={mo}/dt={dt}/'.format(
        input_path=args['input_path'], yr=yr, mo=mo, dt=dt)
    data.write.parquet(sample_input_path, mode='overwrite')


def validate_session(spark, args):
    data = spark.read.load(args['output_path']).where("dt = '{}'".format(args['dt']))
    assert data.count() == 3
    assert 'a4' not in list(data['session_id'])


def validate_activity(spark, args):
    data = spark.read.load(args['output_path'])
    assert data.count() == 2
    data_dict = get_data_dict(data)
    assert data_dict[1]['active_days'] == 3
    assert data_dict[2]['active_days'] == 2
    assert data_dict[1]['num_sessions'] == 4
    assert data_dict[2]['num_sessions'] == 2
    assert data_dict[1]['avg_session_length'] == 90.0
    assert data_dict[2]['median_session_length'] == 60.0
    assert float(data_dict[1]['total_timespent']) == 360.0


def validate_device(spark, args):
    data = spark.read.load(args['output_path'])
    assert data.count() == 2
    data_dict = get_data_dict(data)
    assert float(data_dict[1]['android']) == 120.0
    assert float(data_dict[2]['appletv']) == 120.0
    assert pd.isnull(data_dict[2]['desktop'])


def test_job(spark):
    """Test daily session job AND dependent jobs."""
    input_path = 'tests/sample_input/session/'
    session_output_path = 'tests/sample_output/session/'

    # mock spark job on THREE days
    [dt_1, dt_2, dt_3] = ['2018-12-05', '2018-12-16', '2019-01-01']
    test_args = {
        'dt': dt_1,
        'input_path': input_path,
        'output_path': session_output_path
    }
    setup_sample(spark, test_args, records=session_helper.dt_1_records)
    session.spark_job.run(spark, test_args)

    test_args['dt'] = dt_2
    setup_sample(spark, test_args, records=session_helper.dt_2_records)
    session.spark_job.run(spark, test_args)

    test_args['dt'] = dt_3
    setup_sample(spark, test_args, records=session_helper.dt_3_records)
    session.spark_job.run(spark, test_args)

    validate_session(spark, test_args)

    # dependent jobs - from dt, looking back 28 days
    test_args = {
        'dt': '2019-01-01',
        'input_path': session_output_path,
        'output_path': 'tests/sample_output/activity/',
    }
    activity.spark_job.run(spark, test_args)
    validate_activity(spark, test_args)

    test_args['output_path'] = 'tests/sample_output/device/'
    device.spark_job.run(spark, test_args)
    validate_device(spark, test_args)
