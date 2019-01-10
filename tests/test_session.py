import json
from pyspark.sql import Row
from features.session import spark_job
from tests import session_helper
from tests.utils import get_data_dict


def setup_sample(spark, records=None, dt='', input_path=''):
    """Build a Spark DataFrame out of records from seession_helper and write to parquet."""
    rows = [Row(value=json.dumps(record)) for record in records]
    data = spark.createDataFrame(rows)

    [yr, mo, day] = dt.split('-')
    sample_input_path = '{input_path}/yr={yr}/mo={mo}/dt={dt}/'.format(
        input_path=input_path, yr=yr, mo=mo, dt=dt)
    data.write.parquet(sample_input_path, mode='overwrite')


def validate_activity(spark, output_path):
    data = spark.read.load(output_path)
    assert data.count() == 2
    data_dict = get_data_dict(data)
    assert data_dict[1]['active_days'] == 3
    assert data_dict[2]['active_days'] == 2
    assert data_dict[1]['num_sessions'] == 4
    assert data_dict[2]['num_sessions'] == 2
    assert data_dict[1]['avg_session_length'] == 90.0
    assert data_dict[2]['median_session_length'] == 60.0
    assert float(data_dict[1]['total_timespent']) == 360.0


def validate_device(spark, output_path):
    data = spark.read.load(output_path)
    assert data.count() == 2
    data_dict = get_data_dict(data)
    assert float(data_dict[1]['android']) == 120.0
    assert float(data_dict[2]['apple_tv']) == 120.0
    assert data_dict[2]['desktop'] is None


def test_job(spark):
    input_path = 'tests/sample_input/session/'
    setup_sample(spark, records=session_helper.dt_1_records, dt='2018-12-05', input_path=input_path)
    setup_sample(spark, records=session_helper.dt_2_records, dt='2018-12-15', input_path=input_path)
    setup_sample(spark, records=session_helper.dt_3_records, dt='2018-12-31', input_path=input_path)

    test_args = {
        'dt': '2019-01-01',
        'input_path': input_path,
        'output_path_activity': 'tests/sample_output/activity/',
        'output_path_device': 'tests/sample_output/device/'
    }
    spark_job.run(spark, test_args)
    validate_activity(spark, test_args['output_path_activity'])
    validate_device(spark, test_args['output_path_device'])
