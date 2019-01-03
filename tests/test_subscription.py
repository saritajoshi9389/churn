from datetime import datetime
import pandas as pd
from pyspark.sql import Row

from features.subscription import spark_job
from tests import sub_helper


def test_next_month_1():
    next_mo = spark_job.get_next_month('2018-12-31')
    assert next_mo.month == 1


def test_next_month_2():
    next_mo = spark_job.get_next_month('2019-01-31')
    assert next_mo.day == 28


def test_input_paths():
    s3 = 's3://mybucket/'
    dates = [datetime(2018, 11, 29), datetime(2018, 12, 29), datetime(2019, 01, 04)]
    input_paths = spark_job.get_input_paths(s3, dates)
    assert 's3://mybucket/yr=2019/mo=01' in input_paths
    assert 's3://mybucket/yr=2018/mo=12' in input_paths
    assert 's3://mybucket/yr=2018/mo=11' in input_paths


def setup_sample(spark, records=None, dt='', input_path=''):
    """Build a Spark DataFrame out of records from sub_helper and write to parquet."""
    rows = [Row(**record) for record in records]
    data = spark.createDataFrame(rows)

    timestamp_cols = [
        'current_subscription_start_date', 'current_subscription_end_date',
        'first_purchase_date', 'last_conversion_date',
        'last_reconnect_date', 'last_disconnect_date']

    for col in timestamp_cols:
        data = data.withColumn(col, data[col].cast('timestamp'))

    [yr, mo, day] = dt.split('-')
    sample_input_path = '{input_path}/yr={yr}/mo={mo}/dt={dt}/'.format(
        input_path=input_path, yr=yr, mo=mo, dt=dt)
    data.write.parquet(sample_input_path, mode='overwrite')


def validate_job(spark, output_path):
    data = spark.read.load(output_path)
    datatypes = dict(data.dtypes)
    assert data.count() == 3
    for col in ['has_disconnected',
                'current_days_active',
                'previous_days_active',
                'total_days_active']:
        assert datatypes[col] == 'int'
    df = pd.DataFrame(data.collect(), columns=data.columns)
    df = df.set_index('user_id')
    data_dict = df.to_dict(orient='index')
    assert data_dict[1]['churned'] == 0
    assert data_dict[2]['churned'] == 1
    assert data_dict[5]['churned'] == 0
    assert 3 not in data_dict
    assert 4 not in data_dict
    assert data_dict[1]['total_days_active'] == data_dict[1]['current_days_active']
    assert data_dict[1]['total_days_active'] == 106
    assert data_dict[2]['current_days_active'] == 459
    assert data_dict[5]['total_days_active'] == 198
    assert data_dict[5]['has_disconnected'] == 1
    assert data_dict[5]['previous_days_active'] == 92


def test_job(spark):
    input_path = 'tests/sample_input/subscription/'
    setup_sample(spark, records=sub_helper.query_dt_records, dt='2018-10-15', input_path=input_path)
    setup_sample(spark, records=sub_helper.later_dt_records, dt='2018-11-18', input_path=input_path)

    test_args = {
        'dt': '2018-10-15',
        'input_path': input_path,
        'output_path': 'tests/sample_output/subscription/'
    }
    spark_job.run(spark, test_args)
    validate_job(spark, test_args['output_path'])
