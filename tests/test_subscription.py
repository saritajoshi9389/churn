from datetime import datetime
import pandas as pd
from pyspark.sql import Row

from features.subscription import spark_job
from tests import sub_helper


def setup_sample(spark, records=None, dt='', input_path=''):
    """Build a Spark DataFrame out of records from sub_helper and write to parquet."""
    rows = [Row(**record) for record in records]
    data = spark.createDataFrame(rows)

    timestamp_cols = [
        'subscription_start_date',
        'subscription_expire_date',
        'first_purchase_date']
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
                'months_subscribing']:
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
