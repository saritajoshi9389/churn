import pandas as pd
from features.performance import spark_job


def validate_job(spark, output_path):
    data = spark.read.load(output_path)
    # validate datatypes
    datatypes = dict(data.dtypes)
    int_columns = ['median_startup_time', 'total_startup_time',
                   'total_startup_errors', 'median_bitrate',
                   'median_interrupts', 'total_interrupts',
                   'median_buffering_time', 'total_buffering_time']
    for col in int_columns:
        assert datatypes[col] == 'int' or datatypes[col] == 'bigint'

    double_columns = ['avg_startup_time', 'avg_interrupts',
                      'avg_buffering_time', 'avg_bitrate']
    for col in double_columns:
        assert datatypes[col] == 'double'

    # validate aggregations
    df = pd.DataFrame(data.collect(), columns=data.columns)
    df = df.set_index('user_id')
    data_dict = df.to_dict(orient='index')

    assert data_dict['GORWP105662059']['active_days'] == 4524.0
    assert data_dict['GORWP105662059']['num_essions'] == 4524
    assert data_dict['GORWP16177187']['avg_session_length'] == 4439
    assert data_dict['GORWP2006481']['median_session_length'] == 0
    assert data_dict['GORWP2006481']['total_timespent'] == 0.0


def test_job(spark):
    test_args = {
        'dt': '2019-01-03',
        'input_path': 'tests/sample_input/performance/',
        'output_path': 'tests/sample_output/performance/'
    }
    spark_job.run(spark, test_args)
    validate_job(spark, test_args['output_path'])

