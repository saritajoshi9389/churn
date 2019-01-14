from features.join import spark_job
from pyspark.sql import Row


def setup_subscription(spark, args):
    records = [
        {
            'user_id': 1,
            'months_subscribing': 10,
            'has_disconnected': 1,
            'churned': 0
        },
        {
            'user_id': 2,
            'months_subscribing': 23,
            'has_disconnected': 1,
            'churned': 0
        }
    ]
    rows = [Row(**record) for record in records]
    data = spark.createDataFrame(rows)
    sample_input_path = '{input_path}/dt={dt}/'.format(
        input_path=args['input_subscription'], dt=args['dt'])
    data.write.parquet(sample_input_path, mode='overwrite')


def setup_performance(spark, args):
    records = [
        {
            'user_id': 1,
            'avg_startup_time': 30.0,
            'median_startup_time': 5.0,
            'total_startup_time': 160.0,
            'total_startup_errors': 2.0,
            'avg_interrupts': 3.0,
            'median_interrupts': 1.0,
            'total_interrupts': 6.0,
            'avg_buffering_time': 20.0,
            'median_buffering_time': 10.0,
            'total_buffering_time': 90.0,
            'avg_bitrate': 1320.0,
            'median_bitrate': 989.0
        }
    ]
    rows = [Row(**record) for record in records]
    data = spark.createDataFrame(rows)
    sample_input_path = '{input_path}/dt={dt}/'.format(
        input_path=args['input_performance'], dt=args['dt'])
    data.write.parquet(sample_input_path, mode='overwrite')


def setup_activity(spark, args):
    records = [
        {
            'user_id': 1,
            'active_days': 8,
            'num_sessions': 10,
            'avg_session_length': 124.7,
            'median_session_length': 59.0,
            'total_timespent': 420.0
        }
    ]
    rows = [Row(**record) for record in records]
    data = spark.createDataFrame(rows)
    sample_input_path = '{input_path}/dt={dt}/'.format(
        input_path=args['input_activity'], dt=args['dt'])
    data.write.parquet(sample_input_path, mode='overwrite')


def validate_job(spark, output_path):
    """Left join, no null values."""
    data = spark.read.load(output_path)
    assert data.count() == 2
    for col in data.columns:
        assert data.where(data[col].isNull()).count() == 0


def test_job(spark):
    test_args = {
        'dt': '2019-01-11',
        'input_subscription': 'tests/sample_input/subscription_to_join/',
        'input_performance': 'tests/sample_input/performance_to_join/',
        'input_activity': 'tests/sample_input/activity_to_join/',
        'output_path': 'tests/sample_output/joined/'
    }
    setup_subscription(spark, test_args)
    setup_performance(spark, test_args)
    setup_activity(spark, test_args)

    spark_job.run(spark, test_args)
    validate_job(spark, test_args['output_path'])
