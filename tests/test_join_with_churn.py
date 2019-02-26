import pandas as pd
import tests.test_join as join_test
from features.join_with_churn import spark_job


def validate_job(spark, output_path):
    # the data must be a csv
    data = spark.read.csv(output_path)
    # left join
    assert data.count() == 2
    # no null values
    for col in data.columns:
        assert data.where(data[col].isNull()).count() == 0
    columns = list(data.columns)
    # no headers
    assert columns[0] == '_c0'
    df = pd.DataFrame(data.collect())
    # the first column MUST be churn 1/0 for training data
    for churn_value in list(df[0]):
        assert int(churn_value) in (0, 1)


def test_job(spark):
    test_args = {
        'dt': '2019-01-11',
        'input_subscription': 'tests/sample_input/subscription_to_join/',
        'input_performance': 'tests/sample_input/performance_to_join/',
        'input_activity': 'tests/sample_input/activity_to_join/',
        'output_path': 'tests/sample_output/training/'
    }
    join_test.setup_subscription(spark, test_args)
    join_test.setup_performance(spark, test_args)
    join_test.setup_activity(spark, test_args)

    spark_job.run(spark, test_args)
    validate_job(spark, test_args['output_path'])
