from tests.test_join import setup_subscription
from features.prediction import spark_job
from features.utils import dt_path
from pyspark.sql import Row


def setup_predictions(spark, args):
    records = [
        {
            'predicted_label': 1.0,
            'score': .55
        },
        {
            'predicted_label': 0.0,
            'score': .05
        }
    ]
    rows = [Row(**record) for record in records]
    data = spark.createDataFrame(rows)
    sample_input_path = dt_path(args['input_prediction'], args['dt'])
    data.write.json(sample_input_path, compression='gzip', mode='overwrite')


def validate_job(spark, output_path):
    assert True
    # must be a parquet
    data = spark.read.load(output_path)
    assert data.count() == 2

    columns = list(data.columns)
    for col in ['user_id', 'predicted_churn', 'churn_risk_score']:
        assert col in columns


def test_job(spark):
    test_args = {
        'dt': '2019-01-11',
        'input_subscription': 'tests/sample_input/subscription_to_concat/',
        'input_prediction': 'tests/sample_input/prediction_to_concat/',
        'output_path': 'tests/sample_output/user_prediction/'
    }
    setup_subscription(spark, test_args)
    setup_predictions(spark, test_args)

    spark_job.run(spark, test_args)
    validate_job(spark, test_args['output_path'])
