"""Does not actually do prediction. It just ties predictions to users."""
from features.utils import dt_path
from pyspark.sql.functions import monotonically_increasing_id


def run(spark, args):

    dt = args['dt']
    users = spark.read.load(dt_path(args['input_subscription'], dt))
    predictions = spark.read.json(dt_path(args['input_prediction'], dt))

    # creating a row number is the only way to horizontally concatenate
    users = users.withColumn('row_num', monotonically_increasing_id())
    predictions = predictions.withColumn('row_num', monotonically_increasing_id())

    user_prediction = users.select(['user_id', 'row_num']).join(predictions, 'row_num')

    user_prediction = user_prediction.withColumnRenamed('predicted_label', 'predicted_churn')
    user_prediction = user_prediction.withColumnRenamed('score', 'churn_risk_score')

    user_prediction.write.parquet(
        dt_path(args['output_path'], dt),
        mode='overwrite')
