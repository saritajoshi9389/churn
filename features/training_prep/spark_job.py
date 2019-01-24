from features.utils import dt_path
from pyspark.sql.functions import expr


def run(spark, args):

    dt = args['dt']
    user_subscription = spark.read.load(dt_path(args['input_subscription'], dt))
    user_performance = spark.read.load(dt_path(args['input_performance'], dt))
    user_activity = spark.read.load(dt_path(args['input_activity'], dt))

    users = user_subscription.join(
        user_performance, 'user_id', 'left_outer').join(
        user_activity, 'user_id', 'left_outer')

    performance_features = user_performance.columns
    performance_features.remove('user_id')
    session_features = ['avg_session_length', 'median_session_length']

    for feature in performance_features + session_features:
        median_expr = 'percentile_approx({}, 0.5)'.format(feature)
        median_rows = users.agg(expr(median_expr)).collect()
        median = median_rows[0][median_expr]
        users = users.fillna({feature: median})

    for feature in ['total_timespent', 'num_sessions', 'active_days']:
        users = users.fillna({feature: 0})

    features = list(users.columns)
    features.remove('user_id')
    features.remove('churned')
    features = ['churned'] + features

    users.select(features).write.csv(
        dt_path(args['output_path'], dt),
        compression='gzip',
        mode='overwrite')
