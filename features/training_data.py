import os
from pyspark.sql.functions import expr

args = {
    'bucket': 's3://datascience-feature-eng-test-201901',
    'subscription': 'user-subs-201901',
    'performance': 'user-perf-201901',
    'activity': 'user-activity-201901',
    'device': 'user-device-201901',
    'output': 'users-201901'
}

dt = '2018-10-15'

user_subscription = spark.read.load(os.path.join(args['bucket'], args['subscription'], 'dt={}'.format(dt)))
user_performance = spark.read.load(os.path.join(args['bucket'], args['performance'], 'dt={}'.format(dt)))
user_activity = spark.read.load(os.path.join(args['bucket'], args['activity'], 'dt={}'.format(dt)))

users = user_subscription.join(
    user_performance, 'user_id', 'left_outer').join(
    user_activity, 'user_id', 'left_outer')

performance_features = user_performance.columns
performance_features.remove('user_id')
activity_features = ['avg_session_length', 'median_session_length']

for feature in performance_features + activity_features:
    median_expr = 'percentile_approx({}, 0.5)'.format(feature)
    median_rows = users.agg(expr(median_expr)).collect()
    median = median_rows[0][median_expr]
    users = users.fillna({feature: median})

for feature in ['total_timespent', 'num_sessions', 'active_days']:
    users = users.fillna({feature: 0})

users.write.parquet(os.path.join(args['bucket'], args['output']))

"""
EVERY ONE OF THESE SHOULD BE ZERO
for col in users.columns:
    print users.where(users[col].isNull()).count()
"""
