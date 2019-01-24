from features.utils import dt_path
import features.joined as join


def run(spark, args):
    """Write features w/ churn status in first column"""
    users = join.spark_job.run(spark, args)
    features = list(users.columns)
    # churn must be the first column
    features.remove('churned')
    features = ['churned'] + features
    users.select(features).write.csv(
        dt_path(args['output_path'], args['dt']),
        compression='gzip',
        mode='overwrite')
