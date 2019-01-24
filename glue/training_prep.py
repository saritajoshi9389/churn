import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from features.training_prep import spark_job
from features.utils import yesterday_dt

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'dt',
    'input_subscription',
    'input_performance',
    'input_activity',
    'output_path'])
if args['dt'] == 'yesterday':
    args['dt'] = yesterday_dt()

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark_job.run(spark, args)

job.commit()
