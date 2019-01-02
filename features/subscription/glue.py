import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from features.subscription import spark_job

# INPUT_PATH = 's3://hbo-dap-dcm/hbonow/subscription_market_state/parquet/'
# OUTPUT_PATH = 's3://sagemaker-mllab/training-data-201901/user_sub_length'

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'dt', 'input_path', 'output_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark_job.run(spark, args)

job.commit()
