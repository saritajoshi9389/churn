import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from features.session import spark_job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'dt', 'input_path', 'output_path_device', 'output_path_activity'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark_job.run(spark, args)

job.commit()
