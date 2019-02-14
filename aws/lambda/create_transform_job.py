import boto3
import os
from datetime import datetime, timedelta

S3_INPUT_PATH = 's3://datascience-feature-eng-test-201901/users-joined-201901/'
S3_OUTPUT_PATH = 's3://datascience-feature-eng-test-201901/sagemaker/test-output/'


def lambda_handler(event, context):
    sagemaker = boto3.client('sagemaker')

    transform_inputs = event['TransformArguments']
    if transform_inputs['TransformInput']['DataSource']['S3DataSource']['S3Uri'] == 's3://set-by-lambda':
        yesterday_dt = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        transform_inputs['TransformInput']['DataSource']['S3DataSource']['S3Uri'] = os.path.join(
            S3_INPUT_PATH, 'dt={}'.format(yesterday_dt))
        transform_inputs['TransformOutput']['S3OutputPath'] = os.path.join(
            S3_OUTPUT_PATH, 'dt={}'.format(yesterday_dt))
    transform_inputs['TransformJobName'] = 'churn-daily-scoring-{}'.format(datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
    sagemaker.create_transform_job(**transform_inputs)
    return transform_inputs
