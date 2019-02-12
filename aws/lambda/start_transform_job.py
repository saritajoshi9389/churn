import boto3
import os
from datetime import datetime, timedelta

sagemaker = boto3.client('sagemaker')
S3_PATH = 's3://datascience-feature-eng-test-201901/users-joined-201901/'


def lambda_handler(event, context):
    transform_inputs = event['TransformArguments']
    if transform_inputs['TransformInput']['DataSource']['S3DataSource']['S3Uri'] == '':
        yesterday_dt = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        transform_inputs['TransformInput']['DataSource']['S3DataSource']['S3Uri'] = '{}/dt=yesterday_dt'.format(S3_PATH)

    response = sagemaker.create_transform_job(
        TransformJobName=transform_inputs['TransformJobName'],
        ModelName=transform_inputs['ModelName'],
        TransformInput=transform_inputs['TransformInput'],
        TransformOutput=transform_inputs['TransformOutput'],
        TransformResources=transform_inputs['TransformResources']
    )
    return response['TransformJobArn']
