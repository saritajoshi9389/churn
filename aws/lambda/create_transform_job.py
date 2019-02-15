import boto3
import os
from datetime import datetime, timedelta


def lambda_handler(event, context):
    sagemaker = boto3.client('sagemaker')

    transform_inputs = event['TransformArguments']
    yesterday_dt = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

    input_path = transform_inputs['TransformInput']['DataSource']['S3DataSource']['S3Uri']
    transform_inputs['TransformInput']['DataSource']['S3DataSource']['S3Uri'] = os.path.join(
        input_path, 'dt={}'.format(yesterday_dt))

    transform_inputs['TransformOutput']['S3OutputPath']
    transform_inputs['TransformOutput']['S3OutputPath'] = os.path.join(output_path, 'dt={}'.format(yesterday_dt))

    transform_inputs['TransformJobName'] = 'churn-daily-scoring-{}'.format(datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
    sagemaker.create_transform_job(**transform_inputs)
    return transform_inputs
