import boto3
import time

sagemaker = boto3.client('sagemaker')


def lambda_handler(event, context):
    job_run_state = 'InProgress'
    max_iters = 89
    current_iter = 0
    while job_run_state == 'InProgress' and current_iter < max_iters:
        time.sleep(10)
        job_run_state = sagemaker.describe_transform_job(
            TransformJobName=event['TransformArguments']['TransformJobName'])['TransformJobStatus']
        current_iter = current_iter + 1

    return job_run_state
