import boto3
import time

client = boto3.client('glue')


def lambda_handler(event, context):
    job_run_state = 'RUNNING'
    max_iters = 6
    current_iter = 0
    while job_run_state == 'RUNNING' and current_iter < max_iters:
        time.sleep(15)
        job_run_state = client.get_job_run(
            JobName=event['JobName'],
            RunId=event['JobRunId'])['JobRun']['JobRunState']
        if job_run_state == "STOPPED":
            break
        current_iter = current_iter + 1

    return job_run_state
