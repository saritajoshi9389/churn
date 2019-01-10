import argparse
import boto3

boto3.setup_default_session(profile_name='dsnonprod')

parser = argparse.ArgumentParser()
parser.add_argument('--name', required=True)
parser.add_argument('--dt', required=True)
args = parser.parse_args()

glue = boto3.client('glue')
job_name = args.name

response = glue.start_job_run(
    JobName=job_name,
    Arguments={
        '--dt': args.dt
    })
