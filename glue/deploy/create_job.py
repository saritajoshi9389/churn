import argparse
import boto3

from upload import zip_upload

parser = argparse.ArgumentParser()
parser.add_argument('--name', required=True)
parser.add_argument('--input_path', required=True)
parser.add_argument('--output_path', required=True)
args = parser.parse_args()

job_name = args.name
zip_upload(job_name)

glue = boto3.client('glue')
glue.create_job(
    Name=job_name,
    Role='datascience-glue-service-role',
    Command={
        'Name': job_name,
        'ScriptLocation': 's3://datascience-feature-eng-test-201901/{}.py'.format(job_name)
    },
    DefaultArguments={
        '--extra-py-files': 's3://datascience-feature-eng-test-201901/churn-packages.zip',
        '--input_path': args.input_path,
        '--output_path': args.output_path
    })
