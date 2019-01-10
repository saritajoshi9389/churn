from subprocess import call
import boto3

call(['zip', '-r', 'churn-packages.zip', 'features', 'venv/lib/python2.7/site-packages/dateutil'])
boto3.setup_default_session(profile_name='dsnonprod')
s3 = boto3.resource('s3')

s3.meta.client.upload_file('churn-packages.zip', 'datascience-feature-eng-test-201901', 'churn-packages.zip')
s3.meta.client.upload_file('glue/subscription.py', 'datascience-feature-eng-test-201901', 'subscription.py')

glue = boto3.client('glue')
glue.create_job(
    Name='subscription',
    Role='datascience-glue-service-role',
    Command={
        'Name': 'subscription',
        'ScriptLocation': 's3://datascience-feature-eng-test-201901/subscription.py'
    },
    DefaultArguments={
        '--extra-py-files': 's3://datascience-feature-eng-test-201901/churn-packages.zip',
        '--input_path': 's3://hbo-data-account-receipts/hbonow/subscription_state/parquet',
        '--output_path': 's3://datascience-feature-eng-test-201901/user-sub-length'
    })

