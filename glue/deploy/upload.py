from subprocess import call
import boto3

def zip_upload(job_name):
    call(['zip', '-r', 'churn-packages.zip', 'features', 'venv/lib/python2.7/site-packages/dateutil'])
    boto3.setup_default_session(profile_name='dsnonprod')
    s3 = boto3.resource('s3')

    s3.meta.client.upload_file(
        'churn-packages.zip',
        'datascience-feature-eng-test-201901',
        'churn-packages.zip')
    s3.meta.client.upload_file(
        'glue/{}.py'.format(job_name),
        'datascience-feature-eng-test-201901',
        '{}.py'.format(job_name))
