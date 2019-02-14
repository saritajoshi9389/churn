# Churn scoring MVP

AWS scripts and configs are in `aws/`.

AWS glue scripts requires this repo's `features` module, which contains all Spark jobs for feature engineering.

# Deploy

### Glue

To deploy a Glue job, assume the appropriate role and then you can use the
- web console
- [awscli](https://docs.aws.amazon.com/cli/latest/reference/glue/index.html#cli-aws-glue)
- [Python SDK aka boto3](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-calling.html)

1. Zip the Python requirements
    `zip -r churn-packages.zip features venv/lib/python2.7/site-packages/dateutil`
2. Upload churn-packages.zip to S3, using the appropriate role.
3. If the Glue job (i.e. the script in the `aws/glue/` directory) has changed, upload it to S3 using the appropriate role. However, it is unlikely that the glue job will change as it is primarily boilerplate.
4. If a job with this name and glue script does not exist, create a new one via the above methods. Set the following options (`cli` | `Python keyword argument`):
    - job name (`--name` | `Name`)
    - role (`--role` | `Role`)
    - command (`--command Name=glueetl,ScriptLocation=s3://script-location` | `Command={'Name': 'glueetl', 'ScriptLocation': 's3://script-location'}`) _the command.name must be glueetl_
    - default arguments (`--default-arguments --extra-py-files={S3 LOCATION}/churn-packages.zip` | `DefaultArguments={
        '--extra-py-files': '{S3 LOCATION}/churn-packages.zip'`) _--extra-py-files is a special Glue parameter_
        - You might also add any other job arguments to the default arguments.

To actually run jobs you have a choice of:
- start-job-run
- create-trigger

**Pass all expected arguments (defined in the glue script) to the job.**

To see a job status
- get-job-run

### Sagemaker

### Lambda

Add instructions

### Step functions

# Local setup and tests

You'll need a machine with Spark installed. 

On a laptop, you likely need these in your bash profile:
- export SPARK_HOME={YOUR SPARK HOME}
- export JAVA_HOME={YOUR JAVA HOME}
- export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

Create a virtual environment in this directory: `virtualenv venv` The `venv` folder is in the .gitignore.

Activate the virtual environment: `source venv/bin/activate`

Install requirements: `pip install -r requirements.txt`
    - On an Amazon Linux box, you might also need to do:
    `pip install --target={ABSOLUTE PATH TO YOUR VENV}/venv/lib/python2.7/dist-packages/ scandir pandas --upgrade`

To run tests: `python -m pytest tests/`
