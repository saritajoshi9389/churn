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

Fitting a model automatically creates a model. It's not necessary to deploy a model to use batch transform.

< TODO: FILL IN WHEN NOTEBOOKS ARE IN THIS REPO >

For Batch Transform in boto3 or the CLI you'll need [Create Transform Job](https://docs.aws.amazon.com/cli/latest/reference/sagemaker/create-transform-job.html), plus [Describe Transform Job](https://docs.aws.amazon.com/cli/latest/reference/sagemaker/describe-transform-job.html) to track its progress. Under our pipeline, this will be run by boto3 in Lambda.

### Lambda

Lambda functions are in `aws/lambda/`. The Python files are named exactly as the CLI functions. On AWS they are prefixed with `datascience-{SERVICE}-{NAME}` i.e. `datascience-sagemaker-describe-transform-job`. Inputs referenced in the Lambda function will be passed via Step Function's input.

### Step functions

Step functions are in `aws/step_functions/`:
- {type}\_flow.json has the actual JSON that defines the state machine for Step Functions
- {type}\_input.json has sample generic input to pass to cloud events.

TODO: There are 2 flows and inputs here for Sagemaker Batch Transform (transform) and Glue (etl) but there should in fact be ONE complex flow encompassing Glue, Sagemaker Training and Sagemaker Batch Transform, with Lambda wrappers where necessary.

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
