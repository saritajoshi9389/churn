# Churn scoring MVP

Feature engineering is Spark jobs in `features/`.

AWS glue scripts requires this repo's `features` module, plus `dateutil`. Zip these for use by AWS Glue (e.g. `zip -r churn-packages.zip features venv/lib/python2.7/site-packages/dateutil`) and upload to S3.

AWS glue scripts themselves in `glue/` should be uploaded to S3.

TODO (blocked as we require AWS command-line access):
Scripts to
- zip the requirements and upload to S3
- upload the glue scripts to s3
- create a glue job using the Python SDK
- remove the `dt` argument so it always runs yesterday?

## Local setup and tests

For local setup:

`pip install -r requirements.txt`

For testing:

`python -m pytest tests/`

To actually run the jobs, use AWS Glue (using the scripts in `glue/`, passing the appropriate job arguments) or spark-submit on a cluster (no script has been authored for this).
