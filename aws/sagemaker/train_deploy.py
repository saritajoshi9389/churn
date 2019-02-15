"""
Sample code to deploy a linear learner.
Deploy in notebook - command-line not authorized to deploy.

Prerequisite: Training data in CSV form in S3, with the target / y variable
in the first column.
"""

import boto3
import sagemaker
from sagemaker import get_execution_role
from sagemaker.amazon.amazon_estimator import get_image_uri

bucket = 'datascience-churn'
prefix = 'sagemaker-aws/sagemaker'
NUM_FEATURES = 19

role = get_execution_role()
container = get_image_uri(boto3.Session().region_name, 'linear-learner')
session = sagemaker.Session()

s3_input_train = sagemaker.s3_input(
    s3_data='s3://{}/{}/train'.format(bucket, prefix),
    content_type='text/csv')
s3_input_validation = sagemaker.s3_input(
    s3_data='s3://{}/{}/validation'.format(bucket, prefix),
    content_type='text/csv')
# s3_input_test = sagemaker.s3_input(s3_data='s3://{}/{}/test'.format(
#     bucket, prefix),
#     content_type='text/csv')

linear = sagemaker.estimator.Estimator(
    container,
    role,
    train_instance_count=1,
    train_instance_type='ml.c4.xlarge',
    output_path='s3://{}/{}/model'.format(bucket, prefix),
    sagemaker_session=session)

linear.set_hyperparameters(
    feature_dim=NUM_FEATURES,
    predictor_type='binary_classifier',
    loss='hinge_loss',
    binary_classifier_model_selection_criteria='f_beta',
    f_beta=1.5,
    epochs=5,
    early_stopping_patience=1,
    mini_batch_size=5000)

linear.fit({
    'train': s3_input_train,
    'validation': s3_input_validation,
    # 'test': s3_input_test
})

# to DEPLOY a model - not necessary for batch transform.
# linear_predictor = linear.deploy(initial_instance_count=1,
#                                  instance_type='ml.m4.xlarge')
