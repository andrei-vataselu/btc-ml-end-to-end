import json
import boto3
import os

sagemaker = boto3.client("sagemaker")

def lambda_handler(event, context):
    job_name = f"your-job-name-{int(time.time())}"
    
    response = sagemaker.create_processing_job(
        ProcessingJobName=job_name,
        RoleArn=os.environ["SAGEMAKER_ROLE_ARN"],
        AppSpecification={
            "ImageUri": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sklearn-processing:1.2-1-cpu-py3"
        },
        ProcessingInputs=[
            {
                "InputName": "script",
                "S3Input": {
                    "S3Uri": "s3://<your-bucket>/scripts/train_model_pro_print.py",
                    "LocalPath": "/opt/ml/processing/input"
                }
            }
        ],
        ProcessingOutputConfig={
            "Outputs": [
                {
                    "OutputName": "model-artifacts",
                    "S3Output": {
                        "S3Uri": "s3://<your-bucket>/models/volatility_model/",
                        "LocalPath": "/opt/ml/processing/output",
                        "S3UploadMode": "EndOfJob"
                    }
                }
            ]
        },
        ProcessingResources={
            "ClusterConfig": {
                "InstanceCount": 1,
                "InstanceType": "ml.m5.large",
                "VolumeSizeInGB": 30
            }
        },
        Environment={
            "BUCKET": "<your-bucket>",
            "PREFIX": "processed_partitioned"
        }
    )
    return {"statusCode": 200, "body": json.dumps(response)}
