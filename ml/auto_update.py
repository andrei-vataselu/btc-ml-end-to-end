import json
import os
import boto3
import time
from datetime import datetime, timezone

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
ENDPOINT_NAME = os.getenv("ENDPOINT_NAME", "btc-volatility-endpoint")
MODEL_NAME = os.getenv("MODEL_NAME", "btc-volatility-model")
ENDPOINT_CONFIG_PREFIX = os.getenv("ENDPOINT_CONFIG_PREFIX", "btc-volatility-config")
BUCKET = os.getenv("BUCKET", "btc-prices-decision")
MODEL_ARTIFACT_PREFIX = os.getenv("MODEL_ARTIFACT_PREFIX", "models/volatility_model/volatility_model.joblib")
ROLE_ARN = os.getenv("SAGEMAKER_ROLE_ARN", "<YOUR-SAGEMAKER-EXECUTION-ROLE-ARN>")
INFERENCE_IMAGE = os.getenv("INFERENCE_IMAGE", "683313688378.dkr.ecr.us-east-1.amazonaws.com/sklearn-inference:1.2-cpu-py3")

sagemaker = boto3.client("sagemaker", region_name=AWS_REGION)

def lambda_handler(event, context):
    try:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        new_config_name = f"{ENDPOINT_CONFIG_PREFIX}-{ts}"
        new_model_name = f"{MODEL_NAME}-{ts}"
        model_data_url = f"s3://{BUCKET}/{MODEL_ARTIFACT_PREFIX}"
        print(f"Using model artifact: {model_data_url}")
        print(f"Creating model: {new_model_name}")
        sagemaker.create_model(
            ModelName=new_model_name,
            ExecutionRoleArn=ROLE_ARN,
            PrimaryContainer={
                "Image": INFERENCE_IMAGE,
                "ModelDataUrl": model_data_url,
                "Mode": "SingleModel"
            }
        )
        print(f"Creating endpoint config: {new_config_name}")
        sagemaker.create_endpoint_config(
            EndpointConfigName=new_config_name,
            ProductionVariants=[
                {
                    "VariantName": "AllTraffic",
                    "ModelName": new_model_name,
                    "InitialInstanceCount": 1,
                    "InstanceType": "ml.m5.large",
                    "InitialVariantWeight": 1.0
                }
            ]
        )
        print(f"Updating endpoint {ENDPOINT_NAME}")
        sagemaker.update_endpoint(
            EndpointName=ENDPOINT_NAME,
            EndpointConfigName=new_config_name
        )
        print("Waiting for endpoint to become InService")
        while True:
            status = sagemaker.describe_endpoint(EndpointName=ENDPOINT_NAME)["EndpointStatus"]
            print("Status:", status)
            if status in ("InService", "Failed"):
                break
            time.sleep(60)
        cleanup_old = os.getenv("CLEANUP_OLD", "false").lower() == "true"
        if cleanup_old:
            cleanup_old_resources(current_model=new_model_name, current_config=new_config_name)
        print(f"Endpoint {ENDPOINT_NAME} is now serving model {new_model_name}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "endpoint": ENDPOINT_NAME,
                "model": new_model_name,
                "config": new_config_name,
                "status": "InService"
            })
        }
    except Exception as e:
        print("Endpoint update failed:", e)
        return {"statusCode": 500, "error": str(e)}

def cleanup_old_resources(current_model, current_config):
    print("Cleaning up old endpoint configs and models")
    configs = sagemaker.list_endpoint_configs(SortBy="CreationTime", SortOrder="Descending")
    for c in configs.get("EndpointConfigs", []):
        name = c["EndpointConfigName"]
        if name != current_config and name.startswith(ENDPOINT_CONFIG_PREFIX):
            try:
                print(f"Deleting old config: {name}")
                sagemaker.delete_endpoint_config(EndpointConfigName=name)
            except Exception as e:
                print("Failed to delete config:", e)
    models = sagemaker.list_models(SortBy="CreationTime", SortOrder="Descending")
    for m in models.get("Models", []):
        name = m["ModelName"]
        if name != current_model and name.startswith(MODEL_NAME):
            try:
                print(f"Deleting old model: {name}")
                sagemaker.delete_model(ModelName=name)
            except Exception as e:
                print("Failed to delete model:", e)
