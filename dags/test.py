from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import base64
import time
import jwt
import decimal
import requests
import urllib3
import boto3
from datetime import datetime
from os import path
from airflow.decorators import task
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook


def get_token():
    """Fetch auth token from K8s secret."""
    with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
        namespace = f.read()
    k8sCoreApiClient = KubernetesHook().core_v1_client
    secret = k8sCoreApiClient.read_namespaced_secret("access-token", namespace)
    token_encoded = secret.data["AUTH_TOKEN"]  # type: ignore
    return base64.b64decode(token_encoded).decode("utf-8")

def get_s3_client(endpoint_host: str, ssl_enabled: bool):
    """Return boto3 client configured with dynamic JWT auth."""
    endpoint_url = f"http{'s' if ssl_enabled else ''}://{endpoint_host}"
    jwt_token = get_token()
    s3 = boto3.client(
        "s3",
        aws_access_key_id=jwt_token,
        aws_secret_access_key="s3",
        endpoint_url=endpoint_url,
        use_ssl=ssl_enabled,
    )
    return s3
def get_buckets():
    s3_client = get_s3_client("'http://local-s3-service.ezdata-system.svc.cluster.local:30000", False)
    s3_client.list_buckets()
    for bucket in response['Buckets']:
        print(f"  - {bucket['Name']}")
        
with DAG(
    dag_id='get_buckets',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['example'],
) as dag:
    print_var_task = PythonOperator(
        task_id='get_buckets',
        python_callable=get_buckets,
    )
