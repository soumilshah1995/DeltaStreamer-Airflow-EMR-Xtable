# DeltaStreamer-Airflow-EMR-Xtable
DeltaStreamer-Airflow-EMR-Xtable
![Screenshot 2024-03-23 at 11 14 50 AM](https://github.com/soumilshah1995/DeltaStreamer-Airflow-EMR-Xtable/assets/39345855/8c6021db-4d22-448a-ba5c-7962c1f66547)


# step 1: Download test data and jar files and upload then into folder /jar and test/ into S3
Link: https://drive.google.com/drive/folders/1mQzZSVgxQGksoXkYGb8DSn2jeR48VehS?usp=share_link


![image](https://github.com/soumilshah1995/stable-with-emr-serverless/assets/39345855/eff90463-3970-45bc-8c29-1d0b9660a836)

# Step 2: Create EMR Spark Cluster 

![image](https://github.com/soumilshah1995/stable-with-emr-serverless/assets/39345855/8b042c5a-2e15-434e-865b-47ad57f13615)


# Step 2: Create airflow project and edit env files 
```
brew install astro
astro dev init

```

###### Edit ENV file 
```
DEV_ACCESS_KEY="XX"
DEV_SECRET_KEY="XXX"
DEV_REGION='us-east-1'
```

# Create Dag
```


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json
import os
import time
import uuid
import boto3

# Define job configuration from the provided event
job_config = {
    "jar": [
        "/usr/lib/hudi/hudi-utilities-bundle.jar",
        "s3://<BUCKETNAME>/jar/hudi-extensions-0.1.0-SNAPSHOT-bundled.jar",
        "s3://<BUCKETNAME>/jar/hudi-java-client-0.14.0.jar"
    ],
    "spark_submit_parameters": [
        "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        "--conf spark.sql.hive.convertMetastoreParquet=false",
        "--conf mapreduce.fileoutputcommitter.marksuccessfuljobs=false",
        "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        "--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer"
    ],
    "arguments": {
        "table-type": "COPY_ON_WRITE",
        "op": "UPSERT",
        "enable-sync": True,
        "sync-tool-classes": "io.onetable.hudi.sync.OneTableSyncTool",
        "source-ordering-field": "replicadmstimestamp",
        "source-class": "org.apache.hudi.utilities.sources.ParquetDFSSource",
        "target-table": "invoice",
        "target-base-path": "s3://<BUCKETNAME>/testcases/",
        "payload-class": "org.apache.hudi.common.model.AWSDmsAvroPayload",
        "hoodie-conf": {
            "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
            "hoodie.datasource.write.recordkey.field": "invoiceid",
            "hoodie.datasource.write.partitionpath.field": "destinationstate",
            "hoodie.deltastreamer.source.dfs.root": "s3://<BUCKETNAME>/test/",
            "hoodie.datasource.write.precombine.field": "replicadmstimestamp",
            "hoodie.database.name": "hudidb",
            "hoodie.datasource.hive_sync.enable": True,
            "hoodie.datasource.hive_sync.table": "invoice",
            "hoodie.datasource.hive_sync.partition_fields": "destinationstate",
            "hoodie.onetable.formats.to.sync": "ICEBERG",
            "hoodie.onetable.target.metadata.retention.hr": 168
        }
    },
    "job": {
        "job_name": "delta_streamer_bronze_invoice",
        "created_by": "Soumil Shah",
        "created_at": "2024-03-20",
        "ApplicationId": "XXXX",
        "ExecutionTime": 600,
        "JobActive": True,
        "schedule": "@daily",
        "JobStatusPolling": True,
        "JobDescription": "Ingest data from parquet source",
        "ExecutionArn": "XXX",
    }
}

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 3, 20),
}


def check_job_status(client, run_id, applicationId):
    response = client.get_job_run(applicationId=applicationId, jobRunId=run_id)
    return response['jobRun']['state']


def lambda_handler(event, context):
    # Create EMR serverless client object
    client = boto3.client("emr-serverless",
                          aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
                          aws_secret_access_key=os.getenv("DEV_SECRET_KEY"),
                          region_name=os.getenv("DEV_REGION"))

    # Extracting parameters from the event
    jar = event.get("jar", [])
    # Add --conf spark.jars with comma-separated values from the jar object
    spark_submit_parameters = ' '.join(event.get("spark_submit_parameters", []))  # Convert list to string
    spark_submit_parameters = f'--conf spark.jars={",".join(jar)} {spark_submit_parameters}'  # Join with existing parameters

    arguments = event.get("arguments", {})
    job = event.get("job", {})

    # Extracting job details
    JobName = job.get("job_name")
    ApplicationId = job.get("ApplicationId")
    ExecutionTime = job.get("ExecutionTime")
    ExecutionArn = job.get("ExecutionArn")

    # Processing arguments
    entryPointArguments = []
    for key, value in arguments.items():
        if key == "hoodie-conf":
            # Extract hoodie-conf key-value pairs and add to entryPointArguments
            for hoodie_key, hoodie_value in value.items():
                entryPointArguments.extend(["--hoodie-conf", f"{hoodie_key}={hoodie_value}"])
        elif isinstance(value, bool):
            # Add boolean parameters without values if True
            if value:
                entryPointArguments.append(f"--{key}")
        else:
            entryPointArguments.extend([f"--{key}", f"{value}"])

    # Starting the EMR job run
    response = client.start_job_run(
        applicationId=ApplicationId,
        clientToken=str(uuid.uuid4()),
        executionRoleArn=ExecutionArn,
        jobDriver={
            'sparkSubmit': {
                'entryPoint': "command-runner.jar",
                'entryPointArguments': entryPointArguments,
                'sparkSubmitParameters': spark_submit_parameters
            },
        },
        executionTimeoutMinutes=ExecutionTime,
        name=JobName
    )

    if job.get("JobStatusPolling") == True:
        # Polling for job status
        run_id = response['jobRunId']
        print("Job run ID:", run_id)

        polling_interval = 3
        while True:
            status = check_job_status(client=client, run_id=run_id, applicationId=ApplicationId)
            print("Job status:", status)
            if status in ["FAILED"]:
                break

            if status in ["CANCELLED", "SUCCESS"]:
                break
            time.sleep(polling_interval)  # Poll every 3 seconds

    return {
        "statusCode": 200,
        "body": json.dumps(response)
    }


# Function to execute Lambda handler
def execute_lambda_handler(event, context):
    return lambda_handler(event, context)


# Define Airflow DAG
def create_dag(job_config):
    dag_name = f"dag_{job_config['job']['job_name']}"
    schedule = job_config['job']['schedule']

    dag = DAG(
        dag_id=dag_name,
        default_args=default_args,
        description=job_config['job']['JobDescription'],
        schedule_interval=schedule,
    )

    # Define task to execute Lambda handler
    execute_lambda_task = PythonOperator(
        task_id='execute_lambda_task',
        python_callable=execute_lambda_handler,
        op_kwargs={'event': job_config, 'context': None},
        dag=dag,
    )

    return dag


# Create the DAG
dag = create_dag(job_config)

```
