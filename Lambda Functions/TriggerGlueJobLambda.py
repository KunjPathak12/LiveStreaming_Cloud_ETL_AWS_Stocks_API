import json
import boto3
import os
import time

glue_client = boto3.client('glue')
events_client = boto3.client('events')
sns_client = boto3.client('sns')
sns_topic_arn = os.environ['SNS_TOPIC_ARN']

def lambda_handler(event, context):
    print("Received event:", json.dumps(event, indent=2))  # Log the event for debugging

    # Extract details from the event
    detail = event.get('detail', {})
    s3_key = detail.get('s3_key')

    if not s3_key:
        print("No S3 key found in the event detail.")
        return {
            'statusCode': 400,
            'body': json.dumps('No S3 key found in the event detail.')
        }

    # Determine which Glue job to trigger based on S3 key
    if s3_key.endswith('allTickersData.json'):
        glue_job_name = 'fetchAllTickersETL'
    elif s3_key.endswith('userData.json'):
        glue_job_name = 'fetchUserTickerETL'
    else:
        print(f"Unknown S3 key: {s3_key}")
        return {
            'statusCode': 400,
            'body': json.dumps('Unknown S3 key.')
        }

    # Start the Glue job
    try:
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments={
                '--s3_input_key': s3_key
            }
        )
        job_run_id = response['JobRunId']
        print(f"Started Glue job {glue_job_name}: {job_run_id}")

        # Wait for the Glue job to complete (with a timeout)
        timeout = 600  # Set your timeout (in seconds)
        interval = 60   # Check every 15 seconds

        for _ in range(timeout // interval):
            job_run = glue_client.get_job_run(JobName=glue_job_name, RunId=job_run_id)
            job_status = job_run['JobRun']['JobRunState']
            if job_status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                break
            time.sleep(interval)

        # Send notification to SNS about the job completion
        sns_message = f"Glue job '{glue_job_name}' completed with status: {job_status}."
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=sns_message,
            Subject=f"Glue Job Completed: {glue_job_name}"
        )

        # Send event to EventBridge
        events_client.put_events(
            Entries=[
                {
                    'Source': 'custom.triggerGlueJobsLambda',
                    'DetailType': 'GlueJobCompletion',
                    'Detail': json.dumps({
                        'job_name': glue_job_name,
                        'job_run_id': job_run_id,
                        'status': job_status,
                        's3_key': s3_key,
                    }),
                    'EventBusName': 'default'
                }
            ]
        )

        return {
            'statusCode': 200,
            'body': json.dumps(f'Glue job {glue_job_name} triggered successfully with run ID {job_run_id}!')
        }
    except Exception as e:
        print(f"Error starting Glue job {glue_job_name}: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error starting Glue job {glue_job_name}: {str(e)}')
        }
