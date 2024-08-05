import json
import boto3

glue_client = boto3.client('glue')

def lambda_handler(event, context):
    print("Received event:", json.dumps(event, indent=2))  # Log the event for debugging

    # Extract details from the event
    detail = event.get('detail', {})
    status = detail.get('status')
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
        print(f"Started Glue job {glue_job_name}: {response['JobRunId']}")
        return {
            'statusCode': 200,
            'body': json.dumps(f'Glue job {glue_job_name} triggered successfully!')
        }
    except Exception as e:
        print(f"Error starting Glue job {glue_job_name}: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error starting Glue job {glue_job_name}: {str(e)}')
        }
