import json
import boto3

glue_client = boto3.client('glue')

def lambda_handler(event, context):
    for record in event['Records']:
        payload = json.loads(record['body'])
        s3_key = payload.get('file')
        glue_job_name = payload.get('glueJob')

        if not glue_job_name or not s3_key:
            continue

        # Start the Glue job with the S3 key as an argument
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments={
                '--s3_input_key': s3_key
            }
        )

        print(f"Started Glue job {glue_job_name}: {response['JobRunId']}")

    return {
        'statusCode': 200,
        'body': json.dumps('Glue job triggered successfully!')
    }
