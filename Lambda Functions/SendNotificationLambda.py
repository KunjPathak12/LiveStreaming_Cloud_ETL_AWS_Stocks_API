import json
import boto3
import os

sns_client = boto3.client('sns')

def lambda_handler(event, context):
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    print("Received event:", json.dumps(event, indent=2))  # Log the event for debugging

    # Extract S3 key and job status from the event
    detail = event.get('detail', {})
    s3_key = detail.get('s3_key')
    s3_bucket = "finstockbucket12"

    # Create the S3 file URL
    s3_url = f"https://{s3_bucket}.s3.amazonaws.com/{s3_key}"

    message = {
        "default": f"ETL job '{detail.get('job_name')}' completed with status: {detail.get('status')}.",
        "email": f"ETL job '{detail.get('job_name')}' completed with status: {detail.get('status')}. Output available in S3: {s3_url}"
    }

    response = sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=json.dumps(message),
        Subject=f"ETL Job Notification"
    )

    print(f"Notification sent: {response['MessageId']}")

    return {
        'statusCode': 200,
        'body': json.dumps('Notification sent successfully!')
    }
