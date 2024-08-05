import json
import boto3
import os

sns_client = boto3.client('sns')

def lambda_handler(event, context):
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')

    if not sns_topic_arn:
        print("SNS_TOPIC_ARN environment variable is not set.")
        return {
            'statusCode': 500,
            'body': json.dumps('SNS_TOPIC_ARN environment variable is not set.')
        }

    message = {
        "default": "ETL job succeeded.",
        "email": "ETL job succeeded. Output available in S3."
    }

    try:
        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(message),
            Subject="ETL Job Notification"
        )
        print(f"Notification sent: {response['MessageId']}")
        return {
            'statusCode': 200,
            'body': json.dumps('Notification sent successfully!')
        }
    except Exception as e:
        print(f"Error sending notification: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error sending notification: {str(e)}')
        }
