import json
import boto3
import requests

events_client = boto3.client('events')
s3 = boto3.client('s3')
polygon_api_key = "vS5DqXeWT0Jgu_28xJweTe8kIcMpF2UW"
url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=1000&sort=ticker&apiKey={polygon_api_key}"
s3_key = "bronzeLayer/allTickersData.json"

def lambda_handler(event, context):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        dataFile = json.dumps(data)
        s3.put_object(Bucket="finstockbucket12", Key=s3_key, Body=dataFile)

        # Send event to EventBridge
        events_client.put_events(
            Entries=[
                {
                    'Source': 'custom.fetchTickersLambda',
                    'DetailType': 'FetchTickersComplete',
                    'Detail': json.dumps({
                        'status': 'success',
                        's3_key': s3_key
                    }),
                    'EventBusName': 'default'
                }
            ]
        )
        return {
            'statusCode': 200,
            'body': json.dumps('Data Upload to S3 Successful!')
        }
    except requests.exceptions.HTTPError as http_err:
        return {
            'statusCode': 400,
            'body': json.dumps(f'HTTP error occurred: {str(http_err)}')
        }
    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps(f'Error: {str(e)}')
        }
