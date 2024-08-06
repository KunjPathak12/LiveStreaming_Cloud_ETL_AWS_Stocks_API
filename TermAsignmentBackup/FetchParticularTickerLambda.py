import json
import boto3
import requests

events_client = boto3.client('events')
s3 = boto3.client('s3')
polygon_api_key = "vS5DqXeWT0Jgu_28xJweTe8kIcMpF2UW"

def lambda_handler(event, context):
    start_date = event.get("startDate")
    end_date = event.get("endDate")
    ticker = event.get("tickerName")
    s3_key = f"bronzeLayer/{ticker}userData.json"

    if not start_date or not end_date or not ticker:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required parameters: startDate, endDate, and ticker must be provided.')
        }

    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&limit=5000&apiKey={polygon_api_key}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data_file = json.dumps(data)
        s3.put_object(Bucket="finstockbucket12", Key=s3_key, Body=data_file)

        # Send event to EventBridge
        events_client.put_events(
            Entries=[
                {
                    'Source': 'custom.fetchParticularTickerLambda',
                    'DetailType': 'FetchParticularTickerComplete',
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
    except json.JSONDecodeError as json_err:
        return {
            'statusCode': 400,
            'body': json.dumps(f'Error decoding JSON: {str(json_err)}')
        }
    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps(f'Error: {str(e)}')
        }
