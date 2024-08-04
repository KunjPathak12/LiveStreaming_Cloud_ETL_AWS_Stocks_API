import json
import boto3
import requests

def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client('s3')

    # Polygon API key
    polygon_api_key = "vS5DqXeWT0Jgu_28xJweTe8kIcMpF2UW"

    start_date = event.get("startDate")
    end_date = event.get("endDate")
    ticker = event.get("tickerName")
    s3_key = f"bronzeLayer/{ticker}userData.json"

    if not start_date or not end_date or not ticker:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required parameters: startDate, endDate, and ticker must be provided.')
        }

    # Construct the Polygon API URL
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&limit=5000&apiKey={polygon_api_key}"

    try:
        # Fetch the stock data from the Polygon API
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Convert the data to JSON format
        data_file = json.dumps(data)



        # Upload the data to S3 bucket
        s3.put_object(Bucket="finstockbucket", Key=s3_key, Body=data_file)

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
