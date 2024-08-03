import json
import boto3
import requests

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    sqs = boto3.client('sqs')
    polygon_api_key = "vS5DqXeWT0Jgu_28xJweTe8kIcMpF2UW"  
    startDate = event.get("startDate")
    endDate = event.get("endDate")
    ticker = event.get("tickerName")
    sqs_queue_url = "https://sqs.us-east-1.amazonaws.com/802085315584/FinstockQueue"
    
    if not startDate or not endDate or not ticker:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required parameters: startDate, endDate, and ticker must be provided.')
        }
    
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{startDate}/{endDate}?adjusted=true&sort=asc&limit=5000&apiKey={polygon_api_key}"

    try:
        response = requests.get(url)
        response.raise_for_status()  
        dataFile = response.json()
        dataFile = json.dumps(dataFile)
        
        s3_key = f"bronzeLayer/{ticker}userData.json"
        
        s3.put_object(Bucket="finstocktickers", Key=s3_key, Body=dataFile)
        
        sqs.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=json.dumps({
                "file": s3_key,
                "glueJob": "FetchUserTicker"
            })
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
