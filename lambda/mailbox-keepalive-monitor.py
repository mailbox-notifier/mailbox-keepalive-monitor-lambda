"""
This Lambda function checks the timestamp of an entry in a DynamoDB table and sends an SNS notification
if the entry is older than a specified threshold. It's designed to monitor a mailbox state, alerting if
the state hasn't been updated in a set period.

Environment Variables:
    - DDB_TABLE_NAME: The name of the DynamoDB table to check.
    - SNS_ARN: The ARN of the SNS topic for sending alerts.
    - THRESHOLD_HOURS: The time threshold in hours for sending an alert.

The DynamoDB table should have an entry with a timestamp to compare against the current time.
"""

import datetime
import os

import boto3
import pytz
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    """
    The AWS Lambda handler function.

    Reads the specified DynamoDB table to retrieve a timestamp and checks if this timestamp
    is older than a set threshold. If so, it sends a notification via SNS.

    Args:
        event: The Lambda event object (not used in this function).
        context: The Lambda context object (not used in this function).

    Returns:
        str: A message indicating the outcome of the function execution.
    """
    print(event)  # For testing purposes

    # Retrieve environment variables
    ddb_table_name = os.environ.get('DDB_TABLE_NAME')
    sns_arn = os.environ.get('SNS_ARN')
    threshold_hours_env = os.environ.get('THRESHOLD_HOURS')

    # Verify that all required environment variables are set
    if not ddb_table_name or not sns_arn or not threshold_hours_env:
        return "Error: Environment variables DDB_TABLE_NAME, SNS_ARN, and THRESHOLD_HOURS must be set."

    # Convert threshold hours to integer
    try:
        threshold_hours = int(threshold_hours_env)
    except ValueError:
        return "Error: Invalid value for THRESHOLD_HOURS. It must be an integer."

    # Initialize clients for DynamoDB and SNS
    dynamodb = boto3.resource('dynamodb')
    sns = boto3.client('sns')
    table = dynamodb.Table(ddb_table_name)

    try:
        # Retrieve the timestamp from DynamoDB
        response = table.get_item(Key={'id': 'open'})
        if 'Item' in response and 'timestamp' in response['Item']:
            timestamp_str = response['Item']['timestamp']
            timestamp = datetime.datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
            timestamp = timestamp.replace(tzinfo=pytz.timezone('US/Central'))

            # Check if the timestamp is older than the threshold
            if datetime.datetime.now(pytz.timezone('US/Central')) - timestamp > datetime.timedelta(
                    hours=threshold_hours):
                # Send an SNS notification
                sns.publish(
                    TopicArn=sns_arn,
                    Message=f"The timestamp in DynamoDB is over {threshold_hours} hours old.",
                    Subject="Mailbox State Alert"
                )
                return "SNS notification sent"
            else:
                return "Timestamp is within the threshold"
        else:
            return "No timestamp found in DynamoDB"
    except ClientError as e:
        print(f"Error: {e}")
        return "Error occurred"


# For local testing (if required)
if __name__ == "__main__":
    print(lambda_handler(None, None))
