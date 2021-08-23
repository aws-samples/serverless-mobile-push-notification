import json
import boto3, botocore
import os
from boto3.dynamodb.conditions import Key


SUPPORTED_PLATFORM = {'andriod': os.environ['ANDROID_SNS_PLATFORM'],
                      'ios': os.environ['IOS_SNS_PLATFORM']}
REGISTRATION_DDB_TABLE = os.environ['REGISTRATION_DDB_TABLE']

dynamodb_resource = boto3.resource('dynamodb', region_name='ap-southeast-2')
sns_client = boto3.client('sns', region_name='ap-southeast-2')

table = dynamodb_resource.Table(REGISTRATION_DDB_TABLE)


def http_response(response_code, message):
    return {
        'statusCode': response_code,
        'body': json.dumps(message)
    }


def delete_other_users_same_endpoint(device_push_endpoint, username, mobileOS):
    result = table.query(
    # Add the name of the index you want to use in your query.
        IndexName="endpoint-index",
        KeyConditionExpression=Key('endpoint').eq(device_push_endpoint),
    )
    for item in result['Items']:
        if item['username'] != username and item['mobileOS'] == mobileOS:
            print(f"deleting orphan item: username {username}, os {mobileOS}".format(username=item['username'], mobileOS=mobileOS))
            table.delete_item(
                Key={
                    'username': item['username'],
                    'mobileOS': mobileOS
                },
            )



def lambda_handler(event, context):
    body = json.loads(event['body'])
    print(body)
    if 'username' not in body:
        return http_response(400, "username not in payload")
    username = body['username']
    if 'mobileOS' not in body or body['mobileOS'] not in SUPPORTED_PLATFORM:
        return http_response(400, "mobileOS not in payload or not a valid value")
    mobileOS = body['mobileOS']
    if 'token' not in body:
        return http_response(400, "token not in payload")
    token = body['token']

    try:
        need_update_ddb = False
        response = table.get_item(Key={'username': username, 'mobileOS': mobileOS})
        if 'Item' not in response:
            # create endpoint
            response = sns_client.create_platform_endpoint(
                PlatformApplicationArn=SUPPORTED_PLATFORM[mobileOS],
                Token=token,
            )
            device_push_endpoint = response['EndpointArn']
            need_update_ddb = True
        else:
            # update the endpoint
            device_push_endpoint = response['Item']['endpoint']

        try:
            response = sns_client.get_endpoint_attributes(
                EndpointArn=device_push_endpoint
            )
            endpoint_attributes = response['Attributes']

            previous_token = endpoint_attributes['Token']
            previous_status = endpoint_attributes['Enabled']
            if previous_status.lower() != 'true' or previous_token != token:
                sns_client.set_endpoint_attributes(
                    EndpointArn=device_push_endpoint,
                    Attributes={
                        'Token': token,
                        'Enabled': 'true'
                    }
                )
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'NotFound':
                response = sns_client.create_platform_endpoint(
                    PlatformApplicationArn=SUPPORTED_PLATFORM[mobileOS],
                    Token=token,
                )
                device_push_endpoint = response['EndpointArn']
                need_update_ddb = True
            else:
                print(error)
                return http_response(500, "operation failed")

        delete_other_users_same_endpoint(device_push_endpoint, username, mobileOS)

        if need_update_ddb:
            table.update_item(
                Key={
                    'username': username,
                    'mobileOS': mobileOS
                },
                UpdateExpression="set endpoint=:e",
                ExpressionAttributeValues={
                    ':e': device_push_endpoint
                },
                ReturnValues="UPDATED_NEW"
            )

    except botocore.exceptions.ClientError as error:
        raise error

    return http_response(200, "token registered")
