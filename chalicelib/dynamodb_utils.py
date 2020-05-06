import boto3
import dynamo_json
from chalicelib import utils

DYNAMO_CLIENT = boto3.client('dynamodb')
TABLE_NAME = 'slack-broadcast-webhooks'

# Queries the dynamodb table.
# Note. The query is strongly consistent if made on base table i.e.
# if channel_name is not sent while calling the function.
# If channel name is sent, the read will be eventually consistent as it queries the index.
# If channel ID is not sent, the function returns all the channels for the user.


def get_channels(user_id, channel_id=None, channel_name=None):
    key_condition = 'COGNITO_USERNAME = :user_id'
    expression_attribute_values = {
        ':user_id': {'S': user_id}
    }
    if channel_id is not None:
        key_condition += ' and CHANNEL_ID = :channel_id'
        expression_attribute_values.update({
            ':channel_id': {'S': channel_id}
        })

    if channel_name is None:
        print("Consistent Read")
        res = DYNAMO_CLIENT.query(
            TableName=TABLE_NAME,
            KeyConditionExpression=key_condition,
            ExpressionAttributeValues=expression_attribute_values,
            ConsistentRead=True
        )

    elif channel_name is not None:
        res = DYNAMO_CLIENT.query(
            TableName=TABLE_NAME,
            IndexName="COGNITO_USERNAME-CHANNEL_NAME-index",
            KeyConditionExpression="COGNITO_USERNAME = :user_id AND CHANNEL_NAME = :channel_name",
            ExpressionAttributeValues={
                ':user_id': {'S': user_id},
                ':channel_name': {'S': channel_name}
            }
        )
    # print(res['Items'])
    return res['Items']


def update_channel(user_id, channel_id, channel_name, channel_webhook):
    try:
        dynamo_res = DYNAMO_CLIENT.update_item(
            TableName=TABLE_NAME,
            Key={
                'COGNITO_USERNAME': {'S': user_id},
                'CHANNEL_ID': {'S': channel_id}
            },
            UpdateExpression='SET CHANNEL_NAME = :channel_name, CHANNEL_WEBHOOK = :channel_webhook',
            ConditionExpression='COGNITO_USERNAME= :user_id AND CHANNEL_ID= :channel_id',
            ExpressionAttributeValues={
                ':user_id': {'S': user_id},
                ':channel_id': {'S': channel_id},
                ':channel_name': {'S': channel_name},
                ':channel_webhook': {'S': channel_webhook}
            },
            ReturnValues='ALL_NEW'
        )
        dynamo_res = dynamo_json.unmarshall(dynamo_res['Attributes'])
        dynamo_res = utils.dict_underscore_to_camelcase(dynamo_res)
        return 200, dynamo_res
    except DYNAMO_CLIENT.exceptions.ConditionalCheckFailedException:
        return 400, {'message': 'The channel does not exist.'}


def add_channel(user_id, channel_id, channel_name, channel_webhook):
    res = DYNAMO_CLIENT.put_item(
        TableName=TABLE_NAME,
        Item={
            'COGNITO_USERNAME': {
                'S': user_id
            },
            'CHANNEL_ID': {
                'S': channel_id
            },
            'CHANNEL_NAME': {
                'S': channel_name
            },
            'CHANNEL_WEBHOOK': {
                'S': channel_webhook
            }
        }
    )
    if res['ResponseMetadata']['HTTPStatusCode'] == 200:
        return True
    else:
        return False


def delete_channel(user_id, channel_id):
    try:
        res = DYNAMO_CLIENT.delete_item(
            TableName=TABLE_NAME,
            Key={
                'COGNITO_USERNAME': {'S': user_id},
                'CHANNEL_ID': {'S': channel_id}
            },
            ConditionExpression='attribute_exists(CHANNEL_ID)'
        )
        return res['ResponseMetadata']['HTTPStatusCode']
    except DYNAMO_CLIENT.exceptions.ConditionalCheckFailedException:
        return 404
