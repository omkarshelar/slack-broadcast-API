from chalice import Chalice, Response, CognitoUserPoolAuthorizer, CORSConfig
import logging
import boto3
import uuid
import dynamo_json
from chalicelib import broadcaster, utils, dynamodb_utils

app = Chalice(app_name='slack-broadcast-apis')
app.log.setLevel(logging.ERROR)
# app.log.setLevel(logging.DEBUG) # Enable this if every response has to be logged in Cloudwatch.
# app.debug = True # Enable this only for developement.


# Configuration to enable CORS for API gateway.
# Note : This will automatically add OPTIONS request handler in API gateway.
cors_config = CORSConfig(
    allow_origin='*',
    allow_headers=['Content-Type', 'Authorization',
                   'X-Amz-Date', 'X-Api-Key', 'X-Amz-Security-Token'],
    max_age=600,
    allow_credentials=True
)

authorizer = CognitoUserPoolAuthorizer(
    'MyPool', header='Authorization',
    provider_arns=['arn:aws:cognito-idp:ap-south-1:190911038733:userpool/ap-south-1_yIyVbDlc9'])


@app.route('/channels', methods=['GET'], authorizer=authorizer, cors=cors_config)
def getChannels():
    user_id = utils.get_user_id(app)
    channels = dynamodb_utils.get_channels(user_id)
    response = []
    for channel in channels:
        response.append(utils.dict_underscore_to_camelcase(
            dynamo_json.unmarshall(channel)))
    if len(response) > 0:
        return utils.make_response(200, {
            "channels": response
        })
    else:
        return utils.make_response(404, {
            "message": "No channels found for the you. Please add a channel."
        })


@app.route('/channels', methods=["POST"], authorizer=authorizer, cors=cors_config)
def new_channel():
    user_id = utils.get_user_id(app)
    check_status, check_response = utils.request_params_check(
        app.current_request.json_body, ('channelName', 'channelWebhook'))
    if not check_status:
        return check_response
    channel_name = app.current_request.json_body['channelName']
    channel_webhook = app.current_request.json_body['channelWebhook']
    channel_id = str(uuid.uuid4())
    res = dynamodb_utils.get_channels(user_id, channel_name=channel_name)
    if(len(res) > 0):
        return utils.make_response(409, {
            "message": "Channel with the same name already exists! Please give unique channel name to help differentiate channels."
        })

    status = dynamodb_utils.add_channel(
        user_id, channel_id, channel_name, channel_webhook)
    if status:
        return utils.make_response(201, {
            "message": "New slack channel added.",
            "data": {
                'cognitoUsername': user_id,
                'channelId': channel_id,
                'channelName': channel_name,
                'channelWebhook': channel_webhook
            }
        })

    else:
        return utils.make_response(500, {
            "message": "An error occurred while adding channel. Please try again in some time."
        })


@app.route('/channels', methods=["PUT"], authorizer=authorizer, cors=cors_config)
def edit_channel():
    user_id = utils.get_user_id(app)
    check_status, check_response = utils.request_params_check(
        app.current_request.json_body, ('channelName', 'channelWebhook', 'channelId'))
    if not check_status:
        return check_response
    channel_name = app.current_request.json_body['channelName']
    channel_webhook = app.current_request.json_body['channelWebhook']
    channel_id = app.current_request.json_body['channelId']

    channel = dynamodb_utils.get_channels(user_id, channel_id=channel_id)

    if(len(channel) == 0):
        return utils.make_response(400, {
            "message": "Channel NOT found!"
        })

    filtered_channel = dynamodb_utils.get_channels(
        user_id, channel_name=channel_name)

    if(len(filtered_channel) == 1) and dynamo_json.unmarshall(filtered_channel[0])['CHANNEL_ID'] != channel_id:
        return utils.make_response(409, {
            "message": "Channel with the same name already exists! Please give unique channel name to help differentiate channels."
        })

    response_code, res = dynamodb_utils.update_channel(
        user_id, channel_id, channel_name, channel_webhook)
    if(response_code == 200):
        return utils.make_response(response_code, res)
    else:
        return utils.make_response(response_code, res)


@app.route('/channels', methods=["DELETE"], authorizer=authorizer, cors=cors_config)
def delete_channel():
    user_id = utils.get_user_id(app)
    check_status, check_response = utils.request_params_check(
        app.current_request.json_body, ('channelId',))
    if not check_status:
        return check_response
    channel_id = app.current_request.json_body['channelId']
    res = dynamodb_utils.delete_channel(user_id, channel_id)
    if res == 404:
        return utils.make_response(res, {
            "message": "The requested channel NOT found."
        })
    elif res == 200:
        return utils.make_response(res, {
            "message": "The requested channel has been deleted."
        })
    else:
        return utils.make_response(500, {
            "message": "Oops! Some error occured!"
        })


@app.route('/broadcast', methods=['POST'], authorizer=authorizer, cors=cors_config)
def broadcast_message():
    user_id = utils.get_user_id(app)
    check_status, check_response = utils.request_params_check(
        app.current_request.json_body, ('channels', 'message'))
    if not check_status:
        return check_response
    channels = app.current_request.json_body['channels']
    message = app.current_request.json_body['message']
    if len(channels) <= 0:
        return utils.make_response(400, {
            "message": "No channels received!"
        })
    all_channels = dynamodb_utils.get_channels(user_id)
    all_channels = list(map(dynamo_json.unmarshall, all_channels))
    broadcast_channels = [
        channel for channel in all_channels if channel['CHANNEL_ID'] in channels]

    if len(broadcast_channels) < len(channels):
        return utils.make_response(400, {
            "message": "Some of the channels not found."
        })

    slack_responses = broadcaster.broadcast_message(
        broadcast_channels, message)

    status = list()
    for response in slack_responses:
        status.append(response['success'])

    if True in status and False in status:
        return utils.make_response(207, {
            "slackResponses": slack_responses
        })
    elif False not in status:
        return utils.make_response(200, {
            "slackResponses": slack_responses
        })
    elif True not in status:
        return utils.make_response(400, {
            "message": "None of the messages were sent. Please retry or edit channel webhook to valid URI.",
            "slackResponses": slack_responses
        })
