import requests
import threading
import json
import time

# This function exeutes in a thread. The caller being broadcast_message()


def send_message(channel, message, response_ref):
    message = json.dumps(message)
    webhook_url = channel['CHANNEL_WEBHOOK']
    try:
        response = requests.post(webhook_url, data=message, headers={
            "Content-Type": "application/json"
        })
        if(response.status_code != 200 or response.text != 'ok'):
            time.sleep(2)
            response = requests.post(webhook_url, data=message, headers={
                "Content-Type": "application/json"
            })

        if(response.status_code == 200 and response.text == 'ok'):
            response_ref.append({
                'success': True,
                'channelId': channel['CHANNEL_ID'],
                'channelName': channel['CHANNEL_NAME'],
                'text': response.text,
                'statusCode': response.status_code
            })
        else:
            response_ref.append({
                'success': False,
                'channelId': channel['CHANNEL_ID'],
                'channelName': channel['CHANNEL_NAME'],
                'text': response.text,
                'statusCode': response.status_code
            })
    except:
        response_ref.append({
            'success': False,
            'channelId': channel['CHANNEL_ID'],
            'channelName': channel['CHANNEL_NAME'],
        })


# Broadcast message to slack webhooks.
# This function calls send_message() in multiple threads to speed up the process.
def broadcast_message(channels, message):
    request_threads = list()
    response_status = list()
    for channel in channels:
        t = threading.Thread(target=send_message, args=(
            channel, message, response_status))
        request_threads.append(t)

    for thread in request_threads:
        thread.start()

    for thread in request_threads:
        thread.join()

    return response_status
