from chalice import Response
import gzip
import json

default_headers = {
    'Content-Type': 'application/json'
}

# Helper function to make Chanlice Response object.


def make_response(status_code, body, headers={}):
    response_headers = dict()
    response_headers.update(default_headers)
    response_headers.update(headers)
    return Response(
        status_code=status_code,
        headers=response_headers,
        body=body)

# The the cognito user ID from Lambda context.


def get_user_id(app):
    # return '7cabb7cc-a5a5-450a-96ea-f6af96a2977f'
    return app.current_request.context['authorizer']['claims']['cognito:username']


def underscore_to_camelcase(value):
    value = ''.join(x.capitalize() or '_' for x in value.split('_'))
    return value[0].lower() + value[1:]


def dict_underscore_to_camelcase(old_dict):
    new_dict = dict()
    for attr in old_dict:
        new_attr = underscore_to_camelcase(attr)
        new_dict[new_attr] = old_dict[attr]
    return new_dict


def request_params_check(json_body, required_params):
    request_params = tuple(json_body.keys())
    for param in required_params:
        if param not in request_params:
            return False, make_response(400, {
                "message": "Invalid request. Missing required parameters."
            })

    return True, {}
