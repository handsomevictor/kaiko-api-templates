import os


def api_key_missing():
    raise Exception('KAIKO_API_KEY not found')


api_key = os.environ['KAIKO_API_KEY'] if 'KAIKO_API_KEY' in os.environ else api_key_missing()

