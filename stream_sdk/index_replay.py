from __future__ import print_function
import logging
import os
import datetime

import grpc
from google.protobuf.json_format import MessageToJson

from kaikosdk import sdk_pb2_grpc
from kaikosdk.stream.index_v1 import request_pb2 as pb_index
from google.protobuf.timestamp_pb2 import Timestamp

api_key = os.environ.get('KAIKO_API_KEY')


def index_request(channel: grpc.Channel, index_code, start_unix, end_unix):
    try:
        with channel:
            stub = sdk_pb2_grpc.StreamIndexServiceV1Stub(channel)
            responses = stub.Subscribe(pb_index.StreamIndexServiceRequestV1(
                index_code=index_code,
                interval={
                    'start_time': Timestamp(seconds=start_unix),
                    'end_time': Timestamp(seconds=end_unix)
                }
            ))

            for response in responses:
                print("Received message %s" % (MessageToJson(response, including_default_value_fields=True)))

    except grpc.RpcError as e:
        print(e.details(), e.code())


def run(start_time, end_time):
    credentials = grpc.ssl_channel_credentials(root_certificates=None)
    call_credentials = grpc.access_token_call_credentials(api_key)
    composite_credentials = grpc.composite_channel_credentials(credentials, call_credentials)
    # channel = grpc.secure_channel('gateway-v0-grpc.kaiko.ovh', composite_credentials)

    # use new lts gateway
    channel = grpc.secure_channel('stream-grpc-lts.kaiko.io', composite_credentials)

    index_request(channel=channel,
                  index_code='KK_RR_BTCUSD',
                  start_unix=start_time,
                  end_unix=end_time)


if __name__ == '__main__':
    logging.basicConfig()

    current_time = datetime.datetime.now()
    start_time = current_time - datetime.timedelta(minutes=500)
    end_time = current_time - datetime.timedelta(minutes=10)
    start_time = int(start_time.timestamp())
    end_time = int(end_time.timestamp())

    print(datetime.datetime.fromtimestamp(start_time), datetime.datetime.fromtimestamp(end_time))
    run(start_time=start_time, end_time=end_time)

