import logging
import os
import grpc
from google.protobuf.json_format import MessageToJson
from kaikosdk import sdk_pb2_grpc
from kaikosdk.stream.aggregates_ohlcv_v1 import request_pb2 as pb_ohlcv
from kaikosdk.core import instrument_criteria_pb2
from kaikosdk.stream.market_update_v1 import request_pb2 as pb_market_update
from kaikosdk.stream.market_update_v1 import commodity_pb2 as pb_commodity
import pandas as pd
import json
from kaikosdk.core import instrument_criteria_pb2, data_interval_pb2
from google.protobuf.timestamp_pb2 import Timestamp

api_key = os.environ.get('KAIKO_API_KEY')


def ohlcv_request(channel: grpc.Channel):
    try:
        with channel:
            stub = sdk_pb2_grpc.StreamAggregatesOHLCVServiceV1Stub(channel)
            responses = stub.Subscribe(pb_ohlcv.StreamAggregatesOHLCVRequestV1(
                aggregate='1s',
                instrument_criteria = instrument_criteria_pb2.InstrumentCriteria(
                    exchange = "cbse",
                    instrument_class = "spot",
                    code = "btc-usd"
                ),
                interval=data_interval_pb2.DataInterval(
                    start_time=Timestamp(seconds=1689204840),
                    end_time=Timestamp(seconds=1689204900))
            ))
            for response in responses:
                print("Received message %s" % (MessageToJson(response, including_default_value_fields = True)))
                # print("Received message %s" % list(map(lambda o: o.string_value, response.data.values)))
    except grpc.RpcError as e:
        print(e.details(), e.code())


def run():
    credentials = grpc.ssl_channel_credentials(root_certificates=None)
    call_credentials = grpc.access_token_call_credentials(api_key)
    composite_credentials = grpc.composite_channel_credentials(credentials, call_credentials)
    channel = grpc.secure_channel('gateway-v0-grpc.kaiko.ovh', composite_credentials)
    ohlcv_request(channel=channel)


if __name__ == '__main__':
    logging.basicConfig()
    run()

