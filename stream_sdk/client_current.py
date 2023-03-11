
"""
Use the Kaiko Stream API to subscribe to market updates - input exchange and pairs customization

Here the logic is to use concurrent processes (or threads) to subscribe to multiple exchanges and pairs, so that
the input parameter can be customized.
"""

from __future__ import print_function

import grpc
from google.protobuf.json_format import MessageToJson

from kaikosdk import sdk_pb2_grpc
from kaikosdk.core import instrument_criteria_pb2
from kaikosdk.stream.market_update_v1 import request_pb2 as pb_market_update
from kaikosdk.stream.market_update_v1 import commodity_pb2 as pb_commodity

from concurrent.futures import ProcessPoolExecutor
import os


def market_update_request(exchange, instrument_class, code):
    credentials = grpc.ssl_channel_credentials(root_certificates=None)
    call_credentials = grpc.access_token_call_credentials(os.environ['KAIKO_API_KEY'])
    composite_credentials = grpc.composite_channel_credentials(credentials, call_credentials)
    channel = grpc.secure_channel('gateway-v0-grpc.kaiko.ovh', composite_credentials)

    try:
        with channel:
            stub = sdk_pb2_grpc.StreamMarketUpdateServiceV1Stub(channel)
            responses = stub.Subscribe(pb_market_update.StreamMarketUpdateRequestV1(
                instrument_criteria=instrument_criteria_pb2.InstrumentCriteria(
                    exchange=exchange,
                    instrument_class=instrument_class,
                    code=code
                ),
                commodities=[pb_commodity.SMUC_TRADE]
            ))
            for response in responses:
                print("Received message %s" % (MessageToJson(response, including_default_value_fields=True)))
    except grpc.RpcError as e:
        print(e.details(), e.code())


def run():
    target = {
        'first': {
            'exchange': 'binc',
            'instrument_class': 'spot',
            'code': 'btc-usd,eth-btc'
        },
        'second': {
            'exchange': 'btmx',
            'instrument_class': 'spot',
            'code': 'btc-usdt'
        },
        'third': {
            'exchange': 'cbse',
            'instrument_class': 'spot',
            'code': 'btc-usd'
        }
    }
    exchange_lst = [i['exchange'] for i in target.values()]
    instrument_class_lst = [i['instrument_class'] for i in target.values()]
    code_lst = [i['code'] for i in target.values()]
    print(exchange_lst, instrument_class_lst, code_lst)

    with ProcessPoolExecutor(max_workers=len(target)) as pool:
        pool.map(market_update_request, exchange_lst, instrument_class_lst, code_lst)


if __name__ == '__main__':
    run()
