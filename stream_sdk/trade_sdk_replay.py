import logging
import os
import grpc
from google.protobuf.json_format import MessageToJson
from kaikosdk import sdk_pb2_grpc
from kaikosdk.core import instrument_criteria_pb2
from kaikosdk.stream.market_update_v1 import request_pb2 as pb_market_update
from kaikosdk.stream.market_update_v1 import commodity_pb2 as pb_commodity
import pandas as pd
import json
from kaikosdk.core import instrument_criteria_pb2, data_interval_pb2
from google.protobuf.timestamp_pb2 import Timestamp

api_key = os.environ.get('KAIKO_API_KEY')


def market_update_request(channel: grpc.Channel):
    try:
        with channel:
            stub = sdk_pb2_grpc.StreamMarketUpdateServiceV1Stub(channel)
            responses = stub.Subscribe(pb_market_update.StreamMarketUpdateRequestV1(
                instrument_criteria=instrument_criteria_pb2.InstrumentCriteria(
                    exchange="binc",
                    instrument_class="perpetual-future",
                    # code = "btcst-usdt,eth-btc,ltc-btc,bnb-btc,neo-btc,qtum-eth,eos-eth,snt-eth,bnt-eth,gas-btc,bnb-eth,wtc-btc,lrc-btc,lrc-eth,qtum-btc,omg-btc,omg-eth,zrx-btc,zrx-eth,knc-btc,knc-eth,fun-eth,snm-btc,neo-eth,iota-btc,iota-eth,link-btc,link-eth,xvg-eth"
                    code="btc-usdt"

                ),
                commodities=[
                    pb_commodity.SMUC_TRADE
                    # pb_commodity.SMUC_FULL_ORDER_BOOK
                    # SMUC_FULL_ORDER_BOOK,
                    # SMUC_FULL_ORDER_BOOK
                    # SMUC_TOP_OF_BOOK
                    # pb_commodity.SMUC_TRADE
                ]
                , interval=data_interval_pb2.DataInterval(
                    start_time=Timestamp(seconds=1692621089),
                    end_time=Timestamp(seconds=1692621189
)
                )
            ))
            if os.path.exists('market_update.txt'):
                os.remove('market_update.txt')
                print("market_update.txt Removed!")

            for response in responses:
                print("Received message %s" % (MessageToJson(response, including_default_value_fields=True)))
                temp = MessageToJson(response, including_default_value_fields=True)
                temp = json.loads(temp)

                temp = {k: temp[k] for k in ('tsExchange', 'price', 'tsCollection', 'tsEvent')}
                with open('market_update.txt', 'a') as f:
                    f.write(json.dumps(temp))
                    f.write('\n')
    except grpc.RpcError as e:
        print(e.details(), e.code())


def run():
    credentials = grpc.ssl_channel_credentials(root_certificates=None)
    call_credentials = grpc.access_token_call_credentials(api_key)
    composite_credentials = grpc.composite_channel_credentials(credentials, call_credentials)
    channel = grpc.secure_channel('gateway-v0-grpc.kaiko.ovh', composite_credentials)
    market_update_request(channel)


if __name__ == '__main__':
    logging.basicConfig()
    run()
