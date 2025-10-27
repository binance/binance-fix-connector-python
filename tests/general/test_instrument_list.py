#!/usr/bin/env python3

import logging
import os
import threading
import unittest
from unittest.mock import patch

from binance_fix_connector.fix_connector import (
    BinanceFixConnector,
    create_market_data_session,
    FixMsgTypes,
)
from binance_fix_connector.utils import get_private_key

logging.basicConfig(level=logging.CRITICAL)

PRIVATE_KEY = os.path.join(os.path.dirname(__file__), "../unit_test_key.pem")
FIX_MD_URL = "tcp+tls://localhost:1234"
INSTRUMENT = "BNBUSDT"


class TestInstrumentList(unittest.TestCase):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.sentMessage = 0
        self.logOutSent = False

    def recv_side_effect(self, *args, **kwargs):
        # Trigger end of message after every message sent
        self.sentMessage += 1
        if self.sentMessage % 2 == 0:
            return b""
        else:
            if not self.logOutSent:
                return b"8=FIX.4.4\x019=227\x0135=y\x0149=SPOT\x0156=BMDWATCH\x0134=2\x0152=20250301-01:00:00.001000\x01320=GetInstrumentList\x01146=1\x0155=BNBUSDT\x0115=USDT\x012551=0.01000000\x012552=100000.00000000\x01969=0.01000000\x01562=0.00100000\x011140=900000.00000000\x0125039=0.00100000\x0125040=0.00000001\x0125041=6629.33313692\x0125042=0.00000001\x01969=0.01000000\x0110=110\x01"
            else:
                return b"8=FIX.4.4\x019=84\x0135=5\x0134=4\x0149=SPOT\x0152=20250301-01:00:00.002000\x0156=GhQHzrLR\x0158=Logout acknowledgment.\x0110=212\x01"

    @patch("socket.socket")
    @patch("socket.create_connection")
    @patch("ssl.create_default_context")
    @patch("ssl.SSLContext")
    @patch.object(BinanceFixConnector, "current_utc_time")
    def test_instrument_list(
        self,
        current_utc_time_mock,
        context_mock,
        create_default_context_mock,
        create_connection_mock,
        socket_mock,
    ):
        # Mock the socket instance
        current_utc_time_mock.return_value = "20250301-01:00:00.000000"

        # Simulate receiving message from the socket
        socket_mock.recv.side_effect = self.recv_side_effect
        context_mock.wrap_socket.return_value = socket_mock
        create_connection_mock.return_value = socket_mock
        create_default_context_mock.return_value = context_mock

        # Create FIX client
        client_md = create_market_data_session(
            api_key="API_KEY",
            private_key=get_private_key(PRIVATE_KEY),
            endpoint=FIX_MD_URL,
            recv_window=100,
        )

        # Assert the logon request
        self.assertEqual(1, socket_mock.sendall.call_count)
        self.assertEqual(
            b"8=FIX.4.4\x019=204\x0135=A\x0149=BMDWATCH\x0156=SPOT\x0134=1\x0152=20250301-01:00:00.000000\x0125000=100\x0198=0\x01108=30\x0195=88\x0196=23rxWHmfUGML7o9O9lPkedNBj53wcilb9pFuIxc9Q1pGxHFQaBQnuPwj0ueH09t5Gzl3ybOc67wXGzh9Qcl/Aw==\x01141=Y\x01553=API_KEY\x0125035=2\x0110=219\x01",
            socket_mock.sendall.call_args[0][0],
        )

        # Create instrument list message
        msg = client_md.create_fix_message_with_basic_header("x")
        msg.append_pair(320, "GetInstrumentList")
        msg.append_pair(559, 0)
        msg.append_pair(55, "BNBUSDT")
        client_md.send_message(msg)

        # Assert the actual request
        self.assertEqual(2, socket_mock.sendall.call_count)
        self.assertEqual(
            b"8=FIX.4.4\x019=97\x0135=x\x0149=BMDWATCH\x0156=SPOT\x0134=2\x0152=20250301-01:00:00.000000\x01320=GetInstrumentList\x01559=0\x0155=BNBUSDT\x0110=182\x01",
            socket_mock.sendall.call_args[0][0],
        )

        # Start a thread to consume messages
        receive_thread = threading.Thread(
            target=client_md._BinanceFixConnector__receive_messages(), daemon=True
        )
        receive_thread.start()

        for _ in range(client_md.queue_msg_received.qsize()):
            msg = client_md.queue_msg_received.get()
            if msg.message_type.decode("utf-8") == "y":
                instrument_req_id = (
                    None if not msg.get(320) else msg.get(320).decode("utf-8")
                )
                message_seq_num = (
                    None if not msg.get(34) else msg.get(34).decode("utf-8")
                )
                num_symbols = (
                    0 if not msg.get(146) else int(msg.get(146).decode("utf-8"))
                )

                self.assertEqual("GetInstrumentList", instrument_req_id)
                self.assertEqual("2", message_seq_num)
                self.assertEqual(1, num_symbols)
                for i in range(num_symbols):
                    symbol = (
                        None
                        if not msg.get(55, i + 1)
                        else msg.get(55, i + 1).decode("utf-8")
                    )
                    currency = (
                        None
                        if not msg.get(15, i + 1)
                        else msg.get(15, i + 1).decode("utf-8")
                    )
                    min_price = (
                        None
                        if not msg.get(2551, i + 1)
                        else msg.get(2551, i + 1).decode("utf-8")
                    )
                    max_price = (
                        None
                        if not msg.get(2552, i + 1)
                        else msg.get(2552, i + 1).decode("utf-8")
                    )
                    min_price_inc = (
                        None
                        if not msg.get(969, i + 1)
                        else msg.get(969, i + 1).decode("utf-8")
                    )
                    min_trade_vol = (
                        None
                        if not msg.get(562, i + 1)
                        else msg.get(562, i + 1).decode("utf-8")
                    )
                    max_trade_vol = (
                        None
                        if not msg.get(1140, i + 1)
                        else msg.get(1140, i + 1).decode("utf-8")
                    )
                    min_qty = (
                        None
                        if not msg.get(25039, i + 1)
                        else msg.get(25039, i + 1).decode("utf-8")
                    )
                    min_price_inc = (
                        None
                        if not msg.get(969, i + 1)
                        else msg.get(969, i + 1).decode("utf-8")
                    )
                    market_min_trade_vol = (
                        None
                        if not msg.get(25040, i + 1)
                        else msg.get(25040, i + 1).decode("utf-8")
                    )
                    market_max_trade_vol = (
                        None
                        if not msg.get(25041, i + 1)
                        else msg.get(25041, i + 1).decode("utf-8")
                    )
                    market_min_qty = (
                        None
                        if not msg.get(25042, i + 1)
                        else msg.get(25042, i + 1).decode("utf-8")
                    )

                    self.assertEqual("BNBUSDT", symbol)
                    self.assertEqual("USDT", currency)
                    self.assertEqual("0.01000000", min_price)
                    self.assertEqual("100000.00000000", max_price)
                    self.assertEqual("0.01000000", min_price_inc)
                    self.assertEqual("0.00100000", min_trade_vol)
                    self.assertEqual("900000.00000000", max_trade_vol)
                    self.assertEqual("0.00100000", min_qty)
                    self.assertEqual("0.01000000", min_price_inc)
                    self.assertEqual("0.00000001", market_min_trade_vol)
                    self.assertEqual("6629.33313692", market_max_trade_vol)
                    self.assertEqual("0.00000001", market_min_qty)

        # Logout process
        client_md.logout()
        self.logOutSent = True

        receive_thread = threading.Thread(
            target=client_md._BinanceFixConnector__receive_messages(), daemon=True
        )
        receive_thread.start()

        # Assert the logout request
        self.assertEqual(3, socket_mock.sendall.call_count)
        self.assertEqual(
            b"8=FIX.4.4\x019=58\x0135=5\x0149=BMDWATCH\x0156=SPOT\x0134=3\x0152=20250301-01:00:00.000000\x0110=222\x01",
            socket_mock.sendall.call_args[0][0],
        )

        # Consume until logout message
        messages = client_md.retrieve_messages_until(
            message_type="5", timeout_seconds=1
        )

        # Logout acknowledgment
        log_out_message = messages[-1]

        # Assert logout message
        msg_type = (
            None
            if not log_out_message.get(35)
            else log_out_message.get(35).decode("utf-8")
        )
        msg_text = (
            None
            if not log_out_message.get(58)
            else log_out_message.get(58).decode("utf-8")
        )
        self.assertEqual(FixMsgTypes.LOGOUT, msg_type)
        self.assertEqual("Logout acknowledgment.", msg_text)

        # Close the connection
        client_md.disconnect()
