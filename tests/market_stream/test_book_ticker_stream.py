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


class TestBookTickerStream(unittest.TestCase):
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
                return b"8=FIX.4.4\x019=0000156\x0135=X\x0149=SPOT\x0156=BMDWATCH\x0134=3\x0152=20250224-10:12:53.515372\x01262=BOOK_TICKER_STREAM\x01268=1\x01279=1\x01269=1\x01270=641.67000000\x01271=2.72700000\x0155=BNBUSDT\x0125044=7495113\x0110=241\x01"
            else:
                return b"8=FIX.4.4\x019=84\x0135=5\x0134=4\x0149=SPOT\x0152=20250301-01:00:00.002000\x0156=GhQHzrLR\x0158=Logout acknowledgment.\x0110=212\x01"

    @patch("socket.socket")
    @patch("socket.create_connection")
    @patch("ssl.create_default_context")
    @patch("ssl.SSLContext")
    @patch.object(BinanceFixConnector, "current_utc_time")
    def test_ticker_stream(
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

        # Send a book_ticker request
        msg = client_md.create_fix_message_with_basic_header("V")
        msg.append_pair(262, "BOOK_TICKER_STREAM")  # md req id
        msg.append_pair(263, 1)  # Subscription type

        msg.append_pair(264, 1)  # market depth
        msg.append_pair(266, "Y")  # aggregated book
        msg.append_pair(146, 1)  # NoSymbols
        msg.append_pair(55, INSTRUMENT)  # Symbol
        msg.append_pair(267, 2)  # NoMDEntries
        msg.append_pair(269, 0)  # MDEntry
        msg.append_pair(269, 1)  # MDEntry
        client_md.send_message(msg)

        # Assert the actual request
        self.assertEqual(2, socket_mock.sendall.call_count)
        self.assertEqual(
            b"8=FIX.4.4\x019=134\x0135=V\x0149=BMDWATCH\x0156=SPOT\x0134=2\x0152=20250301-01:00:00.000000\x01262=BOOK_TICKER_STREAM\x01263=1\x01264=1\x01266=Y\x01146=1\x0155=BNBUSDT\x01267=2\x01269=0\x01269=1\x0110=180\x01",
            socket_mock.sendall.call_args[0][0],
        )

        # Start a thread to consume messages
        receive_thread = threading.Thread(
            target=client_md._BinanceFixConnector__receive_messages(), daemon=True
        )
        receive_thread.start()

        # Consume from the internal queue and assert
        for _ in range(client_md.queue_msg_received.qsize()):
            msg = client_md.queue_msg_received.get()
            if msg.message_type.decode("utf-8") == "X":
                subscription_id = (
                    None if not msg.get(262) else msg.get(262).decode("utf-8")
                )
                message_seq_num = (
                    None if not msg.get(34) else msg.get(34).decode("utf-8")
                )
                updates = 0 if not msg.get(268) else int(msg.get(268).decode("utf-8"))
                symbol = None if not msg.get(55) else msg.get(55).decode("utf-8")

                self.assertEqual("BOOK_TICKER_STREAM", subscription_id)
                self.assertEqual("3", message_seq_num)
                self.assertEqual("BNBUSDT", symbol)
                self.assertEqual(1, updates)
                for i in range(updates):
                    update_type = (
                        None
                        if not msg.get(269, i + 1)
                        else msg.get(269, i + 1).decode("utf-8")
                    )
                    price = (
                        None
                        if not msg.get(270, i + 1)
                        else msg.get(270, i + 1).decode("utf-8")
                    )
                    qty = (
                        None
                        if not msg.get(271, i + 1)
                        else msg.get(271, i + 1).decode("utf-8")
                    )
                    last_book_id = (
                        None
                        if not msg.get(25044, i + 1)
                        else msg.get(25044, i + 1).decode("utf-8")
                    )

                    self.assertEqual("1", update_type)
                    self.assertEqual("641.67000000", price)
                    self.assertEqual("2.72700000", qty)
                    self.assertEqual("7495113", last_book_id)

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
            message_type=["5"], timeout_seconds=1
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
