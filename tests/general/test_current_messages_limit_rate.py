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
FIX_URL = "tcp+tls://localhost:1234"
INSTRUMENT = "BNBUSDT"


class TestCurrentMessagesLimitRate(unittest.TestCase):
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
                return b"8=FIX.4.4\x019=166\x0135=XLR\x0149=SPOT\x0156=BMDWATCH\x0134=2\x0152=20250301-01:00:00.001000\x016136=current_message_rate\x0125003=2\x0125004=2\x0125005=1\x0125006=10000\x0125007=60\x0125008=s\x0110=212\x01"
            else:
                return b"8=FIX.4.4\x019=84\x0135=5\x0134=4\x0149=SPOT\x0152=20250301-01:00:00.002000\x0156=GhQHzrLR\x0158=Logout acknowledgment.\x0110=212\x01"

    @patch("socket.socket")
    @patch("socket.create_connection")
    @patch("ssl.create_default_context")
    @patch("ssl.SSLContext")
    @patch.object(BinanceFixConnector, "current_utc_time")
    def test_current_messages_limit_rate(
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
        client = create_market_data_session(
            api_key="API_KEY",
            private_key=get_private_key(PRIVATE_KEY),
            endpoint=FIX_URL,
            recv_window=100,
        )

        # Assert the logon request
        self.assertEqual(1, socket_mock.sendall.call_count)
        self.assertEqual(
            b"8=FIX.4.4\x019=204\x0135=A\x0149=BMDWATCH\x0156=SPOT\x0134=1\x0152=20250301-01:00:00.000000\x0125000=100\x0198=0\x01108=30\x0195=88\x0196=23rxWHmfUGML7o9O9lPkedNBj53wcilb9pFuIxc9Q1pGxHFQaBQnuPwj0ueH09t5Gzl3ybOc67wXGzh9Qcl/Aw==\x01141=Y\x01553=API_KEY\x0125035=2\x0110=219\x01",
            socket_mock.sendall.call_args[0][0],
        )

        # Create instrument list message
        msg = client.create_fix_message_with_basic_header("XLQ")
        msg.append_pair(6136, "current_message_rate")
        client.send_message(msg)

        # Assert the actual request
        self.assertEqual(2, socket_mock.sendall.call_count)
        self.assertEqual(
            b"8=FIX.4.4\x019=86\x0135=XLQ\x0149=BMDWATCH\x0156=SPOT\x0134=2\x0152=20250301-01:00:00.000000\x016136=current_message_rate\x0110=254\x01",
            socket_mock.sendall.call_args[0][0],
        )

        # Start a thread to consume messages
        receive_thread = threading.Thread(
            target=client._BinanceFixConnector__receive_messages(), daemon=True
        )
        receive_thread.start()

        for _ in range(client.queue_msg_received.qsize()):
            msg = client.queue_msg_received.get()
            if msg.message_type.decode("utf-8") == "y":
                req_id = (
                    None if not msg.get(6136) else int(msg.get(6136).decode("utf-8"))
                )
                limit_indicator = (
                    0 if not msg.get(25003) else int(msg.get(25003).decode("utf-8"))
                )

                self.assertEqual("current_message_rate", req_id)
                self.assertEqual("1", limit_indicator)
                for i in range(limit_indicator):
                    limit_type = (
                        None
                        if not msg.get(25004, i + 1)
                        else msg.get(25004, i + 1).decode("utf-8")
                    )
                    limit_count = (
                        None
                        if not msg.get(25005, i + 1)
                        else msg.get(25005, i + 1).decode("utf-8")
                    )
                    limit_max = (
                        None
                        if not msg.get(25006, i + 1)
                        else msg.get(25006, i + 1).decode("utf-8")
                    )
                    limit_reset_interval = (
                        None
                        if not msg.get(25007, i + 1)
                        else msg.get(25007, i + 1).decode("utf-8")
                    )
                    interval_res = (
                        None
                        if not msg.get(25008, i + 1)
                        else msg.get(25008, i + 1).decode("utf-8")
                    )

                    self.assertEqual("2", limit_type)
                    self.assertEqual("1", limit_count)
                    self.assertEqual("10000", limit_max)
                    self.assertEqual("60", limit_reset_interval)
                    self.assertEqual("s", interval_res)

        # Logout process
        client.logout()
        self.logOutSent = True

        receive_thread = threading.Thread(
            target=client._BinanceFixConnector__receive_messages(), daemon=True
        )
        receive_thread.start()

        # Assert the logout request
        self.assertEqual(3, socket_mock.sendall.call_count)
        self.assertEqual(
            b"8=FIX.4.4\x019=58\x0135=5\x0149=BMDWATCH\x0156=SPOT\x0134=3\x0152=20250301-01:00:00.000000\x0110=222\x01",
            socket_mock.sendall.call_args[0][0],
        )

        # Consume until logout message
        messages = client.retrieve_messages_until(message_type=["5"], timeout_seconds=1)

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
        client.disconnect()
