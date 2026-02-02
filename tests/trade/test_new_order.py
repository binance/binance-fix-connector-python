#!/usr/bin/env python3

import logging
import os
import threading
import unittest
from unittest.mock import patch

from binance_fix_connector.fix_connector import (
    BinanceFixConnector,
    create_order_entry_session,
    FixMsgTypes,
)
from binance_fix_connector.utils import get_private_key

logging.basicConfig(level=logging.CRITICAL)

PRIVATE_KEY = os.path.join(os.path.dirname(__file__), "../unit_test_key.pem")
FIX_OE_URL = "tcp+tls://localhost:1234"
INSTRUMENT = "BNBUSDT"


class TestNewOrder(unittest.TestCase):
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
                return b"8=FIX.4.4\x019=342\x0135=8\x0149=SPOT\x0156=BOETRADE\x0134=2\x0152=20250204-09:10:09.754155\x0117=10062143\x0111=1738660209676610000\x0137=4709412\x0138=1.00000000\x0140=2\x0154=2\x0155=BNBUSDT\x0144=730.00000000\x0159=1\x0160=20250204-09:10:09.753771\x0125018=20250204-09:10:09.753771\x0125001=3\x01150=0\x0114=0.00000000\x01151=1.00000000\x0125017=0.00000000\x011057=Y\x0132=0.00000000\x0139=0\x01636=Y\x0125023=20250204-09:10:09.753771\x0110=234\x01"
            else:
                return b"8=FIX.4.4\x019=84\x0135=5\x0134=4\x0149=SPOT\x0152=20250301-01:00:00.002000\x0156=GhQHzrLR\x0158=Logout acknowledgment.\x0110=212\x01"

    @patch("socket.socket")
    @patch("socket.create_connection")
    @patch("ssl.create_default_context")
    @patch("ssl.SSLContext")
    @patch.object(BinanceFixConnector, "current_utc_time")
    def test_new_order(
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
        client_oe = create_order_entry_session(
            api_key="API_KEY",
            private_key=get_private_key(PRIVATE_KEY),
            endpoint=FIX_OE_URL,
        )

        # Create instrument list message
        msg = client_oe.create_fix_message_with_basic_header("D")
        msg.append_pair(38, 1)
        msg.append_pair(40, 2)
        msg.append_pair(11, "1740758400000001000")
        msg.append_pair(44, 730)
        msg.append_pair(54, 2)
        msg.append_pair(55, "BNBUSDT")
        msg.append_pair(59, 1)
        client_oe.send_message(msg)

        # Assert the actual request
        self.assertEqual(2, socket_mock.sendall.call_count)
        self.assertEqual(
            b"8=FIX.4.4\x019=119\x0135=D\x0149=BOETRADE\x0156=SPOT\x0134=2\x0152=20250301-01:00:00.000000\x0138=1\x0140=2\x0111=1740758400000001000\x0144=730\x0154=2\x0155=BNBUSDT\x0159=1\x0110=201\x01",
            socket_mock.sendall.call_args[0][0],
        )

        # Start a thread to consume messages
        receive_thread = threading.Thread(
            target=client_oe._BinanceFixConnector__receive_messages(), daemon=True
        )
        receive_thread.start()

        # Logout process
        client_oe.logout()
        self.logOutSent = True

        receive_thread = threading.Thread(
            target=client_oe._BinanceFixConnector__receive_messages(), daemon=True
        )
        receive_thread.start()

        for _ in range(client_oe.queue_msg_received.qsize()):
            msg = client_oe.queue_msg_received.get()
            if msg.message_type.decode("utf-8") == "8":
                cl_ord_id = None if not msg.get(11) else msg.get(11).decode("utf-8")
                order_qty = None if not msg.get(38) else msg.get(38).decode("utf-8")
                ord_type = None if not msg.get(40) else msg.get(40).decode("utf-8")
                side = None if not msg.get(54) else msg.get(54).decode("utf-8")
                symbol = None if not msg.get(55) else msg.get(55).decode("utf-8")
                price = None if not msg.get(44) else msg.get(44).decode("utf-8")
                time_in_force = None if not msg.get(59) else msg.get(59).decode("utf-8")
                cum_qty = None if not msg.get(14) else msg.get(14).decode("utf-8")
                last_qty = None if not msg.get(32) else msg.get(32).decode("utf-8")
                ord_status = None if not msg.get(39) else msg.get(39).decode("utf-8")
                ord_rej_reason = (
                    None if not msg.get(103) else msg.get(103).decode("utf-8")
                )
                error_code = (
                    None if not msg.get(25016) else msg.get(25016).decode("utf-8")
                )
                text = None if not msg.get(58) else msg.get(58).decode("utf-8")

                self.assertEqual("BNBUSDT", symbol)
                self.assertEqual("1738660209676610000", cl_ord_id)
                self.assertEqual("1.00000000", order_qty)
                self.assertEqual("2", ord_type)
                self.assertEqual("2", side)
                self.assertEqual("730.00000000", price)
                self.assertEqual("1", time_in_force)
                self.assertEqual("0.00000000", cum_qty)
                self.assertEqual("0.00000000", last_qty)
                self.assertEqual("0", ord_status)
                self.assertEqual(None, ord_rej_reason)
                self.assertEqual(None, error_code)
                self.assertEqual(None, text)

        # Logout process
        client_oe.logout()
        self.logOutSent = True

        receive_thread = threading.Thread(
            target=client_oe._BinanceFixConnector__receive_messages(), daemon=True
        )
        receive_thread.start()

        # Assert the logout request
        self.assertEqual(4, socket_mock.sendall.call_count)
        self.assertEqual(
            b"8=FIX.4.4\x019=58\x0135=5\x0149=BOETRADE\x0156=SPOT\x0134=4\x0152=20250301-01:00:00.000000\x0110=219\x01",
            socket_mock.sendall.call_args[0][0],
        )

        # Consume until logout message
        messages = client_oe.retrieve_messages_until(
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
        client_oe.disconnect()


if __name__ == "__main__":
    unittest.main()
