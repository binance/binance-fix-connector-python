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
ORD_STATUS = {
    "0": "NEW",
    "1": "PARTIALLY_FILLED",
    "2": "FILLED",
    "4": "CANCELED",
    "6": "PENDING_CANCEL",
    "8": "REJECTED",
    "A": "PENDING_NEW",
    "C": "EXPIRED",
}
ORD_TYPES = {"1": "MARKET", "2": "LIMIT", "3": "STOP", "4": "STOP_LIMIT", "P": "PEGGED"}
SIDES = {"1": "BUY", "2": "SELL"}
TIME_IN_FORCE = {
    "1": "GOOD_TILL_CANCEL",
    "3": "IMMEDIATE_OR_CANCEL",
    "4": "FILL_OR_KILL",
}
ORD_REJECT_REASON = {"99": "OTHER"}
ORD_EXEC_TYPE = {
    "0": "NEW",
    "4": "CANCELED",
    "5": "REPLACED",
    "8": "REJECTED",
    "F": "TRADE",
    "C": "EXPIRED",
}
SELF_TRADE_PREVENTION_MODE = {
    "1": "NONE",
    "2": "EXPIRE_TAKER",
    "3": "EXPIRE_MAKER",
    "4": "EXPIRE_BOTH",
}

LIST_STATUS = {"2": "RESPONSE", "4": "EXEC_STARTED", "5": "ALL_DONE"}
LIST_ORD_STATUS = {"3": "EXECUTING", "6": "ALL_DONE", "7": "REJECT"}
LIST_ORD_TYPE = {"1": "ONE_CANCELS_THE_OTHER", "2": "ONE_TRIGGERS_THE_OTHER"}
LIST_TRIG_TYPE = {"ACTIVATED": "1", "PARTIALLY_FILLED": "2", "FILLED": "3"}
LIST_TRIG_ACTION = {"RELEASE": "1", "CANCEL": "2"}
INSTRUMENT = "BNBUSDT"


class TestInstrumentList(unittest.TestCase):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.receivedMessage = 0
        self.logOutSent = False

    def recv_side_effect(self, *args, **kwargs):
        # Trigger end of message after every message sent
        self.receivedMessage += 1
        if self.receivedMessage % 4 == 0:
            return b""
        else:
            if not self.logOutSent:
                if self.receivedMessage == 1:
                    return b"8=FIX.4.4\x019=306\x0135=N\x0149=SPOT\x0156=BOETRADE\x0134=2\x0152=20250301-01:00:00.000001\x0155=BNBUSDT\x0166=16889\x01429=4\x01431=3\x0125014=1740758400000001000\x0125015=1740758400000001000\x0160=20250301-01:00:00.000001\x011385=2\x0173=2\x0111=w1740758400000001000\x0155=BNBUSDT\x0137=5279178\x0111=p1740758400000001000\x0155=BNBUSDT\x0137=5279179\x0125010=1\x0125011=3\x0125012=0\x0125013=1\x0110=194\x01"
                if self.receivedMessage == 2:
                    return b"8=FIX.4.4\x019=352\x0135=8\x0149=SPOT\x0156=BOETRADE\x0134=3\x0152=20250301-01:00:00.000001\x0117=11493629\x0111=w1740758400000001000\x0137=5279178\x0138=1.00000000\x0140=2\x0154=1\x0155=BNBUSDT\x0144=730.00000000\x0159=1\x0160=20250301-01:00:00.000001\x0125018=20250301-01:00:00.000001\x0166=16889\x0125001=3\x01150=0\x0114=0.00000000\x01151=1.00000000\x0125017=0.00000000\x011057=Y\x0132=0.00000000\x0139=0\x01636=Y\x0125023=20250329-08:00:00.000001\x0110=079\x01"
                elif self.receivedMessage == 3:
                    return b"8=FIX.4.4\x019=315\x0135=8\x0149=SPOT\x0156=BOETRADE\x0134=4\x0152=20250301-01:00:00.000001\x0117=11493630\x0111=p1740758400000001000\x0137=5279179\x0138=1.00000000\x0140=2\x0154=2\x0155=BNBUSDT\x0144=735.00000000\x0159=1\x0160=20250301-01:00:00.000001\x0125018=20250301-01:00:00.000001\x0166=16889\x0125001=3\x01150=0\x0114=0.00000000\x01151=1.00000000\x0125017=0.00000000\x011057=Y\x0132=0.00000000\x0139=A\x0110=024\x01"
                elif self.receivedMessage == 5:
                    return b"8=FIX.4.4\x019=376\x0135=8\x0149=SPOT\x0156=BOETRADE\x0134=5\x0152=20250301-01:00:00.000001\x0117=11493631\x0111=w1740758400000001000\x0137=5279178\x0138=1.00000000\x0140=2\x0154=1\x0155=BNBUSDT\x0144=730.00000000\x0159=1\x0160=20250301-01:00:00.000001\x0125018=20250301-01:00:00.000001\x0166=16889\x0125001=3\x01150=F\x0114=1.00000000\x01151=0.00000000\x0125017=576.12000000\x011057=Y\x011003=956281\x0131=576.12000000\x0132=1.00000000\x0139=2\x0125023=20250329-08:00:00.000001\x0110=224\x01"
                else:
                    return b"8=FIX.4.4\x019=352\x0135=8\x0149=SPOT\x0156=BOETRADE\x0134=6\x0152=20250301-01:00:00.000001\x0117=11493633\x0111=p1740758400000001000\x0137=5279179\x0138=1.00000000\x0140=2\x0154=2\x0155=BNBUSDT\x0144=735.00000000\x0159=1\x0160=20250301-01:00:00.000001\x0125018=20250301-01:00:00.000001\x0166=16889\x0125001=3\x01150=0\x0114=0.00000000\x01151=1.00000000\x0125017=0.00000000\x011057=Y\x0132=0.00000000\x0139=0\x01636=Y\x0125023=20250329-08:00:00.000001\x0110=072\x01"
            else:
                return b"8=FIX.4.4\x019=84\x0135=5\x0149=SPOT\x0156=BOETRADE\x0134=5\x0152=20250301-01:00:00.000005\x0158=Logout acknowledgment.\x0110=088\x01"

    @patch("socket.socket")
    @patch("socket.create_connection")
    @patch("ssl.create_default_context")
    @patch("ssl.SSLContext")
    @patch.object(BinanceFixConnector, "current_utc_time")
    def test_list_OTO_order(
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

        # Assert the logon request
        self.assertEqual(1, socket_mock.sendall.call_count)
        self.assertEqual(
            b"8=FIX.4.4\x019=209\x0135=A\x0149=BOETRADE\x0156=SPOT\x0134=1\x0152=20250301-01:00:00.000000\x0198=0\x01108=30\x0195=88\x0196=eI7NNqsvWINcco9+rUQFjB4O1bsZBHp5uYaeq/V9d736Omn//d0ZQR5uW/91Ylf4lTiO0QpN1StIWZBjBuI5AA==\x01141=Y\x01553=API_KEY\x0125035=2\x0125036=1\x019406=N\x0110=056\x01",
            socket_mock.sendall.call_args[0][0],
        )

        # Create OTO order message
        msg = client_oe.create_fix_message_with_basic_header("E")
        identifier = "1740758400000001000"
        working_leg_id = f"w{identifier}"
        pending_leg_id = f"p{identifier}"

        msg.append_pair(73, 2)
        msg.append_pair(11, working_leg_id)
        msg.append_pair(55, INSTRUMENT)
        msg.append_pair(54, 1)
        msg.append_pair(38, 1)
        msg.append_pair(40, 2)
        msg.append_pair(44, 730)
        msg.append_pair(59, 1)
        msg.append_pair(11, pending_leg_id)
        msg.append_pair(55, INSTRUMENT)
        msg.append_pair(54, 2)
        msg.append_pair(38, 1)
        msg.append_pair(40, 2)
        msg.append_pair(44, 735)
        msg.append_pair(59, 1)
        msg.append_pair(25010, 1)
        msg.append_pair(25011, 3)
        msg.append_pair(25012, 0)
        msg.append_pair(25013, 1)
        msg.append_pair(1385, 2)
        msg.append_pair(25014, f"{identifier}")
        client_oe.send_message(msg)

        # Assert the actual request
        self.assertEqual(2, socket_mock.sendall.call_count)
        self.assertEqual(
            b"8=FIX.4.4\x019=252\x0135=E\x0149=BOETRADE\x0156=SPOT\x0134=2\x0152=20250301-01:00:00.000000\x0173=2\x0111=w1740758400000001000\x0155=BNBUSDT\x0154=1\x0138=1\x0140=2\x0144=730\x0159=1\x0111=p1740758400000001000\x0155=BNBUSDT\x0154=2\x0138=1\x0140=2\x0144=735\x0159=1\x0125010=1\x0125011=3\x0125012=0\x0125013=1\x011385=2\x0125014=1740758400000001000\x0110=019\x01",
            socket_mock.sendall.call_args[0][0],
        )

        # Start a thread to consume messages
        receive_thread = threading.Thread(
            target=client_oe._BinanceFixConnector__receive_messages(), daemon=True
        )
        receive_thread.start()
        for _ in range(client_oe.queue_msg_received.qsize()):
            msg = client_oe.queue_msg_received.get()
            msg_type = None if not msg.get(35) else msg.get(35).decode("utf-8")
            if msg_type == "N":
                symbol = None if not msg.get(55) else msg.get(55).decode("utf-8")
                list_status_type = (
                    None if not msg.get(429) else msg.get(429).decode("utf-8")
                )
                list_ord_status = (
                    None if not msg.get(431) else msg.get(431).decode("utf-8")
                )
                cl_list_id = (
                    None if not msg.get(25014) else msg.get(25014).decode("utf-8")
                )
                contingency = (
                    None if not msg.get(1385) else msg.get(1385).decode("utf-8")
                )

                self.assertEqual("BNBUSDT", symbol)
                self.assertEqual(
                    "EXEC_STARTED", LIST_STATUS.get(list_status_type, list_status_type)
                )
                self.assertEqual(
                    "EXECUTING", LIST_ORD_STATUS.get(list_ord_status, list_ord_status)
                )
                self.assertEqual(f"{identifier}", cl_list_id)
                self.assertEqual(
                    "ONE_TRIGGERS_THE_OTHER",
                    LIST_ORD_TYPE.get(contingency, contingency),
                )

                orders = 0 if not msg.get(73) else int(msg.get(73).decode("utf-8"))
                for i in range(orders):
                    cl_ord_id = (
                        None
                        if not msg.get(11, i + 1)
                        else msg.get(11, i + 1).decode("utf-8")
                    )
                    symbol = (
                        None
                        if not msg.get(55, i + 1)
                        else msg.get(55, i + 1).decode("utf-8")
                    )
                    ord_rej_reason = (
                        None
                        if not msg.get(103, i + 1)
                        else msg.get(103, i + 1).decode("utf-8")
                    )
                    error_code = (
                        None
                        if not msg.get(25016, i + 1)
                        else msg.get(25016, i + 1).decode("utf-8")
                    )
                    text = (
                        None
                        if not msg.get(58, i + 1)
                        else msg.get(58, i + 1).decode("utf-8")
                    )

                    self.assertEqual(
                        f"w{identifier}" if i == 0 else f"p{identifier}", cl_ord_id
                    )
                    self.assertEqual("BNBUSDT", symbol)
                    self.assertEqual(None, ord_rej_reason)
                    self.assertEqual(None, error_code)
                    self.assertEqual(None, text)
            else:
                msg_seq_num = (
                    None if not msg.get(34) else int(msg.get(34).decode("utf-8"))
                )
                if msg_seq_num == 3:
                    exec_id = None if not msg.get(17) else msg.get(17).decode("utf-8")
                    cl_ord_id = None if not msg.get(11) else msg.get(11).decode("utf-8")
                    order_id = None if not msg.get(37) else msg.get(37).decode("utf-8")
                    order_qty = None if not msg.get(38) else msg.get(38).decode("utf-8")
                    ord_type = None if not msg.get(40) else msg.get(40).decode("utf-8")
                    side = None if not msg.get(54) else msg.get(54).decode("utf-8")
                    symbol = None if not msg.get(55) else msg.get(55).decode("utf-8")
                    price = None if not msg.get(44) else msg.get(44).decode("utf-8")
                    time_in_force = (
                        None if not msg.get(59) else msg.get(59).decode("utf-8")
                    )
                    transact_time = (
                        None if not msg.get(60) else msg.get(60).decode("utf-8")
                    )
                    order_creation_time = (
                        None if not msg.get(25018) else msg.get(25018).decode("utf-8")
                    )
                    list_id = None if not msg.get(66) else msg.get(66).decode("utf-8")
                    self_trade_prevention_mode = (
                        None if not msg.get(25001) else msg.get(25001).decode("utf-8")
                    )
                    exec_type = (
                        None if not msg.get(150) else msg.get(150).decode("utf-8")
                    )
                    cum_qty = None if not msg.get(14) else msg.get(14).decode("utf-8")
                    leaves_qty = (
                        None if not msg.get(151) else msg.get(151).decode("utf-8")
                    )
                    cum_quote_qty = (
                        None if not msg.get(25017) else msg.get(25017).decode("utf-8")
                    )
                    aggressor_indicator = (
                        None if not msg.get(1057) else msg.get(1057).decode("utf-8")
                    )
                    last_qty = None if not msg.get(32) else msg.get(32).decode("utf-8")
                    ord_status = (
                        None if not msg.get(39) else msg.get(39).decode("utf-8")
                    )

                    self.assertEqual("11493629", exec_id)
                    self.assertEqual("w1740758400000001000", cl_ord_id)
                    self.assertEqual("5279178", order_id)
                    self.assertEqual("1.00000000", order_qty)
                    self.assertEqual("LIMIT", ORD_TYPES.get(ord_type, ord_type))
                    self.assertEqual("BUY", SIDES.get(side, side))
                    self.assertEqual("BNBUSDT", symbol)
                    self.assertEqual("730.00000000", price)
                    self.assertEqual(
                        "GOOD_TILL_CANCEL",
                        TIME_IN_FORCE.get(time_in_force, time_in_force),
                    )
                    self.assertEqual("20250301-01:00:00.000001", transact_time)
                    self.assertEqual("20250301-01:00:00.000001", order_creation_time)
                    self.assertEqual("16889", list_id)
                    self.assertEqual(
                        "EXPIRE_MAKER",
                        SELF_TRADE_PREVENTION_MODE.get(self_trade_prevention_mode),
                    )
                    self.assertEqual("NEW", ORD_EXEC_TYPE.get(exec_type))
                    self.assertEqual("0.00000000", cum_qty)
                    self.assertEqual("1.00000000", leaves_qty)
                    self.assertEqual("0.00000000", cum_quote_qty)
                    self.assertEqual("Y", aggressor_indicator)
                    self.assertEqual("0.00000000", last_qty)
                    self.assertEqual("NEW", ORD_STATUS.get(ord_status))
                elif msg_seq_num == 4:
                    exec_id = None if not msg.get(17) else msg.get(17).decode("utf-8")
                    cl_ord_id = None if not msg.get(11) else msg.get(11).decode("utf-8")
                    order_id = None if not msg.get(37) else msg.get(37).decode("utf-8")
                    side = None if not msg.get(54) else msg.get(54).decode("utf-8")
                    symbol = None if not msg.get(55) else msg.get(55).decode("utf-8")
                    price = None if not msg.get(44) else msg.get(44).decode("utf-8")
                    ord_status = (
                        None if not msg.get(39) else msg.get(39).decode("utf-8")
                    )

                    self.assertEqual("11493630", exec_id)
                    self.assertEqual("p1740758400000001000", cl_ord_id)
                    self.assertEqual("5279179", order_id)
                    self.assertEqual("SELL", SIDES.get(side, side))
                    self.assertEqual("BNBUSDT", symbol)
                    self.assertEqual("735.00000000", price)
                    self.assertEqual("PENDING_NEW", ORD_STATUS.get(ord_status))
                elif msg_seq_num == 5:
                    order_id = None if not msg.get(37) else msg.get(37).decode("utf-8")
                    side = None if not msg.get(54) else msg.get(54).decode("utf-8")
                    exec_type = (
                        None if not msg.get(150) else msg.get(150).decode("utf-8")
                    )
                    cum_qty = None if not msg.get(14) else msg.get(14).decode("utf-8")
                    leaves_qty = (
                        None if not msg.get(151) else msg.get(151).decode("utf-8")
                    )
                    cum_quote_qty = (
                        None if not msg.get(25017) else msg.get(25017).decode("utf-8")
                    )
                    trade_id = (
                        None if not msg.get(1003) else msg.get(1003).decode("utf-8")
                    )
                    last_px = None if not msg.get(31) else msg.get(31).decode("utf-8")
                    last_qty = None if not msg.get(32) else msg.get(32).decode("utf-8")

                    self.assertEqual("5279178", order_id)
                    self.assertEqual("BUY", SIDES.get(side, side))
                    self.assertEqual("TRADE", ORD_EXEC_TYPE.get(exec_type))
                    self.assertEqual("1.00000000", cum_qty)
                    self.assertEqual("0.00000000", leaves_qty)
                    self.assertEqual("576.12000000", cum_quote_qty)
                    self.assertEqual("956281", trade_id)
                    self.assertEqual("576.12000000", last_px)
                    self.assertEqual("1.00000000", last_qty)
                else:
                    exec_id = None if not msg.get(17) else msg.get(17).decode("utf-8")
                    cl_ord_id = None if not msg.get(11) else msg.get(11).decode("utf-8")
                    order_id = None if not msg.get(37) else msg.get(37).decode("utf-8")
                    side = None if not msg.get(54) else msg.get(54).decode("utf-8")
                    exec_type = (
                        None if not msg.get(150) else msg.get(150).decode("utf-8")
                    )
                    cum_qty = None if not msg.get(14) else msg.get(14).decode("utf-8")
                    leaves_qty = (
                        None if not msg.get(151) else msg.get(151).decode("utf-8")
                    )
                    cum_quote_qty = (
                        None if not msg.get(25017) else msg.get(25017).decode("utf-8")
                    )
                    trade_id = (
                        None if not msg.get(1003) else msg.get(1003).decode("utf-8")
                    )
                    last_px = None if not msg.get(31) else msg.get(31).decode("utf-8")
                    last_qty = None if not msg.get(32) else msg.get(32).decode("utf-8")

                    self.assertEqual("11493633", exec_id)
                    self.assertEqual("p1740758400000001000", cl_ord_id)
                    self.assertEqual("5279179", order_id)
                    self.assertEqual("SELL", SIDES.get(side, side))
                    self.assertEqual("NEW", ORD_EXEC_TYPE.get(exec_type))
                    self.assertEqual("0.00000000", cum_qty)
                    self.assertEqual("1.00000000", leaves_qty)
                    self.assertEqual("0.00000000", cum_quote_qty)
                    self.assertEqual(None, trade_id)
                    self.assertEqual(None, last_px)
                    self.assertEqual("0.00000000", last_qty)

        # Logout process
        client_oe.logout()
        self.logOutSent = True

        receive_thread = threading.Thread(
            target=client_oe._BinanceFixConnector__receive_messages(), daemon=True
        )
        receive_thread.start()

        # Assert the logout request
        self.assertEqual(3, socket_mock.sendall.call_count)
        self.assertEqual(
            b"8=FIX.4.4\x019=58\x0135=5\x0149=BOETRADE\x0156=SPOT\x0134=3\x0152=20250301-01:00:00.000000\x0110=218\x01",
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
