#!/usr/bin/env python3

import time

from binance_fix_connector.fix_connector import create_order_entry_session
from binance_fix_connector.utils import get_api_key, get_private_key
from constants import path, FIX_OE_URL, INSTRUMENT, ORD_STATUS

# Credentials
API_KEY, PATH_TO_PRIVATE_KEY_PEM_FILE = get_api_key(path)

client_oe = create_order_entry_session(
    api_key=API_KEY,
    private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
    endpoint=FIX_OE_URL,
)

client_oe.retrieve_messages_until(message_type=["A"])

# PLACING SIMPLE ORDER
cl_ord_id = str(time.time_ns())
msg = client_oe.create_fix_message_with_basic_header("D")
msg.append_pair(38, 0.02)  # ORD QTY
msg.append_pair(40, 1)  # ORD TYPE
msg.append_pair(11, cl_ord_id)  # CL ORD ID
msg.append_pair(54, 1)  # SIDE
msg.append_pair(55, INSTRUMENT)  # SYMBOL
client_oe.send_message(msg)

responses = client_oe.retrieve_messages_until(message_type=["8"])
resp = next(
    (x for x in responses if x.message_type.decode("utf-8") == "8"),
    None,
)

ord_id = None if not resp.get(37) else resp.get(37).decode("utf-8")

# PLACING CANCEL REQUEST AND NEW ORDER SINGLE MESSAGE
cl_ord_id_2 = str(time.time_ns())
msg = client_oe.create_fix_message_with_basic_header("XCN")
msg.append_pair(11, cl_ord_id_2)  # ClOrdID
msg.append_pair(25033, 2)  # OrderCancelRequestAndNewOrderSingleMode
msg.append_pair(37, ord_id)  # ord_id
msg.append_pair(25034, cl_ord_id)  # CancelClOrdID
msg.append_pair(40, 1)  # ORD TYPE
msg.append_pair(55, INSTRUMENT)  # SYMBOL
msg.append_pair(54, 2)  # SIDE
msg.append_pair(38, 0.01)  # QTY
client_oe.send_message(msg)

amend_responses = client_oe.retrieve_messages_until(
    message_type=["8", "9"], message_cl_ord_id=cl_ord_id_2
)
resp = next(
    (
        x
        for x in amend_responses
        if x.get(11) and x.get(11).decode("utf-8") == cl_ord_id
    ),
    None,
)

msg_type = None if not resp.get(35) else resp.get(35).decode("utf-8")
if msg_type == "8":
    order_id = None if not resp.get(37) else resp.get(37).decode("utf-8")
    symbol = None if not resp.get(55) else resp.get(55).decode("utf-8")
    order_qty = None if not resp.get(38) else resp.get(38).decode("utf-8")
    self_trade_prevention_type = (
        None if not resp.get(25001) else resp.get(25001).decode("utf-8")
    )
    exec_type = None if not resp.get(150) else resp.get(150).decode("utf-8")
    cum_qty = None if not resp.get(14) else resp.get(14).decode("utf-8")
    leaves_qty = None if not resp.get(151) else resp.get(151).decode("utf-8")
    cum_quote_qty = None if not resp.get(25017) else resp.get(25017).decode("utf-8")
    aggressor_indicator = None if not resp.get(1057) else resp.get(1057).decode("utf-8")
    trade_id = None if not resp.get(1003) else resp.get(1003).decode("utf-8")
    last_px = None if not resp.get(31) else resp.get(31).decode("utf-8")
    last_qty = None if not resp.get(32) else resp.get(32).decode
    order_status = None if not resp.get(39) else resp.get(39).decode("utf-8")
    client_oe.logger.info("CANCEL REQUEST AND NEW ORDER SINGLE processed successfully.")
    client_oe.logger.info(f"Client order ID: {cl_ord_id}")
    client_oe.logger.info(f"Symbol: {symbol}")
    client_oe.logger.info(
        f"Order -> Self Trade Prevention Type: {self_trade_prevention_type} | Exec Type: {exec_type} | Cum Qty: {cum_qty} | Leaves Qty: {leaves_qty} | Cum Quote Qty: {cum_quote_qty} | Aggressor Indicator: {aggressor_indicator} | Trade ID: {trade_id} | Last Px: {last_px}",
    )
    client_oe.logger.info(
        f"Status: {ORD_STATUS.get(order_status, order_status)}",
    )
elif msg_type == "9":
    order_id = None if not resp.get(37) else resp.get(37).decode("utf-8")
    symbol = None if not resp.get(55) else resp.get(55).decode("utf-8")
    error_text = None if not resp.get(25016) else resp.get(25016).decode("utf-8")
    text = None if not resp.get(58) else resp.get(58).decode("utf-8")

    client_oe.logger.info("*" * 50)
    client_oe.logger.info("Parsing response Order Cancel Reject (9) for an order.")
    client_oe.logger.info(f"Order -> Order ID: {order_id} / Symbol: {symbol}")
    client_oe.logger.info(f"Error code: {error_text} | Reason: {text}")

# LOGOUT
client_oe.logger.info("LOGOUT (5)")
client_oe.logout()
client_oe.retrieve_messages_until(message_type=["5"])
client_oe.logger.info(
    "Closing the connection with server as we already sent the logout message"
)
client_oe.disconnect()
