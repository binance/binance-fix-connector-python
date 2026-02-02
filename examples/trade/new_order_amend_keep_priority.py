#!/usr/bin/env python3

import time

from binance_fix_connector.fix_connector import create_order_entry_session
from binance_fix_connector.utils import get_api_key, get_private_key
from constants import path, FIX_OE_URL, INSTRUMENT

# Credentials
API_KEY, PATH_TO_PRIVATE_KEY_PEM_FILE = get_api_key(path)

client_oe = create_order_entry_session(
    api_key=API_KEY,
    private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
    endpoint=FIX_OE_URL,
)

client_oe.retrieve_messages_until(message_type=["A"])

# PLACING SIMPLE ORDER
msg = client_oe.create_fix_message_with_basic_header("D")
msg.append_pair(38, 0.01)  # ORD QTY
msg.append_pair(40, 1)  # ORD TYPE
msg.append_pair(11, str(time.time_ns()))  # CL ORD ID
msg.append_pair(54, 1)  # SIDE
msg.append_pair(55, INSTRUMENT)  # SYMBOL
client_oe.send_message(msg)


responses = client_oe.retrieve_messages_until(message_type=["8"])
resp = next(
    (x for x in responses if x.message_type.decode("utf-8") == "8"),
    None,
)

ord_id = None if not resp.get(37) else resp.get(37).decode("utf-8")

# PLACING AMEND KEEP PRIORITY ORDER
cl_ord_id = str(time.time_ns())
msg = client_oe.create_fix_message_with_basic_header("XAK")
msg.append_pair(11, cl_ord_id)  # ClOrdID
msg.append_pair(37, ord_id)  # ord_id
msg.append_pair(55, INSTRUMENT)  # SYMBOL
msg.append_pair(38, 0.02)  # QTY
client_oe.send_message(msg)


amend_responses = client_oe.retrieve_messages_until(
    message_type=["8", "XAR"], message_cl_ord_id=cl_ord_id
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
if msg_type == "XAR":
    order_id = None if not resp.get(37) else resp.get(37).decode("utf-8")
    symbol = None if not resp.get(55) else resp.get(55).decode("utf-8")
    order_qty = None if not resp.get(38) else resp.get(38).decode("utf-8")
    error_text = None if not resp.get(25016) else resp.get(25016).decode("utf-8")
    text = None if not resp.get(58) else resp.get(58).decode("utf-8")

    client_oe.logger.info("Parsing response Order Amend Reject (XAR) for an order.")
    client_oe.logger.info(
        f"Order -> Order ID: {order_id} / Symbol: {symbol} / Order Qty: {order_qty}"
    )
    client_oe.logger.info(f"Error code: {error_text} | Reason: {text}")
elif msg_type == "8":
    order_id = None if not resp.get(37) else resp.get(37).decode("utf-8")
    symbol = None if not resp.get(55) else resp.get(55).decode("utf-8")
    order_qty = None if not resp.get(38) else resp.get(38).decode("utf-8")
    ord_status = None if not resp.get(39) else resp.get(39).decode("utf-8")

    client_oe.logger.info("Parsing response Execution Report (8) for an order amend.")
    client_oe.logger.info(
        f"Order -> Order ID: {order_id} / Symbol: {symbol} / Order Qty: {order_qty} / Status: {ord_status}"
    )

# LOGOUT
client_oe.logger.info("LOGOUT (5)")
client_oe.logout()
client_oe.retrieve_messages_until(message_type=["5"])
client_oe.logger.info(
    "Closing the connection with server as we already sent the logout message"
)
client_oe.disconnect()
