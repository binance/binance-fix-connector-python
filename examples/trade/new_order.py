#!/usr/bin/env python3

import time

from binance_fix_connector.fix_connector import create_order_entry_session
from binance_fix_connector.utils import get_api_key, get_private_key
from constants import (
    path,
    FIX_OE_URL,
    INSTRUMENT,
    ORD_REJECT_REASON,
    ORD_STATUS,
    ORD_TYPES,
    SIDES,
    TIME_IN_FORCE,
)

# Credentials
API_KEY, PATH_TO_PRIVATE_KEY_PEM_FILE = get_api_key(path)

client_oe = create_order_entry_session(
    api_key=API_KEY,
    private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
    endpoint=FIX_OE_URL,
)
client_oe.retrieve_messages_until(message_type=["A"])

example = "This example shows how to place a single order. Order type LIMIT.\nCheck https://github.com/binance/binance-spot-api-docs/blob/master/fix-api.md#newordersingled for additional types."
client_oe.logger.info(example)

# PLACING SIMPLE ORDER
msg = client_oe.create_fix_message_with_basic_header("D")
msg.append_pair(38, 1)  # ORD QTY
msg.append_pair(40, 2)  # ORD TYPE
msg.append_pair(11, str(time.time_ns()))  # CL ORD ID
msg.append_pair(44, 730)  # PRICE
msg.append_pair(54, 2)  # SIDE
msg.append_pair(55, INSTRUMENT)  # SYMBOL
msg.append_pair(59, 1)  # TIME IN FORCE
client_oe.send_message(msg)


responses = client_oe.retrieve_messages_until(message_type=["8"])
resp = next(
    (x for x in responses if x.message_type.decode("utf-8") == "8"),
    None,
)
client_oe.logger.info("Parsing response Execution Report (8) for an order LIMIT type.")

cl_ord_id = None if not resp.get(11) else resp.get(11).decode("utf-8")
order_qty = None if not resp.get(38) else resp.get(38).decode("utf-8")
ord_type = None if not resp.get(40) else resp.get(40).decode("utf-8")
side = None if not resp.get(54) else resp.get(54).decode("utf-8")
symbol = None if not resp.get(55) else resp.get(55).decode("utf-8")
price = None if not resp.get(44) else resp.get(44).decode("utf-8")
time_in_force = None if not resp.get(59) else resp.get(59).decode("utf-8")
cum_qty = None if not resp.get(14) else resp.get(14).decode("utf-8")
last_qty = None if not resp.get(32) else resp.get(32).decode("utf-8")
ord_status = None if not resp.get(39) else resp.get(39).decode("utf-8")
ord_rej_reason = None if not resp.get(103) else resp.get(103).decode("utf-8")
error_code = None if not resp.get(25016) else resp.get(25016).decode("utf-8")
text = None if not resp.get(58) else resp.get(58).decode("utf-8")


client_oe.logger.info(f"Client order ID: {cl_ord_id}")
client_oe.logger.info(f"Symbol: {symbol}")
client_oe.logger.info(
    f"Order -> Type: {ORD_TYPES.get(ord_type, ord_type)} | Side: {SIDES.get(side, side)} | TimeInForce: {TIME_IN_FORCE.get(time_in_force,time_in_force)}",
)
client_oe.logger.info(
    f"Price: {price} | Quantity: {order_qty} | cum qty: {cum_qty} | last qty: {last_qty}"
)
client_oe.logger.info(
    f"Status: {ORD_STATUS.get(ord_status,ord_status)} | Msg: {ORD_REJECT_REASON.get(ord_rej_reason,ord_rej_reason)}",
)
client_oe.logger.info(f"Error code: {error_code} | Reason: {text}")


# LOGOUT
client_oe.logger.info("LOGOUT (5)")
client_oe.logout()
client_oe.retrieve_messages_until(message_type=["5"])
client_oe.logger.info(
    "Closing the connection with server as we already sent the logout message"
)
client_oe.disconnect()
