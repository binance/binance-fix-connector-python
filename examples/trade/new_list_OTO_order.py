#!/usr/bin/env python3

import time

from binance_fix_connector.fix_connector import create_order_entry_session
from binance_fix_connector.utils import get_api_key, get_private_key
from constants import (
    path,
    FIX_OE_URL,
    INSTRUMENT,
    LIST_STATUS_TYPE,
    LIST_ORD_STATUS,
    LIST_ORD_TYPE,
    ORD_REJECT_REASON,
    ORD_TYPES,
    ORD_STATUS,
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
client_oe.get_all_new_messages_received()

example = "This example shows how to place a list order type OTO, where both legs are Order type LIMIT.\nCheck https://github.com/binance/binance-spot-api-docs/blob/master/fix-api.md#neworderliste for additional types."
client_oe.logger.info(example)

# PLACING OTO ORDER
msg = client_oe.create_fix_message_with_basic_header("E")
identifier = f"{time.time_ns()}"
working_leg_id = f"w{identifier}"
pending_leg_id = f"p{identifier}"

msg.append_pair(73, 2)
msg.append_pair(11, working_leg_id)
msg.append_pair(55, INSTRUMENT)
msg.append_pair(54, 2)  # SELL
msg.append_pair(38, 1)  # QTY
msg.append_pair(40, 2)  # LIMIT
msg.append_pair(44, 730)  # PRICE
msg.append_pair(59, 1)  # GTC
msg.append_pair(11, pending_leg_id)
msg.append_pair(55, INSTRUMENT)
msg.append_pair(54, 2)  # SELL
msg.append_pair(38, 1)  # QTY
msg.append_pair(40, 2)  # LIMIT
msg.append_pair(44, 735)  # PRICE
msg.append_pair(59, 1)  # GTC
msg.append_pair(25010, 1)
msg.append_pair(25011, 3)
msg.append_pair(25012, 0)
msg.append_pair(25013, 1)
msg.append_pair(1385, 2)  # OTO
msg.append_pair(25014, f"{identifier}")
client_oe.send_message(msg)

responses = client_oe.retrieve_messages_until(message_type=["N"])
resp = next(
    (x for x in responses if x.message_type.decode("utf-8") == "N"),
    None,
)
client_oe.logger.info("*" * 50)
client_oe.logger.info("Parsing response List status (N) for an OTO type.")
client_oe.logger.info("*" * 50)


symbol = None if not resp.get(55) else resp.get(55).decode("utf-8")
list_status_type = None if not resp.get(429) else resp.get(429).decode("utf-8")
list_ord_status = None if not resp.get(431) else resp.get(431).decode("utf-8")
cl_list_id = None if not resp.get(25014) else resp.get(25014).decode("utf-8")
contingency = None if not resp.get(1385) else resp.get(1385).decode("utf-8")
header = f"Symbol: {symbol} | List status: {LIST_STATUS_TYPE.get(list_status_type,list_status_type)} | List order status: {LIST_ORD_STATUS.get(list_ord_status,list_ord_status)}"
header_2 = f"Client list id: {cl_list_id} | List type: {LIST_ORD_TYPE.get(contingency,contingency)} | "
client_oe.logger.info(header)
client_oe.logger.info(header_2)

orders = 0 if not resp.get(73) else int(resp.get(73).decode("utf-8"))
for i in range(orders):
    cl_ord_id = None if not resp.get(11, i + 1) else resp.get(11, i + 1).decode("utf-8")
    symbol = None if not resp.get(55, i + 1) else resp.get(55, i + 1).decode("utf-8")
    ord_rej_reason = (
        None if not resp.get(103, i + 1) else resp.get(103, i + 1).decode("utf-8")
    )
    error_code = (
        None if not resp.get(25016, i + 1) else resp.get(25016, i + 1).decode("utf-8")
    )
    text = None if not resp.get(58, i + 1) else resp.get(58, i + 1).decode("utf-8")
    body = f"Client order ID: {cl_ord_id} | Symbol: {symbol}"
    body_2 = f"Reason: {ORD_REJECT_REASON.get(ord_rej_reason,ord_rej_reason)} | Error code: {error_code} | Msg: {text}"

    client_oe.logger.info(body)
    client_oe.logger.info(body_2)


# +++++++++++++++++++++++++++
responses = client_oe.retrieve_messages_until(message_type=["8"])
responses.extend(client_oe.retrieve_messages_until(message_type=["8"]))
resp = next(
    (
        x
        for x in responses
        if x.message_type.decode("utf-8") == "8"
        and x.get(11)
        and x.get(11).decode("utf-8") == working_leg_id
    ),
    None,
)

client_oe.logger.info("*" * 50)
client_oe.logger.info("Parsing response Execution Report (8) for the working leg order")
client_oe.logger.info("*" * 50)

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
    f"Status: {ORD_STATUS.get(ord_status,ord_status)} | Reason: {ORD_REJECT_REASON.get(ord_rej_reason,ord_rej_reason)}",
)
client_oe.logger.info(f"Error code: {error_code} | Reason: {text}")


resp = next(
    (
        x
        for x in responses
        if x.message_type.decode("utf-8") == "8"
        and x.get(11)
        and x.get(11).decode("utf-8") == working_leg_id
    ),
    None,
)

client_oe.logger.info("*" * 50)
client_oe.logger.info("Parsing response Execution Report (8) for the pending leg order")
client_oe.logger.info("*" * 50)

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
    f"Status: {ORD_STATUS.get(ord_status,ord_status)} | Reason: {ORD_REJECT_REASON.get(ord_rej_reason,ord_rej_reason)}",
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
