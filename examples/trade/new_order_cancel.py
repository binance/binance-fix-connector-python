#!/usr/bin/env python3

import time

from binance_fix_connector.fix_connector import create_order_entry_session
from binance_fix_connector.utils import get_api_key, get_private_key
from constants import (
    path,
    FIX_OE_URL,
    INSTRUMENT,
    CONTINGENCY_TYPE,
    LIST_STATUS_TYPE,
    LIST_ORD_STATUS,
)

# Credentials
API_KEY, PATH_TO_PRIVATE_KEY_PEM_FILE = get_api_key(path)

client_oe = create_order_entry_session(
    api_key=API_KEY,
    private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
    endpoint=FIX_OE_URL,
)

client_oe.retrieve_messages_until(message_type=["A"])

# PLACING CANCEL ORDER
msg = client_oe.create_fix_message_with_basic_header("F")
msg.append_pair(11, "order_id")  # ClOrdID
msg.append_pair(55, INSTRUMENT)  # SYMBOL
client_oe.send_message(msg)

responses = client_oe.retrieve_messages_until(message_type=["3", "8", "9", "N"])


def show_execution_response(responses, client_oe):
    """Show execution response result."""

    client_oe.logger.info(
        "Parsing response Execution Report (8) for an order LIMIT type."
    )

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


def show_order_cancel_reject_response(responses, client_oe):
    """Show order cancel reject response result."""

    client_oe.logger.info("Parsing response Order Cancel Reject (9) ...")

    cl_ord_id = None if not resp.get(11) else resp.get(11).decode("utf-8")
    orig_cl_ord_id = None if not resp.get(41) else resp.get(41).decode("utf-8")
    order_id = None if not resp.get(37) else resp.get(37).decode("utf-8")
    symbol = None if not resp.get(55) else resp.get(55).decode("utf-8")
    cancel_restrictions = (
        None if not resp.get(25002) else resp.get(25002).decode("utf-8")
    )
    cxl_rej_response_to = None if not resp.get(434) else resp.get(434).decode("utf-8")
    error_code = None if not resp.get(25016) else resp.get(25016).decode("utf-8")
    text = None if not resp.get(58) else resp.get(58).decode("utf-8")

    client_oe.logger.info(f"Client order ID: {cl_ord_id}")
    client_oe.logger.info(f"Original Client order ID: {orig_cl_ord_id}")
    client_oe.logger.info(f"Order ID: {order_id}")
    client_oe.logger.info(f"Symbol: {symbol}")
    client_oe.logger.info(f"Cancel Restrictions: {cancel_restrictions}")
    client_oe.logger.info(f"Cancel Reject Response To: {cxl_rej_response_to}")
    client_oe.logger.info(f"Reason: {text}")


def show_list_status_response(responses, client_oe):
    """Show list status response result."""

    client_oe.logger.info("Parsing response List Status (N) ...")
    symbol = None if not resp.get(55) else resp.get(55).decode("utf-8")
    list_id = None if not resp.get(66) else resp.get(66).decode("utf-8")
    contingency_type = None if not resp.get(1385) else resp.get(1385).decode("utf-8")
    list_status_type = None if not resp.get(429) else resp.get(429).decode("utf-8")
    list_order_status = None if not resp.get(431) else resp.get(431).decode("utf-8")
    list_reject_reason = None if not resp.get(1386) else resp.get(1386).decode("utf-8")
    transact_time = None if not resp.get(60) else resp.get(60).decode("utf-8")
    error_code = None if not resp.get(25016) else resp.get(25016).decode("utf-8")
    text = None if not resp.get(58) else resp.get(58).decode("utf-8")
    no_orders = 0 if not resp.get(73) else int(resp.get(73).decode("utf-8"))

    client_oe.logger.info(f"Symbol: {symbol}")
    client_oe.logger.info(f"List ID: {list_id}")
    client_oe.logger.info(
        f"Contingency Type: {CONTINGENCY_TYPE.get(contingency_type, contingency_type)}"
    )
    client_oe.logger.info(
        f"List Status Type: {LIST_STATUS_TYPE.get(list_status_type, list_status_type)}"
    )
    client_oe.logger.info(
        f"List Order Status: {LIST_ORD_STATUS.get(list_order_status, list_order_status)}"
    )
    client_oe.logger.info(f"List Reject Reason: {list_reject_reason}")
    client_oe.logger.info(f"Transact Time: {transact_time}")
    client_oe.logger.info(f"Number of Orders: {no_orders}")
    client_oe.logger.info(f"Error code: {error_code} | Reason: {text}")


for x in responses:
    if x.message_type.decode("utf-8") == "8":
        client_oe.logger.info("Parsing a ExecutionReport (8) response...")
        show_execution_response(responses, client_oe)
    elif x.message_type.decode("utf-8") == "9":
        client_oe.logger.info("Parsing a OrderCancelReject (9) response...")
        show_order_cancel_reject_response(responses, client_oe)
    elif x.message_type.decode("utf-8") == "N":
        client_oe.logger.info("Parsing a ListStatus (N) response...")
        show_list_status_response(responses, client_oe)

# LOGOUT
client_oe.logger.info("LOGOUT (5)")
client_oe.logout()
client_oe.retrieve_messages_until(message_type=["5"])
client_oe.logger.info(
    "Closing the connection with server as we already sent the logout message"
)
client_oe.disconnect()
