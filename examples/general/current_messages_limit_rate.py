#!/usr/bin/env python3

from binance_fix_connector.fix_connector import (
    BinanceFixConnector,
    create_market_data_session,
    create_order_entry_session,
)
from binance_fix_connector.utils import get_api_key, get_private_key
from constants import path, RESOLUTIONS, LIMIT_TYPES, FIX_OE_URL, FIX_MD_URL

# Credentials
API_KEY, PATH_TO_PRIVATE_KEY_PEM_FILE = get_api_key(path)


def show_rendered_limit_session(client: BinanceFixConnector) -> None:
    """Show the current LIMITS the session has."""
    responses = client.retrieve_messages_until(message_type=["XLR"])
    for msg in responses:
        if msg.message_type.decode("utf-8") == "XLR":
            limits = 0 if not msg.get(25003) else int(msg.get(25003).decode("utf-8"))
            client.logger.info("Parsing response LimitResponse (XLR)")
            _info_header = f"Limits: ({limits})"
            client.logger.info(_info_header)
            for i in range(limits):
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
                interval = (
                    None
                    if not msg.get(25007, i + 1)
                    else msg.get(25007, i + 1).decode("utf-8")
                )
                interval_res = (
                    None
                    if not msg.get(25008, i + 1)
                    else msg.get(25008, i + 1).decode("utf-8")
                )
                interval_str = (
                    ""
                    if not interval
                    else f"| Interval: {interval} {RESOLUTIONS.get(interval_res, interval_res)}"
                )
                _info_body = f"Type: {LIMIT_TYPES.get(limit_type, limit_type)} | Count: {limit_count} | Max: {limit_max} {interval_str}"
                client.logger.info(_info_body)


# FIX OE
client_oe = create_order_entry_session(
    api_key=API_KEY,
    private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
    endpoint=FIX_OE_URL,
)
client_oe.retrieve_messages_until(message_type=["A"])

example = "This example shows how to query for current session limits and how to parse it's data. Check https://github.com/binance/binance-spot-api-docs/blob/master/fix-api.md#limitqueryxlq for additional information."
client_oe.logger.info(example)

msg = client_oe.create_fix_message_with_basic_header("XLQ")
msg.append_pair(6136, "current_message_rate")
client_oe.logger.info("LimitQuery (XLQ)")
client_oe.send_message(msg)
show_rendered_limit_session(client_oe)

# LOGOUT
client_oe.logger.info("LOGOUT (5)")
client_oe.logout()
client_oe.retrieve_messages_until(message_type=["5"])
client_oe.logger.info(
    "Closing the connection with server as we already sent the logout message"
)
client_oe.disconnect()

# FIX MD
client_md = create_market_data_session(
    api_key=API_KEY,
    private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
    endpoint=FIX_MD_URL,
)
client_md.retrieve_messages_until(message_type=["A"])

example = "This example shows how to query for current session limits and how to parse it's data. Check https://github.com/binance/binance-spot-api-docs/blob/master/fix-api.md for additional information."
client_md.logger.info(example)

msg = client_md.create_fix_message_with_basic_header("XLQ")
msg.append_pair(6136, "current_message_rate")
client_md.logger.info("LimitQuery (XLQ)")
client_md.send_message(msg)
show_rendered_limit_session(client_md)

# LOGOUT
client_md.logger.info("LOGOUT (5)")
client_md.logout()
client_md.retrieve_messages_until(message_type=["5"])
client_md.logger.info(
    "Closing the connection with server as we already sent the logout message"
)
client_md.disconnect()
