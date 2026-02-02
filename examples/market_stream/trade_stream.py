#!/usr/bin/env python3

import time
from datetime import datetime, timedelta

from binance_fix_connector.fix_connector import (
    BinanceFixConnector,
    create_market_data_session,
)
from binance_fix_connector.utils import get_api_key, get_private_key
from constants import (
    path,
    ACTION,
    AGGRESSOR_SIDE,
    FIX_MD_URL,
    INSTRUMENT,
    UPDATE,
    TIMEOUT_SECONDS,
)

# Credentials
API_KEY, PATH_TO_PRIVATE_KEY_PEM_FILE = get_api_key(path)


def show_rendered_market_trade_stream(client: BinanceFixConnector) -> None:
    """Show the current TRADE stream messages received."""
    for _ in range(client.queue_msg_received.qsize()):
        msg = client.queue_msg_received.get()
        if msg.message_type.decode("utf-8") == "X":
            subscription_id = None if not msg.get(262) else msg.get(262).decode("utf-8")
            updates = 0 if not msg.get(268) else int(msg.get(268).decode("utf-8"))
            symbol = None if not msg.get(55) else msg.get(55).decode("utf-8")
            header = f"Subscription: {subscription_id} -> {updates} updates received for Symbol: {symbol}"
            client.logger.info(header)
            for i in range(updates):
                update_type = (
                    None
                    if not msg.get(269, i + 1)
                    else msg.get(269, i + 1).decode("utf-8")
                )
                update_type = f"Update type: {UPDATE.get(update_type, update_type)}"
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
                trade_id = (
                    None
                    if not msg.get(1003, i + 1)
                    else msg.get(1003, i + 1).decode("utf-8")
                )
                transact_time = (
                    None
                    if not msg.get(60, i + 1)
                    else msg.get(60, i + 1).decode("utf-8")
                )
                aggressor_side = (
                    None
                    if not msg.get(2446, i + 1)
                    else msg.get(2446, i + 1).decode("utf-8")
                )
                aggressor_side = f"Aggressor side: {AGGRESSOR_SIDE.get(aggressor_side ,aggressor_side)}"
                body = f"{update_type} | trade_id: {trade_id} | Transaction time: {transact_time} | Price: {price} | Qty: {qty} | {aggressor_side}"
                client.logger.info(body)


client_md = create_market_data_session(
    api_key=API_KEY,
    private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
    endpoint=FIX_MD_URL,
)
client_md.retrieve_messages_until(message_type=["A"])

example = "This example shows how to subscribe to a trade stream.\nCheck https://github.com/binance/binance-spot-api-docs/blob/master/fix-api.md#tradestream for additional types."
client_md.logger.info(example)


msg = client_md.create_fix_message_with_basic_header("V")
msg.append_pair(262, "TRADE_STREAM")  # md req id
msg.append_pair(263, 1)  # Subscription type

msg.append_pair(264, 1)  # market depth
msg.append_pair(266, "Y")  # aggregated book
msg.append_pair(146, 1)  # NoSymbols
msg.append_pair(55, INSTRUMENT)  # Symbol
msg.append_pair(267, 1)  # NoMDEntries
msg.append_pair(269, 2)  # MDEntry

client_md.logger.info("*" * 50)
client_md.logger.info("MARKET_DATA_REQUEST (V): SUBSCRIBING")
client_md.logger.info("*" * 50)
client_md.send_message(msg)


client_md.logger.info(
    f"Subscribed to the Trade stream, showing stream for {TIMEOUT_SECONDS} seconds."
)
timeout = datetime.now() + timedelta(seconds=TIMEOUT_SECONDS)
while datetime.now() < timeout:
    time.sleep(0.01)
    show_rendered_market_trade_stream(client_md)

client_md.logger.info(
    f"Subscribed to the Trade stream, showing stream for {TIMEOUT_SECONDS} seconds."
)

msg = client_md.create_fix_message_with_basic_header("V")
msg.append_pair(262, "TRADE_STREAM")  # md req id
msg.append_pair(263, 2)  # Subscription type

msg.append_pair(264, 1)  # market depth
msg.append_pair(266, "Y")  # aggregated book
msg.append_pair(146, 1)  # NoSymbols
msg.append_pair(55, INSTRUMENT)  # Symbol
msg.append_pair(267, 1)  # NoMDEntries
msg.append_pair(269, 2)  # MDEntry

client_md.logger.info("*" * 50)
client_md.logger.info("MARKET_DATA_REQUEST (V): UNSUBSCRIBING")
client_md.logger.info("*" * 50)
client_md.send_message(msg)

# LOGOUT
client_md.logger.info("LOGOUT (5)")
client_md.logout()
client_md.retrieve_messages_until(message_type=["5"])
client_md.logger.info(
    "Closing the connection with server as we already sent the logout message"
)
client_md.disconnect()
