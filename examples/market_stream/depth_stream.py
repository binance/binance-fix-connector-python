#!/usr/bin/env python3

import time
from datetime import datetime, timedelta


from binance_fix_connector.fix_connector import (
    BinanceFixConnector,
    create_market_data_session,
)
from binance_fix_connector.utils import get_api_key, get_private_key
from constants import path, ACTION, FIX_MD_URL, INSTRUMENT, UPDATE, TIMEOUT_SECONDS

# Credentials
API_KEY, PATH_TO_PRIVATE_KEY_PEM_FILE = get_api_key(path)


def show_rendered_snapshot_message(client: BinanceFixConnector) -> None:
    """Show the snapshot message received."""
    responses = client.retrieve_messages_until(message_type=["W"])
    for msg in responses:
        if msg.message_type.decode("utf-8") == "W":
            client.logger.info("Parsing a MarketDataSnapshot (W) ...")
            subscription_id = None if not msg.get(262) else msg.get(262).decode("utf-8")
            updates = 0 if not msg.get(268) else int(msg.get(268).decode("utf-8"))
            symbol = None if not msg.get(55) else msg.get(55).decode("utf-8")
            last_book_id = (
                None if not msg.get(25044) else msg.get(25044).decode("utf-8")
            )
            header = f"Snapshot: {subscription_id} -> {updates} updates received for Symbol: {symbol} and LastBookId: {last_book_id}"
            client.logger.info(header)
            for i in range(updates):
                update_type = (
                    None
                    if not msg.get(269, i + 1)
                    else msg.get(269, i + 1).decode("utf-8")
                )
                update_type = f"Update type: {UPDATE.get(update_type,update_type)}"
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
                body = f"{update_type} | Price: {price} | Qty: {qty}"
                client.logger.info(body)


def show_rendered_market_depth_stream(client: BinanceFixConnector) -> None:
    """Show the current DEPTH stream messages received."""
    for _ in range(client.queue_msg_received.qsize()):
        msg = client.queue_msg_received.get()
        if msg.message_type.decode("utf-8") == "X":
            subscription_id = None if not msg.get(262) else msg.get(262).decode("utf-8")
            updates = 0 if not msg.get(268) else int(msg.get(268).decode("utf-8"))
            symbol = None if not msg.get(55) else msg.get(55).decode("utf-8")
            first_book_id = (
                None if not msg.get(25043) else msg.get(25043).decode("utf-8")
            )
            last_book_id = (
                None if not msg.get(25044) else msg.get(25044).decode("utf-8")
            )
            header = f"Subscription: {subscription_id} -> {updates} updates received for Symbol: {symbol} between FirstBookId: {first_book_id} and LastBookId: {last_book_id}"
            client.logger.info(header)
            qty_index = 0
            for i in range(updates):
                action = (
                    None
                    if not msg.get(279, i + 1)
                    else msg.get(279, i + 1).decode("utf-8")
                )
                update_type = (
                    None
                    if not msg.get(269, i + 1)
                    else msg.get(269, i + 1).decode("utf-8")
                )
                price = (
                    None
                    if not msg.get(270, i + 1)
                    else msg.get(270, i + 1).decode("utf-8")
                )
                qty = (
                    None
                    if not msg.get(271, i + 1 - qty_index)
                    else msg.get(271, i + 1 - qty_index).decode("utf-8")
                )
                qty_str = f"| Qty: {qty}"
                if action == "2":
                    qty_str = ""
                    qty_index += 1

                body = f"Action: {ACTION.get(action, action)} | Update: {UPDATE.get(update_type,update_type)} | Price: {price} {qty_str}"
                client.logger.info(body)


client_md = create_market_data_session(
    api_key=API_KEY,
    private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
    endpoint=FIX_MD_URL,
)
client_md.retrieve_messages_until(message_type=["A"])

example = "This example shows how to subscribe to a book depth stream.\nCheck https://github.com/binance/binance-spot-api-docs/blob/master/fix-api.md#diffdepthstream for additional types."
client_md.logger.info(example)


msg = client_md.create_fix_message_with_basic_header("V")
msg.append_pair(262, "DEPTH_STREAM")  # md req id
msg.append_pair(263, 1)  # Subscription type

msg.append_pair(264, 50)  # market depth
msg.append_pair(266, "Y")  # aggregated book
msg.append_pair(146, 1)  # NoSymbols
msg.append_pair(55, INSTRUMENT)  # Symbol
msg.append_pair(267, 2)  # NoMDEntries
msg.append_pair(269, 0)  # MDEntry
msg.append_pair(269, 1)  # MDEntry

client_md.logger.info("*" * 50)
client_md.logger.info("MARKET_DATA_REQUEST (V): SUBSCRIBING")
client_md.logger.info("*" * 50)
client_md.send_message(msg)
client_md.logger.info(
    f"Subscribed to the Depth stream, showing stream for {TIMEOUT_SECONDS} seconds."
)

show_rendered_snapshot_message(client_md)
timeout = datetime.now() + timedelta(seconds=TIMEOUT_SECONDS)
while datetime.now() < timeout:
    time.sleep(0.01)
    show_rendered_market_depth_stream(client_md)

msg = client_md.create_fix_message_with_basic_header("V")
msg.append_pair(262, "DEPTH_STREAM")  # md req id
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
