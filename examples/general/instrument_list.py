#!/usr/bin/env python3

import time

from binance_fix_connector.fix_connector import (
    BinanceFixConnector,
    create_market_data_session,
)
from binance_fix_connector.utils import get_api_key, get_private_key
from constants import path, FIX_MD_URL

# Credentials
API_KEY, PATH_TO_PRIVATE_KEY_PEM_FILE = get_api_key(path)


def show_rendered_instrument_list(client: BinanceFixConnector) -> None:
    """Show the instrument list messages received."""
    for _ in range(client.queue_msg_received.qsize()):
        msg = client.queue_msg_received.get()
        if msg.message_type.decode("utf-8") == "y":
            instrument_req_id = (
                None if not msg.get(320) else msg.get(320).decode("utf-8")
            )
            symbols = 0 if not msg.get(146) else int(msg.get(146).decode("utf-8"))
            for i in range(symbols):
                symbol = (
                    None
                    if not msg.get(55, i + 1)
                    else msg.get(55, i + 1).decode("utf-8")
                )
                currency = (
                    None
                    if not msg.get(15, i + 1)
                    else msg.get(15, i + 1).decode("utf-8")
                )
                header = f"Symbol: {symbol}, Currency: {currency}"
                min_price = (
                    None
                    if not msg.get(2551, i + 1)
                    else msg.get(2551, i + 1).decode("utf-8")
                )
                max_price = (
                    None
                    if not msg.get(2552, i + 1)
                    else msg.get(2552, i + 1).decode("utf-8")
                )
                min_price_inc = (
                    None
                    if not msg.get(969, i + 1)
                    else msg.get(969, i + 1).decode("utf-8")
                )
                body1 = f"Min price: {min_price} | Max price: {max_price} | Min price inc: {min_price_inc}"
                min_trade_vol = (
                    None
                    if not msg.get(562, i + 1)
                    else msg.get(562, i + 1).decode("utf-8")
                )
                max_trade_vol = (
                    None
                    if not msg.get(1140, i + 1)
                    else msg.get(1140, i + 1).decode("utf-8")
                )
                min_qty = (
                    None
                    if not msg.get(25039, i + 1)
                    else msg.get(25039, i + 1).decode("utf-8")
                )
                body2 = f"Min trade vol: {min_trade_vol} | Max trade vol: {max_trade_vol} | Min Qty: {min_qty}"
                market_min_trade_vol = (
                    None
                    if not msg.get(25040, i + 1)
                    else msg.get(25040, i + 1).decode("utf-8")
                )
                market_max_trade_vol = (
                    None
                    if not msg.get(25041, i + 1)
                    else msg.get(25041, i + 1).decode("utf-8")
                )
                market_min_qty = (
                    None
                    if not msg.get(25042, i + 1)
                    else msg.get(25042, i + 1).decode("utf-8")
                )
                body3 = f"Market Min trade vol: {market_min_trade_vol} | Market Max trade vol: {market_max_trade_vol} | Market Min Qty: {market_min_qty}"

                client.logger.info(header)
                client.logger.info(body1)
                client.logger.info(body2)
                client.logger.info(body3)


client_md = create_market_data_session(
    api_key=API_KEY,
    private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
    endpoint=FIX_MD_URL,
)
client_md.retrieve_messages_until(message_type=["A"])

example = "This example shows how to query information about active instruments.\nCheck https://github.com/binance/binance-spot-api-docs/blob/master/fix-api.md#instrumentlistrequestx for additional types."
client_md.logger.info(example)


msg = client_md.create_fix_message_with_basic_header("x")
msg.append_pair(320, "GetInstrumentList")  # md req id
msg.append_pair(559, 0)  # InstrumentListRequestType: Single symbol
msg.append_pair(55, "BNBUSDT")  # Symbol


client_md.logger.info("*" * 50)
client_md.logger.info("INSTRUMENT_LIST_REQUEST (x): Single symbol")
client_md.logger.info("*" * 50)
client_md.send_message(msg)

time.sleep(1)
show_rendered_instrument_list(client_md)

# LOGOUT
client_md.logger.info("LOGOUT (5)")
client_md.logout()
client_md.retrieve_messages_until(message_type=["5"])
client_md.logger.info(
    "Closing the connection with server as we already sent the logout message"
)
client_md.disconnect()
