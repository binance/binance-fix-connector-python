import os
from pathlib import Path

# FIX URLs
FIX_OE_URL = "tcp+tls://fix-oe.testnet.binance.vision:9000"
FIX_MD_URL = "tcp+tls://fix-md.testnet.binance.vision:9000"

# Credentials
path = os.path.join(Path(__file__).parent.resolve(), "config.ini")

# Response types
ACTION = {"0": "NEW", "1": "CHANGE", "2": "DELETE"}
AGGRESSOR_SIDE = {"1": "BUY", "2": "SELL"}
CONTINGENCY_TYPE = {
    "1": "ONE_CANCELS_THE_OTHER ",
    "2": "ONE_TRIGGERS_THE_OTHER",
}
LIMIT_TYPES = {"1": "ORDER_LIMIT", "2": "MESSAGE_LIMIT", "3": "SUBSCRIPTION_LIMIT"}
LIST_ORD_STATUS = {"3": "EXECUTING", "6": "ALL_DONE", "7": "REJECT"}
LIST_ORD_TYPE = {"1": "ONE_CANCELS_THE_OTHER", "2": "ONE_TRIGGERS_THE_OTHER"}
LIST_STATUS_TYPE = {
    "2": "RESPONSE",
    "4": "EXEC_STARTED",
    "5": "ALL_DONE",
    "100": "UPDATED",
}
LIST_TRIG_TYPE = {"ACTIVATED": "1", "PARTIALLY_FILLED": "2", "FILLED": "3"}
LIST_TRIG_ACTION = {"RELEASE": "1", "CANCEL": "2"}
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
ORD_REJECT_REASON = {"99": "OTHER"}
RESOLUTIONS = {"s": "SECOND", "m": "MINUTE", "h": "HOUR", "d": "DAY"}
SIDES = {"1": "BUY", "2": "SELL"}
TIME_IN_FORCE = {
    "1": "GOOD_TILL_CANCEL",
    "3": "IMMEDIATE_OR_CANCEL",
    "4": "FILL_OR_KILL",
}
UPDATE = {"0": "BID", "1": "OFFER", "2": "TRADE"}

# Parameters
INSTRUMENT = "BNBUSDT"
TIMEOUT_SECONDS = 20
