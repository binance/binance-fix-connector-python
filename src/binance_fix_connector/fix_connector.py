#!/usr/bin/env python3
from __future__ import annotations

import base64
import contextlib
import logging
import socket
import ssl
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from queue import Queue
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from simplefix import FixMessage

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric import ed25519

_SOH_ = "\x01"
GREEN = "\033[32m"
BLUE = "\u001b[34m"
RESET = "\x1b[0m"
MAX_BUFFER_SIZE = 4096
MAX_SENDER_ID_LENGTH = 8
MIN_FIX_MESSAGE_LENGTH = 37
TRAILER_SIZE = 6
FIX_MD_URL = "tcp+tls://fix-md.binance.com:9000"
FIX_OE_URL = "tcp+tls://fix-oe.binance.com:9000"
FIX_DC_URL = "tcp+tls://fix-dc.binance.com:9000"


class FixMsgTypes:
    HEARTBEAT = "0"
    TEST_REQUEST = "1"
    LOGOUT = "5"
    LOGON = "A"
    REJECT = "3"
    NEWS = "B"


class FixTags:
    BEGIN_STRING = "8"
    BODY_LENGTH = "9"
    CHECKSUM = "10"
    MSG_SEQ_NUM = "34"
    MSG_TYPE = "35"
    SENDER_COMP_ID = "49"
    SENDING_TIME = "52"
    TARGET_COMP_ID = "56"
    TEXT = "58"
    RAW_DATA_LENGTH = "95"
    RAW_DATA = "96"
    ENCRYPT_METHOD = "98"
    HEART_BT_INT = "108"
    TEST_REQ_ID = "112"
    RESET_SEQ_NUM_FLAG = "141"
    USERNAME = "553"
    DROP_COPY_FLAG = "9406"

    RECV_WINDOW = "25000"
    MESSAGE_HANDLING = "25035"
    RESPONSE_MODE = "25036"


def _create_session(
    api_key: str,
    private_key: ed25519.Ed25519PrivateKey,
    endpoint: str,
    sender_comp_id: str,
    *,
    target_comp_id: str = "SPOT",
    fix_version: str = "FIX.4.4",
    socket_buffer_size: int = MAX_BUFFER_SIZE,
    heart_bt_int: int = 30,
    reset_seq_num_flag: bool = True,
    encrypt_method: int = 0,
    message_handling: int = 2,
    response_mode: int | None = None,
    drop_copy_flag: bool | None = None,
    recv_window: int | None = None,
) -> BinanceFixConnector:
    session = BinanceFixConnector(
        endpoint=endpoint,
        api_key=api_key,
        private_key=private_key,
        sender_comp_id=sender_comp_id,
        target_comp_id=target_comp_id,
        fix_version=fix_version,
        socket_buffer_size=socket_buffer_size,
        heart_bt_int=heart_bt_int,
        reset_seq_num_flag=reset_seq_num_flag,
        encrypt_method=encrypt_method,
        message_handling=message_handling,
        response_mode=response_mode,
        drop_copy_flag=drop_copy_flag,
    )
    session.connect()
    session.logon(recv_window=recv_window)
    return session


def create_market_data_session(
    api_key: str,
    private_key: ed25519.Ed25519PrivateKey,
    endpoint: str = FIX_MD_URL,
    sender_comp_id: str = "WATCH",
    target_comp_id: str = "SPOT",
    fix_version: str = "FIX.4.4",
    heart_bt_int: int = 30,
    message_handling: int = 2,
    recv_window: int | None = None,
) -> BinanceFixConnector:
    """
    Create a session to the FIX market data service.

    Message handling:   1->UNORDERED
                        2->SEQUENTIAL
    """
    return _create_session(
        endpoint=endpoint,
        api_key=api_key,
        private_key=private_key,
        sender_comp_id=("BMD" + sender_comp_id)[0:MAX_SENDER_ID_LENGTH],
        target_comp_id=target_comp_id,
        fix_version=fix_version,
        heart_bt_int=heart_bt_int,
        socket_buffer_size=MAX_BUFFER_SIZE,
        reset_seq_num_flag="Y",
        encrypt_method=0,
        message_handling=message_handling,
        recv_window=recv_window,
    )


def create_order_entry_session(
    api_key: str,
    private_key: ed25519.Ed25519PrivateKey,
    endpoint: str = FIX_OE_URL,
    sender_comp_id: str = "TRADE",
    target_comp_id: str = "SPOT",
    fix_version: str = "FIX.4.4",
    heart_bt_int: int = 30,
    message_handling: int = 2,
    response_mode: int = 1,
    recv_window: int | None = None,
) -> BinanceFixConnector:
    """
    Create a session to the FIX order-entry service.

    Response mode:  1->EVERYTHING
                    2->ONLY_ACKS
    Message handling:   1->UNORDERED
                        2->SEQUENTIAL
    """
    return _create_session(
        endpoint=endpoint,
        api_key=api_key,
        private_key=private_key,
        sender_comp_id=("BOE" + sender_comp_id)[0:MAX_SENDER_ID_LENGTH],
        target_comp_id=target_comp_id,
        fix_version=fix_version,
        heart_bt_int=heart_bt_int,
        socket_buffer_size=MAX_BUFFER_SIZE,
        reset_seq_num_flag="Y",
        encrypt_method=0,
        response_mode=response_mode,
        message_handling=message_handling,
        drop_copy_flag="N",
        recv_window=recv_window,
    )


def create_drop_copy_session(
    api_key: str,
    private_key: ed25519.Ed25519PrivateKey,
    endpoint: str = FIX_DC_URL,
    sender_comp_id: str = "TECH",
    target_comp_id: str = "SPOT",
    fix_version: str = "FIX.4.4",
    heart_bt_int: int = 30,
    message_handling: int = 2,
    response_mode: int = 1,
    recv_window: int | None = None,
) -> BinanceFixConnector:
    """
    Create a session to the FIX drop-copy service.

    Response mode:  1->EVERYTHING
                    2->ONLY_ACKS
    Message handling:   1->UNORDERED
                        2->SEQUENTIAL
    """
    return _create_session(
        endpoint=endpoint,
        api_key=api_key,
        private_key=private_key,
        sender_comp_id=("BDC" + sender_comp_id)[0:MAX_SENDER_ID_LENGTH],
        target_comp_id=target_comp_id,
        fix_version=fix_version,
        heart_bt_int=heart_bt_int,
        socket_buffer_size=MAX_BUFFER_SIZE,
        reset_seq_num_flag="Y",
        encrypt_method=0,
        response_mode=response_mode,
        message_handling=message_handling,
        drop_copy_flag="Y",
        recv_window=recv_window,
    )


class BinanceFixConnector:
    def __init__(
        self,
        endpoint: str,
        api_key: str,
        private_key: ed25519.Ed25519PrivateKey,
        sender_comp_id: str,
        *,
        target_comp_id: str = "SPOT",
        fix_version: str = "FIX.4.4",
        socket_buffer_size: int = MAX_BUFFER_SIZE,
        heart_bt_int: int = 30,
        reset_seq_num_flag: bool = True,
        encrypt_method: int = 0,
        message_handling: int = 2,
        response_mode: int = 1,
        drop_copy_flag: bool = False,
        restart: bool = True,
    ) -> None:
        """
        Create a fix session.

        Args:
        ----
            endpoint (str): The server endpoint
            api_key (str): The api key registered for the user
            private_key (ed25519.Ed25519PrivateKey): the ed25519 private key used to register the api key
            sender_comp_id (str): the sender id (client)
            target_comp_id (str, optional):The target id (server). Defaults to "SPOT".
            fix_version (str, optional): The fix version protocol used. Defaults to "FIX.4.4".
            socket_buffer_size (int, optional): The socket buffer when receiving messages from server. Defaults to 4096.

            heart_bt_int (int, optional): The heartbeat interval. Defaults to 30
            reset_seq_num_flag (bool, optional): The reset seq num flag. Defaults to True.

            encrypt_method (int, optional): The encrypt method. Defaults to 0 (None).
            message_handling (int, optional): The message handling. Defaults to 2 (SEQUENTIAL).
            response_mode (int, optional): The response mode. Defaults to 1 (EVERYTHING).
            drop_copy_flag (bool, optional): The drop copy flag. Defaults to False.
            restart (bool, optional): Whether to enable automatic session restart upon server notification. Defaults to True.


        Raises:
        ------
            ValueError: Raised when some mandatory arguments are not sent

        """
        error_message = ""
        if not endpoint:
            error_message += "endpoint can not be None or empty\n"
        self.endpoint = endpoint

        if not api_key:
            error_message += "api_key can not be None or empty\n"
        self.api_key = api_key

        if not private_key:
            error_message += "private_key can not be None or empty\n"
        self.private_key = private_key

        if not sender_comp_id:
            error_message += "sender_comp_id can not be None or empty\n"
        elif len(sender_comp_id) > MAX_SENDER_ID_LENGTH:
            error_message += "sender_comp_id can not be longer than 8 characters\n"
        self.sender_comp_id = str(sender_comp_id)

        if error_message:
            raise ValueError(error_message)

        self.target_comp_id = str(target_comp_id)
        self.fix_version = str(fix_version)

        self.heart_bt_int = heart_bt_int
        self.reset_seq_num_flag = reset_seq_num_flag
        self.encrypt_method = encrypt_method
        self.message_handling = message_handling
        self.response_mode = response_mode
        self.drop_copy_flag = drop_copy_flag

        self.socket_buffer_size: int = socket_buffer_size

        self.lock = threading.Lock()
        self.priv_key: ed25519.Ed25519PrivateKey = None

        self.sock = None
        self.ssl_sock = None
        self.receive_thread = None
        self.is_connected: bool = False

        self.msg_seq_num: int = 1
        self.queue_msg_received: Queue[FixMessage] = Queue()
        self.messages_sent: list[FixMessage] = []

        self.restart: bool = restart
        self.restart_flag: bool = False
        self.restart_session = None
        self.restart_timer = None
        self.restart_time = None

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            handlers=[logging.StreamHandler(stream=sys.stdout)],
        )

        self.logger = logging.getLogger("BinanceFixConnector")
        self.__data: bytes = b""

    def current_utc_time(self) -> str:
        """
        Return the current utc time which will be used for signature and fix message header.

        Returns
        -------
            - datetime in string format YYYYmmdd-HH:MM:SS.ffffff

        """
        return datetime.now(timezone.utc).strftime("%Y%m%d-%H:%M:%S.%f")

    def get_next_seq_num(self) -> str:
        """
        Return next seq num to be used for the fix message to be sent to server.

        Returns
        -------
            str: next seq num valid

        """
        with self.lock:
            self.msg_seq_num += 1
            return str(self.msg_seq_num)

    def generate_signature(
        self,
        sender_comp_id: str,
        target_comp_id: str,
        msg_seq_num: int,
        sending_time: str,
    ) -> str:
        """
        Generate the signature required to login in the server.

        Args:
        ----
            sender_comp_id (str): the sender comp id
            target_comp_id (str): the target comp id
            msg_seq_num (int): the msq seq num
            sending_time (str): the sending time

        Raises:
        ------
            ValueError: When the private key is not provided

        Returns:
        -------
            signed_signature: signature ready to be used.

        """
        if not self.private_key:
            msg = "Please provide a ed25219 key"
            raise ValueError(msg)
        signed_headers = f"A{_SOH_}{sender_comp_id}{_SOH_}{target_comp_id}{_SOH_}{msg_seq_num}{_SOH_}{sending_time}"
        signature = self.private_key.sign(bytes(signed_headers, "ASCII"))
        return base64.b64encode(signature).decode("ASCII")

    def parse_server_response(self) -> list[FixMessage]:
        """
        Parse the response from the server and create a fix message for every message serve has sent.

        Returns
        -------
            list[FixMessage]: The list of (FIX) messages server has sent.

        """
        if len(self.__data) < MIN_FIX_MESSAGE_LENGTH:
            return []
        raw_data = self.__data.decode("utf-8")
        msg = raw_data if raw_data.startswith(_SOH_) else f"{_SOH_}{raw_data}"
        raw_messages = [f"8={x}" for x in msg.split(f"{_SOH_}8=") if x]
        messages: list[FixMessage] = []
        for i in range(len(raw_messages)):
            tag_values = [
                x
                for x in raw_messages[i].split(_SOH_)
                if "=" in x and not x.startswith("=")
            ]
            if (
                len(tag_values) > 1
                and tag_values[0] == "8="
                and tag_values[1] == self.fix_version
            ):
                tag_values.pop(0)
                tag_values[0] = f"8={self.fix_version}"
            if tag_values[-1].startswith("10=") and len(tag_values[-1]) >= TRAILER_SIZE:
                fix_msg = FixMessage()
                fix_msg.append_strings(tag_values)
                messages.append(fix_msg)
            else:  # uncompleted message
                self.__data = bytes(f"{_SOH_}".join(raw_messages[i:]).encode("ASCII"))
                return messages

        self.__data = b""
        return messages

    def connect(self) -> None:
        """Create a socket connection between the client and the server."""
        try:
            if self.sock:
                self.sock.close()
                self.sock = None
            url = urlparse(self.endpoint)
            sock = socket.create_connection((url.hostname, url.port))
            context = ssl.create_default_context()
            self.sock = context.wrap_socket(sock, server_hostname=url.hostname)
            self.logger.info("-" * 100)
            self.logger.info(
                "FIX Client (%s:%s): Connected to %s",
                self.sock.getsockname()[0],
                self.sock.getsockname()[1],
                self.endpoint,
            )
            self.logger.info("-" * 100)
            self.logger.info("LOGIN (A)")
            self.is_connected = True
            if self.receive_thread is None or not self.receive_thread.is_alive():
                self.receive_thread = threading.Thread(
                    target=self.__receive_messages, daemon=True
                )
                self.receive_thread.start()

        except Exception:
            self.logger.exception("Error connecting")
            raise

    def __receive_messages(self) -> None:
        """Read the data sent from server and process the messages accordingly."""
        messages: list[FixMessage] = []
        while self.is_connected:
            try:
                data = self.sock.recv(self.socket_buffer_size)
                self.__data += data
                if not data:
                    break
                messages = self.parse_server_response()
                if messages:
                    self.__data = b""
                    for msg in messages:
                        try:
                            clean_message = (
                                msg.encode().decode("utf-8").replace(_SOH_, "|")
                            )
                        except ValueError:
                            self.logger.warning(
                                "Message decoded but could not be logged (missing MsgType: 35)"
                            )
                            continue

                        self.logger.info(
                            "%sServer=>Client: %s%s", GREEN, clean_message, RESET
                        )
                    self.on_message_received(messages)

            except OSError:
                break
            except Exception:
                self.logger.exception("Error receiving message")
                self.disconnect()
                raise

    def on_message_received(self, messages: list[FixMessage]) -> None:
        """
        Process every message received from server.

        Args:
        ----
            messages (list[FixMessage]): The messages to be processed

        """
        with self.lock:
            for msg in messages:
                self.queue_msg_received.put(msg)
        for message in messages:
            msg_type = (
                None
                if not message.get(FixTags.MSG_TYPE)
                else message.get(FixTags.MSG_TYPE).decode("utf-8")
            )
            if msg_type == FixMsgTypes.TEST_REQUEST:
                test_req_resp_id = (
                    None
                    if not message.get("112")
                    else message.get("112").decode("utf-8")
                )

                if test_req_resp_id is None:
                    self.logger.error(
                        "Error: TestReqID (112) not found in the message."
                    )
                    return
                self.logger.debug(
                    "Sending a heartbeat message as we received a TestRequest message from server"
                )
                self.heartbeat(test_req_resp_id)
            if msg_type == FixMsgTypes.NEWS:
                self.logger.info("News message received from server.")
                news_text = (
                    None if not message.get(148) else message.get(148).decode("utf-8")
                )
                self.logger.info("NewsText: %s", news_text)
                if self.restart:
                    self.schedule_restart()
            if msg_type == FixMsgTypes.LOGOUT and self.restart is False:
                self.logger.info(
                    "Logout message received from server. Closing connection."
                )
                self.logout()
                self.disconnect()

    def get_all_new_messages_received(self) -> list[FixMessage]:
        """
        Return all the FIX messages received from the server until now.
        If no new messages received, it returns [].

        Returns
        -------
            list[FixMessage]: The list of fix messages received from server.

        """
        with self.lock:
            return [
                self.queue_msg_received.get()
                for _ in range(self.queue_msg_received.qsize())
            ]

    def retrieve_messages_until(
        self,
        message_type: str | list[str],
        message_cl_ord_id: str | None = None,
        timeout_seconds: int = 3,
    ) -> list[FixMessage]:
        """Return all the FIX messages received from the server until message of desired type is received."""
        # with self.lock:
        if isinstance(message_type, str):
            message_type = [message_type]
        messages: list[FixMessage] = []
        timeout = datetime.now() + timedelta(seconds=timeout_seconds)
        while datetime.now() < timeout:
            for _ in range(self.queue_msg_received.qsize()):
                msg = self.queue_msg_received.get()
                messages.append(msg)
                if message_cl_ord_id:
                    cl_ord_id = (
                        None if not msg.get("11") else msg.get("11").decode("utf-8")
                    )
                    if cl_ord_id == message_cl_ord_id:
                        return messages
                elif message_type and msg.get("35").decode("utf-8") in message_type:
                    return messages

            time.sleep(0.001)
        return messages

    def send_message(self, message: FixMessage, *, raw: bool = False) -> None:
        """
        Send the Fix Message to the server.

        Unless 'raw' is set, this function will calculate and
        correctly set the BodyLength (9) and Checksum (10) fields, and
        ensure that the BeginString (8), Body Length (9), Message Type
        (35) and Checksum (10) fields are in the right positions.

        This function does no further validation of the message content.

        Args:
        ----
            message (FixMessage): The message
            raw (bool, optional): If True, encode pairs exactly as provided.

        """
        with self.lock:  # save the logon message for future auto_reconnects
            self.messages_sent.append(message)

        if not self.sock:
            self.logger.error("Error: No connection established. can't send message.")
            return
        try:
            self.sock.sendall(message.encode(raw))
            clean_message = message.encode().decode("utf-8").replace(chr(1), "|")
            self.logger.info("%sClient=>Server: %s%s", BLUE, clean_message, RESET)
        except Exception:
            self.logger.exception("Error sending message")

    def create_fix_message_with_basic_header(
        self,
        msg_type: str,
        recv_window: str | None = None,
    ) -> FixMessage:
        """
        Return a basic FixMessage with the mandatory headers required for a valid message.

        Args:
        ----
            msg_type (str): The msg type
            recv_window (str | None, optional): The recv window.

        Returns:
        -------
            FixMessage: the fix message ready to be filled with the body tags

        """
        msg = FixMessage()

        msg.append_pair(FixTags.BEGIN_STRING, self.fix_version, header=True)
        msg.append_pair(FixTags.MSG_TYPE, msg_type, header=True)
        msg.append_pair(FixTags.SENDER_COMP_ID, self.sender_comp_id, header=True)
        msg.append_pair(FixTags.TARGET_COMP_ID, self.target_comp_id, header=True)
        msg.append_pair(FixTags.MSG_SEQ_NUM, self.get_next_seq_num(), header=True)
        msg.append_pair(FixTags.SENDING_TIME, self.current_utc_time(), header=True)
        msg.append_pair(FixTags.RECV_WINDOW, recv_window, header=True)

        return msg

    def logon(
        self,
        recv_window: str | None = None,
    ) -> None:
        """
        Logon method.

        Args:
        ----
            recv_window (str | None, optional): The recv window. Defaults to None.

        """
        if self.restart_flag:
            self.logger.info(
                "The Server will soon restart. Can't start any new connections"
            )
        else:
            self.msg_seq_num = 0
            msg = self.create_fix_message_with_basic_header(
                FixMsgTypes.LOGON, recv_window
            )
            signature = self.generate_signature(
                self.sender_comp_id,
                self.target_comp_id,
                self.msg_seq_num,
                msg.get(FixTags.SENDING_TIME).decode("utf-8"),
            )

            msg.append_pair(FixTags.ENCRYPT_METHOD, self.encrypt_method, header=False)
            msg.append_pair(FixTags.HEART_BT_INT, self.heart_bt_int, header=False)
            msg.append_data(
                FixTags.RAW_DATA_LENGTH, FixTags.RAW_DATA, signature, header=False
            )

            msg.append_pair(
                FixTags.RESET_SEQ_NUM_FLAG, self.reset_seq_num_flag, header=False
            )

            msg.append_pair(FixTags.USERNAME, self.api_key, header=False)
            msg.append_pair(
                FixTags.MESSAGE_HANDLING, self.message_handling, header=False
            )
            msg.append_pair(FixTags.RESPONSE_MODE, self.response_mode, header=False)
            msg.append_pair(FixTags.DROP_COPY_FLAG, self.drop_copy_flag, header=False)

            self.send_message(msg)

    def logout(self, text: str | None = None, recv_window: str | None = None) -> None:
        """
        Logout method.

        Args:
        ----
            text (str | None, optional): The reason to logout. Defaults to None.
            recv_window (str | None, optional): The recv window. Defaults to None.

        """
        msg = self.create_fix_message_with_basic_header(FixMsgTypes.LOGOUT, recv_window)
        msg.append_pair(FixTags.TEXT, text, header=False)

        self.send_message(msg)

    def heartbeat(
        self, test_req_id: str | None = None, recv_window: str | None = None
    ) -> None:
        """
        Heartbeat method.

        Args:
        ----
            test_req_id (str | None, optional): The identifier for a test request. Defaults to None.
            recv_window (str | None, optional): The recv window. Defaults to None.

        """
        msg = self.create_fix_message_with_basic_header(
            FixMsgTypes.HEARTBEAT, recv_window
        )
        msg.append_pair(FixTags.TEST_REQ_ID, test_req_id, header=False)

        self.send_message(msg)

    def test_request(
        self, test_req_id: str | None = None, recv_window: str | None = None
    ) -> None:
        """
        Test request method.

        Args:
        ----
            test_req_id (str | None, optional): The identifier for a test request. Defaults to None.
            recv_window (str | None, optional): The recv window. Defaults to None.

        """
        msg = self.create_fix_message_with_basic_header(
            FixMsgTypes.TEST_REQUEST, recv_window
        )
        msg.append_pair(FixTags.TEST_REQ_ID, test_req_id, header=False)

        self.send_message(msg)

    def disconnect(self) -> None:
        """Stop the connection with the server by shuting down the socket connection."""
        self.is_connected = False
        if self.sock:
            with contextlib.suppress(OSError):
                self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()

    def schedule_restart(self) -> None:
        """Schedule the session restart in 10 minutes."""
        if not self.restart_flag:
            self.restart_flag = True
            self.restart_time = datetime.now() + timedelta(minutes=10)
            self.logger.info(f"Session restart scheduled for {self.restart_time}")

            self.restart_session = _create_session(
                api_key=self.api_key,
                private_key=self.private_key,
                endpoint=self.endpoint,
                sender_comp_id=self.sender_comp_id,
                target_comp_id=self.target_comp_id,
                fix_version=self.fix_version,
                socket_buffer_size=self.socket_buffer_size,
                heart_bt_int=self.heart_bt_int,
                reset_seq_num_flag=self.reset_seq_num_flag,
                encrypt_method=self.encrypt_method,
                message_handling=self.message_handling,
                response_mode=self.response_mode,
                drop_copy_flag=self.drop_copy_flag,
            )

            if self.restart_timer is None or not self.restart_timer.is_alive():
                self.restart_timer = threading.Thread(
                    target=self._restart_timer_thread, daemon=True
                )
                self.restart_timer.start()

    def _restart_timer_thread(self) -> None:
        """Thread that waits until restart time and then performs the restart."""
        while self.restart_flag and datetime.now() < self.restart_time:
            time.sleep(1)

        if self.restart_flag:
            self.logger.info("Performing scheduled restart...")
            self.reconnect()

    def reconnect(self) -> None:
        """Perform the actual reconnection to the new session."""
        if not self.restart_flag or not self.restart_session:
            self.logger.warning("No restart scheduled or restart session not created")
            return

        try:
            self.logger.info("Disconnecting current session...")
            self.disconnect()

            time.sleep(1)

            self.logger.info("Connecting to new session...")
            self.restart_session.connect()
            self.sock = self.restart_session.sock
            self.ssl_sock = self.restart_session.ssl_sock
            self.receive_thread = self.restart_session.receive_thread
            self.is_connected = self.restart_session.is_connected
            self.msg_seq_num = self.restart_session.msg_seq_num
            self.queue_msg_received = self.restart_session.queue_msg_received
            self.messages_sent = self.restart_session.messages_sent

            self.__dict__.update(self.restart_session.__dict__)

            self.logger.info("Restart completed successfully")
            self.restart_flag = False
            self.restart_time = None

        except Exception as e:
            self.logger.exception("Error during restart")
            self.restart_flag = False
            self.restart_time = None
            raise
