import unittest
import threading
import time

from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, PropertyMock
from binance_fix_connector.fix_connector import (
    BinanceFixConnector,
    FixMsgTypes,
    FixTags,
    _create_session,
)


class TestFixSessionRestart(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures."""
        self.api_key = "test_api_key"
        self.private_key = MagicMock()
        self.endpoint = "test.endpoint.com"
        self.sender_comp_id = "TEST123"
        self.target_comp_id = "SPOT"

        self.session = BinanceFixConnector(
            api_key=self.api_key,
            private_key=self.private_key,
            endpoint=self.endpoint,
            sender_comp_id=self.sender_comp_id,
            target_comp_id=self.target_comp_id,
            fix_version="FIX.4.4",
            heart_bt_int=30,
            message_handling=2,
        )

        self.session.sock = MagicMock()
        self.session.is_connected = True
        self.session.logger = MagicMock()

    def create_mock_message(self, msg_type, additional_fields=None):
        """Helper to create mock FIX messages."""
        mock_msg = MagicMock()
        fields = {FixTags.MSG_TYPE: msg_type.encode("utf-8")}
        if additional_fields:
            fields.update(
                {
                    k: v.encode("utf-8") if isinstance(v, str) else v
                    for k, v in additional_fields.items()
                }
            )

        def get_side_effect(tag):
            return fields.get(tag)

        mock_msg.get.side_effect = get_side_effect
        return mock_msg

    @patch("binance_fix_connector.fix_connector._create_session")
    def test_news_message_triggers_restart_schedule(self, mock_create_session):
        """Test that receiving a NEWS message schedules a restart."""
        mock_new_session = MagicMock()
        mock_create_session.return_value = mock_new_session

        news_msg = self.create_mock_message(
            FixMsgTypes.NEWS, {148: "Server restart scheduled in 10 minutes"}
        )

        with patch("binance_fix_connector.fix_connector.datetime") as mock_datetime:
            mock_now = datetime(2024, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            mock_datetime.side_effect = lambda *args, **kw: (
                datetime(*args, **kw) if args else mock_datetime
            )

            self.session.on_message_received([news_msg])

            self.assertTrue(self.session.restart_flag)
            self.assertIsNotNone(self.session.restart_time)
            self.assertIsNotNone(self.session.restart_timer)
            self.assertIsInstance(self.session.restart_timer, threading.Thread)

    @patch("binance_fix_connector.fix_connector._create_session")
    @patch("binance_fix_connector.fix_connector.threading.Thread")
    def test_multiple_news_messages_dont_create_multiple_timers(
        self, mock_thread, mock_create_session
    ):
        """Test that multiple NEWS messages don't create multiple timers."""
        mock_new_session = MagicMock()
        mock_create_session.return_value = mock_new_session

        mock_thread_instance = MagicMock()
        mock_thread.return_value = mock_thread_instance

        news_msg = self.create_mock_message(
            FixMsgTypes.NEWS, {148: "Server restart scheduled in 10 minutes"}
        )

        with patch("binance_fix_connector.fix_connector.datetime") as mock_datetime:
            mock_now = datetime(2024, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now

            self.session.on_message_received([news_msg])
            first_timer = self.session.restart_timer

            self.session.on_message_received([news_msg])
            second_timer = self.session.restart_timer

            self.assertIs(first_timer, second_timer)
            self.assertEqual(mock_thread.call_count, 1)

    @patch("binance_fix_connector.fix_connector.time.sleep")
    def test_reconnect_performs_proper_restart(self, mock_sleep):
        """Test that reconnect properly restarts the session."""
        mock_new_session = MagicMock()
        mock_new_session.connect = MagicMock()
        mock_new_session.is_connected = True
        mock_new_session.msg_seq_num = 100
        mock_new_session.sock = MagicMock()
        mock_new_session.queue_msg_received = MagicMock()
        mock_new_session.messages_sent = []

        self.session.restart_flag = True
        self.session.restart_session = mock_new_session
        self.session.msg_seq_num = 50
        self.session.disconnect = MagicMock()
        self.session.reconnect()
        self.session.disconnect.assert_called_once()

        mock_new_session.connect.assert_called_once()

        self.assertEqual(self.session.msg_seq_num, 100)
        self.assertFalse(self.session.restart_flag)
        self.assertIsNone(self.session.restart_time)

    @patch("binance_fix_connector.fix_connector._create_session")
    def test_restart_preserves_session_settings(self, mock_create_session):
        """Test that all session settings are preserved during restart."""
        self.session.heart_bt_int = 45
        self.session.message_handling = 1
        self.session.response_mode = 2
        self.session.drop_copy_flag = "Y"

        mock_new_session = MagicMock()
        mock_create_session.return_value = mock_new_session

        news_msg = self.create_mock_message(
            FixMsgTypes.NEWS, {148: "Server restart scheduled in 10 minutes"}
        )

        with patch("binance_fix_connector.fix_connector.datetime") as mock_datetime:
            mock_now = datetime(2024, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now

            self.session.on_message_received([news_msg])

            mock_create_session.assert_called_once()

            call_args = mock_create_session.call_args

            self.assertEqual(call_args.kwargs["api_key"], self.api_key)
            self.assertEqual(call_args.kwargs["private_key"], self.private_key)
            self.assertEqual(call_args.kwargs["endpoint"], self.endpoint)
            self.assertEqual(call_args.kwargs["sender_comp_id"], self.sender_comp_id)
            self.assertEqual(call_args.kwargs["heart_bt_int"], 45)
            self.assertEqual(call_args.kwargs["message_handling"], 1)
            self.assertEqual(call_args.kwargs["response_mode"], 2)
            self.assertEqual(call_args.kwargs["drop_copy_flag"], "Y")

    @patch("binance_fix_connector.fix_connector._create_session")
    @patch("binance_fix_connector.fix_connector.threading.Thread")
    def test_timer_thread_is_daemon(self, mock_thread, mock_create_session):
        """Test that the timer thread is created as a daemon."""
        mock_new_session = MagicMock()
        mock_create_session.return_value = mock_new_session
        news_msg = self.create_mock_message(
            FixMsgTypes.NEWS, {148: "Server restart scheduled in 10 minutes"}
        )

        with patch("binance_fix_connector.fix_connector.datetime") as mock_datetime:
            mock_now = datetime(2024, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now

            self.session.on_message_received([news_msg])

            mock_thread.assert_called_once()
            args, kwargs = mock_thread.call_args
            self.assertTrue(kwargs.get("daemon", False))

    @patch("binance_fix_connector.fix_connector.time.sleep")
    def test_restart_timer_triggers_reconnect(self, mock_sleep):
        """Test that the timer thread calls reconnect after 10 minutes."""
        self.session.restart_flag = True
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        future_time = base_time + timedelta(minutes=10)
        self.session.restart_time = future_time

        self.session.reconnect = MagicMock()

        with patch("binance_fix_connector.fix_connector.datetime") as mock_datetime:
            mock_datetime.now.side_effect = [
                base_time,
                future_time + timedelta(seconds=1),
            ]

            self.session._restart_timer_thread()
            self.session.reconnect.assert_called_once()

            mock_sleep.assert_called()

    @patch("binance_fix_connector.fix_connector._create_session")
    def test_news_message_does_not_trigger_restart_when_disabled(
        self, mock_create_session
    ):
        """Test that receiving a NEWS message does NOT schedule a restart when restart=False."""
        session_no_restart = BinanceFixConnector(
            api_key=self.api_key,
            private_key=self.private_key,
            endpoint=self.endpoint,
            sender_comp_id=self.sender_comp_id,
            target_comp_id=self.target_comp_id,
            fix_version="FIX.4.4",
            heart_bt_int=30,
            message_handling=2,
            restart=False,
        )

        session_no_restart.sock = MagicMock()
        session_no_restart.is_connected = True
        session_no_restart.logger = MagicMock()

        mock_new_session = MagicMock()
        mock_create_session.return_value = mock_new_session

        news_msg = self.create_mock_message(
            FixMsgTypes.NEWS, {148: "Server restart scheduled in 10 minutes"}
        )

        session_no_restart.on_message_received([news_msg])

        self.assertFalse(session_no_restart.restart_flag)
        self.assertIsNone(session_no_restart.restart_time)
        self.assertIsNone(session_no_restart.restart_timer)

        mock_create_session.assert_not_called()

        session_no_restart.logger.info.assert_any_call(
            "News message received from server."
        )

    @patch("binance_fix_connector.fix_connector._create_session")
    def test_logout_message_when_restart_disabled(self, mock_create_session):
        """Test that receiving a LOGOUT message when restart is disabled properly disconnects."""
        session_no_restart = BinanceFixConnector(
            api_key=self.api_key,
            private_key=self.private_key,
            endpoint=self.endpoint,
            sender_comp_id=self.sender_comp_id,
            target_comp_id=self.target_comp_id,
            fix_version="FIX.4.4",
            heart_bt_int=30,
            message_handling=2,
            restart=False,
        )

        session_no_restart.sock = MagicMock()
        session_no_restart.is_connected = True
        session_no_restart.logger = MagicMock()
        session_no_restart.disconnect = MagicMock()
        session_no_restart.logout = MagicMock()

        logout_msg = self.create_mock_message(FixMsgTypes.LOGOUT)

        session_no_restart.on_message_received([logout_msg])

        session_no_restart.logger.info.assert_called_with(
            "Logout message received from server. Closing connection."
        )
        session_no_restart.logout.assert_called_once()
        session_no_restart.disconnect.assert_called_once()


if __name__ == "__main__":
    unittest.main()
