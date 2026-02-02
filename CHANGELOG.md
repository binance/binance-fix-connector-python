# Changelog

## 1.2.0 - 2026-02-02

### Added
- Added a `restart` flag to the `FixConnector` connector to restart when receiving a `NEWS` message.

### Updated
- Updated `retrieve_messages_until` method to accept a list of message types.
- Fixed messages parsing issue that caused the error: `Field missing '=' separator`.

## 1.1.0 - 2025-10-27

### Updated
- Added parameters `min_price`, `max_price` and `min_price_increment` to `InstrumentList` response.

## 1.0.1 - 2025-05-08

### Removed
- Removed the references for `auto-reconnect` in the dropcopy session to fix the following [issue](https://github.com/binance/binance-fix-connector-python/issues/2).

## 1.0.0 - 2025-03-24

### Added
- First release, details in the `README.md` file