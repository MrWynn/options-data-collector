# Options Data Collector

Collects matched option market data from Binance Options and Deribit, normalizes it into per-instrument CSV files, and supports long-running background deployment on Linux servers.

## Main Features

- Discovers matched option contracts between Binance and Deribit for configured underlyings and target expiries.
- Subscribes to Binance and Deribit websocket streams for both orderbook and trade data.
- Dynamically selects Deribit BTC options nearest to +/-0.3 Delta across the third through seventh expirations.
- Saves data into per-exchange, per-date, per-type, per-instrument CSV files.
- Preserves exchange message field names in CSV output and keeps exchange timestamps as raw Unix milliseconds.
- Writes periodic sink health logs and sends a Lark notification when the process exits.
- Includes a Linux management script for `start`, `stop`, `restart`, and `status`.

## Project Layout

- `collector_main.py`: CLI entrypoint.
- `collector_runtime.py`: runtime orchestration.
- `collector_binance.py`: Binance websocket collection.
- `collector_deribit.py`: Deribit websocket collection.
- `collector_sink_csv.py`: buffered CSV sink.
- `collector_ctl.sh`: Linux background process management script.

## Requirements

- Python 3.9+
- `pip`
- Network access to Binance and Deribit REST and websocket endpoints

Install dependencies locally:

```bash
python -m pip install -r requirements.txt
```

## Local Usage

Discover matched instruments:

```bash
python collector_main.py discover --underlyings BTC
```

Run the collector in the foreground:

```bash
python collector_main.py run --underlyings BTC --output-dir data
```

Multiple underlyings:

```bash
python collector_main.py run --underlyings BTC,ETH --output-dir data
```

Run the dynamic Deribit BTC option book collector:

```bash
python collector_main.py deribit-books --output-dir data
```

This command monitors Delta for all options in the third through seventh active
BTC option expirations. It writes one snapshot per second for the nearest +0.3
Delta Call and -0.3 Delta Put in each expiration.

## Output

CSV files are written under:

```text
./data/<exchange>/<utc-date>/<data-type>/<instrument>.csv
```

Examples:

```text
data/binance/2026-04-10/trade/BTC-260417-63000-P.csv
data/deribit/2026-04-10/orderbook/BTC-17APR26-63000-P.csv
```

The `deribit-books` command writes one daily UTC file:

```text
data/deribit_options_books_2026-04-10.csv
```

## Linux Server Usage

Make the script executable:

```bash
chmod +x collector_ctl.sh
```

Start the collector in the background:

```bash
./collector_ctl.sh start
```

Stop or restart:

```bash
./collector_ctl.sh stop
./collector_ctl.sh restart
./collector_ctl.sh status
```

The script will:

- create `./venv` automatically if it does not exist
- install dependencies from `requirements.txt` if needed
- run the collector with `nohup`
- write logs to `./logs/collector.log`
- keep a pid file in `./run/collector.pid`

Useful environment overrides:

```bash
BASE_PYTHON_BIN=/usr/bin/python3 ./collector_ctl.sh start
UNDERLYINGS=BTC,ETH ./collector_ctl.sh start
OUTPUT_DIR=/home/ec2-user/app/jason/options/data ./collector_ctl.sh start
```

Run the dynamic Deribit book collector in the background:

```bash
./collector_ctl.sh start deribit-books
```

The previous environment-variable form remains supported when no command
argument is provided:

```bash
COLLECTOR_COMMAND=deribit-books ./collector_ctl.sh start
```

## Logs and Monitoring

Runtime logs go to standard output locally, or `logs/collector.log` when started via `collector_ctl.sh`.

Important log types:

- exchange connection logs
- periodic `CSV sink stats` logs
- `CSV sink failed` if the sink task crashes
- `Lark notification failed` if exit notification delivery fails

Tail the server log:

```bash
tail -f logs/collector.log
```

## Exit Notification

When the process exits normally or abnormally, it sends a Lark webhook message containing:

- command
- underlyings
- exit code
- exit reason

## Tests

Run focused tests:

```bash
python -m unittest test_collector_binance.py test_collector_lark.py
```

Run all tests:

```bash
python -m unittest
```
