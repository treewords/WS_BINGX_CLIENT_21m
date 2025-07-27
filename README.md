# BingX Complete Streamer

This project provides a complete solution for streaming cryptocurrency market data from the BingX exchange. It handles historical data backfilling, live streaming, data aggregation, and technical indicator calculations.

## Features

*   **Historical Data Backfill**: Automatically fetches historical candle data on startup to ensure a complete dataset.
*   **Live Streaming**: Seamlessly transitions from historical backfilling to live data streaming via WebSockets.
*   **Gap Detection**: Detects and fills data gaps upon reconnection.
*   **Data Aggregation**: Aggregates 3-minute candles into larger 21-minute and 12-minute buckets.
*   **Technical Indicators**:
    *   Calculates Moving Averages (MA) with crossover detection (Golden Cross/Death Cross).
    *   Calculates Average True Range (ATR) for volatility measurement.
*   **Data Persistence**: Saves aggregated data in Parquet format, partitioned by date.
*   **State Recovery**: Persists and recovers the application state to handle restarts gracefully.
*   **Health Check**: Provides an HTTP endpoint for monitoring the application's health.
*   **Command-Line Interface**: Offers a flexible CLI for customizing the application's behavior.

## Installation

1.  Clone the repository:
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  Install the required Python packages:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

You can run the streamer with default settings:

```bash
python bingx_complete_streamer.py
```

### Command-Line Arguments

The script supports several command-line arguments to customize its behavior:

| Argument             | Description                                                   | Default      |
| -------------------- | ------------------------------------------------------------- | ------------ |
| `--symbol`           | Trading symbol to stream.                                     | `BTC-USDT`   |
| `--interval`         | Candle interval.                                              | `3m`         |
| `--history-days`     | Number of days of historical data to backfill.                | `3`          |
| `--no-backfill`      | Disable the initial historical backfill.                      | `False`      |
| `--no-gap-fill`      | Disable automatic gap filling on reconnect.                   | `False`      |
| `--output`           | Directory to save data files.                                 | `bingx_data` |
| `--debug`            | Enable debug logging.                                         | `False`      |
| `--ma-short`         | Short moving average period.                                  | `3`          |
| `--ma-long`          | Long moving average period.                                   | `7`          |
| `--no-ma`            | Disable moving average calculations.                          | `False`      |
| `--atr-period`       | ATR period.                                                   | `14`         |
| `--no-atr`           | Disable ATR calculations.                                     | `False`      |
| `--memory-mapped`    | Enable memory-mapped files for large datasets.                | `False`      |
| `--verify-bucket`    | Verify a specific bucket alignment (e.g., "YYYY-MM-DD HH:MM").|              |
| `--show-alignment`   | Show the TradingView alignment guide.                         |              |

**Example:**

```bash
python bingx_complete_streamer.py --symbol ETH-USDT --history-days 7 --ma-short 10 --ma-long 50
```

## Configuration

The core configuration is managed within the `Config` class in `bingx_complete_streamer.py`. You can modify this class to change default behaviors such as WebSocket URLs, API endpoints, and aggregation rules.

## Data Output

The streamer saves aggregated candle data in the Parquet format, which is efficient for storing and querying large datasets. The data is stored in the directory specified by the `--output` argument (default: `bingx_data/parquet`).

The data is partitioned by date, making it easy to perform time-based queries.

## Technical Indicators

### Moving Averages (MA)

The script calculates short and long-period moving averages. It also detects two types of crossovers:
*   **Golden Cross**: The short-term MA crosses above the long-term MA, often considered a bullish signal.
*   **Death Cross**: The short-term MA crosses below the long-term MA, often considered a bearish signal.

### Average True Range (ATR)

ATR is a technical analysis indicator that measures market volatility. It is calculated based on the True Range (TR) over a specified period.

## Health Check

The application runs a simple HTTP server to provide health status. The server runs on a free port, which is logged on startup. To check the health, send a GET request to the `/health` endpoint.

*   **Healthy**: Returns a `200 OK` status with `{"status": "ok"}`.
*   **Unhealthy**: Returns a `503 Service Unavailable` status with a JSON payload describing the reason for the unhealthy state.

## License

This project is open-source and available under the [MIT License](LICENSE).
