# J-Quants market data updater

Market data checks the trading calendar and compares the J-Quants V2 bulk catalog for 13 datasets, including Nikkei 225 options. It resumes from the existing private R2 snapshot, applies new or corrected source files, validates coverage and row quality, and publishes through a conditional manifest write.

An external Cloudflare scheduler dispatches `Market data / daily` at 19:30 JST on weekdays and at 04:00 JST from Tuesday through Saturday. The early slot picks up the options release and the confirmed financial statements summary. `incremental-probe` checks authentication, the trading date, the existing snapshot and the catalog in read-only mode.

Production credentials belong to the `market-data` GitHub environment for `main`. Set `MARKET_DATA_WRITER_ENABLED=true` only when this repository is the active writer. The production runner exposes fixed status fields in its log. The runner discards acquisition output and removes its private staging directory at exit.

Run the synthetic tests locally with `uv sync --locked --extra dev` and `uv run --no-sync pytest -q`.
