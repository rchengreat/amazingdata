# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

Fetches Chinese A-share market data from the AmazingData (中国银河证券星耀数智) SDK and writes Parquet files to `/volume1/amazingdata/data/` on the NAS. These files are consumed by the `qa` project's Airflow ETL pipeline. File names and schemas must match the existing `/volume1/tgw/` files exactly so the `qa` project can switch data sources without code changes.

## Infrastructure

- **NAS**: Synology D923+, IP `192.168.100.15` (LAN) / `100.126.211.115` (Tailscale)
- **SSH user**: `13817878619`, password: `Half2@100!`
- **Docker image**: `amazingdata-fetcher:latest` (built on NAS from `/volume1/amazingdata/Dockerfile`)
- **Offline deps**: `/volume1/amazingdata/deps/` — wheels pre-downloaded and baked into image (no internet on NAS)
- **File upload to NAS**: SSH/SCP doesn't work with key auth for this user; use Finder SMB (`smb://13817878619@192.168.100.15/`) or sftp

## Running Scripts on NAS

All scripts run inside Docker. The standard command (run on NAS via SSH):

```bash
sudo /usr/local/bin/docker run --rm \
  --user 1026:100 \
  -v /volume1/amazingdata/data:/volume1/amazingdata/data \
  -v /volume1/amazingdata/sdk_cache:/volume1/amazingdata/sdk_cache \
  -v /volume1/amazingdata/logs:/app/logs \
  --env-file /volume1/amazingdata/.env \
  -e NUMBA_CACHE_DIR=/tmp/numba_cache \
  amazingdata-fetcher:latest \
  python3 scripts/<script_name>.py [args]
```

`-e NUMBA_CACHE_DIR=/tmp/numba_cache` is required — without it numba fails to cache JIT functions due to read-only filesystem.

`run_docker.sh` in the project root is the DSM Task Scheduler wrapper (does not include `NUMBA_CACHE_DIR` yet — add it if running via that wrapper).

---

## Behavioral Guidelines

**Think Before Coding** — State assumptions explicitly. If multiple interpretations exist, present them. If something is unclear, ask before implementing.

**Simplicity First** — Minimum code that solves the problem. No features beyond what was asked, no abstractions for single-use code, no speculative flexibility.

**Surgical Changes** — Touch only what you must. Don't improve adjacent code, comments, or formatting. Match existing style. Remove only imports/variables made unused by your own changes.

**Goal-Driven Execution** — For multi-step tasks, state a brief plan with verifiable success criteria before starting. Ask clarifying questions before implementation rather than after mistakes.

## SDK Architecture

```python
import AmazingData as ad

ad.login(username, password, host, port)  # from client.get_client()

bdo = ad.BaseData()       # get_code_list(), get_backward_factor(), get_calendar()
ido = ad.InfoData()       # most info/history interfaces
mdo = ad.MarketData(calendar)  # query_kline()
```

**Critical SDK behaviors:**
- Most `ido` methods default to `is_local=True` which reads from a Windows local path (`D://AmazingData_local_data//`). Always pass `is_local=False` to fetch from server.
- SDK returns either a `pd.DataFrame` or a `dict{code: DataFrame}`. When dict, concat with `pd.concat(list(d.values()), ignore_index=True)`.
- `get_backward_factor` returns a wide DataFrame (index=dates, columns=stock codes). Must unstack to long format: `df.unstack().reset_index()` → columns `[instrument, datetime, backward_factor]`.
- `get_industry_base_info` and `get_margin_summary` take no `code_list` argument.
- `get_industry_constituent` needs **industry index codes** (e.g., `851783.SI`) — read from `info_industry_basic_history.parquet` `INDEX_CODE` column, NOT stock codes.
- `get_index_constituent` and `get_index_weight` need **exchange index codes** from `bdo.get_code_list('EXTRA_INDEX_A_SH_SZ')` (≈624 codes like `000300.SH`), NOT stock codes.
- `query_kline` args: `(code_list, begin_date=int(YYYYMMDD), end_date=int(YYYYMMDD), period=ad.constant.Period.day.value)`

## Scripts and Schedule

| Script | Output Files | Schedule |
|--------|-------------|----------|
| `fetch_info.py` | `info_stock_basic`, `info_stock_factor`, `info_industry_basic/detail_history`, `info_index_detail/weight_history` | 工作日 03:00 |
| `fetch_equity.py` | `equity_structure_history`, `equity_dividend_history` | 工作日 04:30/04:45 |
| `fetch_finance.py` | `finance_balance/cash_flow/income_history` | 工作日 05:00/06:00/07:00 |
| `fetch_kline.py [--type stock\|index\|etf\|all] [--date YYYYMMDD]` | `extra_stock_{date}`, `extra_index_{date}`, `extra_etf_{date}` | 工作日 15:15/15:45/16:00 |
| `fetch_margin.py` | `margin_summary_history`, `margin_detail_history` | 工作日 16:15/16:30 |
| `monthly_cleanup.py [--month YYYYMM]` | Merges daily kline files → `extra_{type}_history.parquet`, deletes daily files | 月初 2日 |

## Code Conventions

- `src/amazingdata_fetcher/client.py`: `get_client()` — call once at start of `main()`
- `src/amazingdata_fetcher/writer.py`: `write_parquet(df, output_dir, filename)` — always use this, not `df.to_parquet()` directly
- All scripts add `sys.path.insert(0, str(Path(__file__).parent.parent / "src"))` to resolve the local `amazingdata_fetcher` package
- Environment variables: `OUTPUT_DIR` (default `/volume1/amazingdata/data`), `SDK_CACHE_DIR` (default `/volume1/amazingdata/sdk_cache`)
- SDK cache (`local_path`) stores HDF5 files — requires `tables` (PyTables) package. This is included in the Docker image deps.

## Docker Image

Built on NAS. To rebuild after adding deps:
```bash
ssh 13817878619@192.168.100.15
cd /volume1/amazingdata
sudo docker build -t amazingdata-fetcher:latest .
```

After uploading new scripts via SMB, set permissions:
```bash
ssh 13817878619@192.168.100.15 "chmod 755 /volume1/amazingdata/scripts/*.py"
```
