# AmazingData 数据采集项目

通过中国银河证券星耀数智（AmazingData）SDK 拉取 A 股市场数据，以 Parquet 文件形式落地到 NAS，供 `qa` 项目 Airflow ETL 流程消费。

## 数据流向

```
AmazingData SDK（本项目 Docker 容器，运行于 NAS）
        ↓  写 Parquet 文件
NAS /volume1/amazingdata/data/
        ↓  qa docker-compose 挂载为 /opt/airflow/tgw_data
qa 项目 Airflow（每日 ETL → MySQL → 因子计算 → 模型预测）
```

---

## 目录结构

```
amazingdata/
├── README.md
├── CLAUDE.md                        ← Claude Code 工作指南
├── .env                             ← 凭证与路径（不上传 git）
├── .env.template                    ← 环境变量模板
├── Dockerfile                       ← 构建 amazingdata-fetcher:latest 镜像
├── run_docker.sh                    ← NAS 手动运行脚本封装
├── pyproject.toml                   ← 依赖管理
├── deps/                            ← 离线 wheel 包（NAS 无外网，构建时 baked in）
│   └── *.whl
├── scripts/                         ← 主要数据拉取脚本
│   ├── fetch_stock_info.py          ← info_stock_basic + info_stock_factor（工作日 03:00）
│   ├── fetch_index_info.py          ← info_index_detail/weight_history（工作日 03:30）
│   ├── fetch_industry_info.py       ← info_industry_basic/detail_history（工作日 03:30）
│   ├── fetch_equity.py              ← equity_structure/dividend_history（工作日 04:30）
│   ├── fetch_finance.py             ← finance_balance/cash_flow/income_history（工作日 05:00）
│   ├── fetch_kline.py               ← extra_{stock|index|etf}_{date}.parquet（工作日 15:15）
│   ├── fetch_margin.py              ← margin_summary/detail_history（工作日 16:15）
│   ├── monthly_cleanup.py           ← 合并日 K 线文件（每月 2 日）
│   └── extract_ad_stock.ipynb       ← 参考 notebook（手动测试用，非生产）
├── dags/                            ← Airflow DAG 文件（挂载进 qa Airflow 容器）
│   ├── amazingdata_fetch_stock_info.py
│   ├── amazingdata_fetch_index_info.py
│   ├── amazingdata_fetch_industry_info.py
│   ├── amazingdata_fetch_equity.py
│   ├── amazingdata_fetch_finance.py
│   ├── amazingdata_fetch_kline.py
│   ├── amazingdata_fetch_margin.py
│   └── amazingdata_monthly_cleanup.py
└── src/
    └── amazingdata_fetcher/
        ├── client.py                ← SDK 登录封装（get_client）
        ├── writer.py                ← Parquet 写入（write_parquet，zstd 压缩）
        └── incremental.py           ← 增量合并工具（load_existing, append_new_rows 等）
```

---


## 脚本说明与调度时间

| 脚本                       | 输出文件                                                                                                               | 调度（工作日）      |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------ | ------------ |
| `fetch_equity.py`        | `equity_structure_history.parquet`<br>`equity_dividend_history.parquet`                                            | 03:30        |
| `fetch_finance.py`       | `finance_balance_sheet_history.parquet`<br>`finance_cash_flow_history.parquet`<br>`finance_income_history.parquet` | 05:00        |
| `fetch_industry_info.py` | `info_industry_basic_history.parquet`<br>`info_industry_detail_history.parquet`                                    | 14:30        |
| `fetch_index_info.py`    | `info_index_detail_history.parquet`<br>`info_index_weight_history.parquet`                                         | 15:55        |
| `fetch_stock_info.py`    | `info_stock_basic.parquet`<br>`info_stock_factor.parquet`                                                          | 17:00        |
| `fetch_kline.py`         | `extra_stock_{date}.parquet`<br>`extra_index_{date}.parquet`<br>`extra_etf_{date}.parquet`                         | 17:15        |
| `fetch_margin.py`        | `margin_summary_history.parquet`<br>`margin_detail_history.parquet`                                                | 18:00        |
| `monthly_cleanup.py`     | 合并 → `extra_{type}_history.parquet`                                                                                | 每月 2 日 01:00 |

### 增量策略说明

- **info_stock_basic**：仅拉取 `get_code_list()` 中不在已有文件里的新代码（`new_codes`）。
- **info_stock_factor**：每次全量下载宽表并覆写，因为复权因子会对历史日期溯源调整。若文件已在今日 15:30 后写入则跳过。
- **info_index_detail / info_industry_detail**：追加比已有文件 `INDATE` 更新的行。
- **info_index_weight**：追加比已有文件 `TRADE_DATE` 更新的行，逐个代码调用（规避 SDK bug）。
- **equity**:  Full download + overwrite every run using only code_list, local_path, is_local
- **finance**：使用code_list = `全量股票`, local_path = '/volume1/amazingdata/sdk_cache/infodata/', is_local = TRUE，进行增量获取
- **margin**: 使用code_list = `全量股票 - 剔除列表`, local_path = '/volume1/amazingdata/sdk_cache/infodata/', is_local = TRUE，进行增量获取
- **kline**：每天输出独立的日期文件(begin_date = end_date = today)，`monthly_cleanup.py` 月初合并为 `history` 文件。

---

## 核心模块

### `src/amazingdata_fetcher/client.py`

```python
from amazingdata_fetcher.client import get_client
get_client()  # 从 .env 读取凭证，调用 ad.login()，每个脚本启动时调用一次
```

### `src/amazingdata_fetcher/writer.py`

```python
from amazingdata_fetcher.writer import write_parquet
write_parquet(df, output_dir, "filename.parquet")
# 使用 zstd 压缩，index=False，自动创建目录，输出日志
```

### `src/amazingdata_fetcher/incremental.py`

```python
from amazingdata_fetcher.incremental import load_existing, max_date_str, append_new_rows, new_codes

existing = load_existing(path)           # 读取已有 parquet，不存在返回 None
max_dt = max_date_str(existing, "COL")   # 返回日期列最大值（YYYYMMDD 字符串）
result = append_new_rows(existing, df, "DATE_COL", max_dt)  # 追加新行
delta = new_codes(existing, all_codes, "MARKET_CODE")       # 返回新增代码列表
```

---

## 本地手动运行

### 在 NAS 上手动执行（标准命令）

```bash
ssh 13817878619@192.168.1.4
sudo /usr/local/bin/docker run --rm \
  --user 1026:100 \
  -v /volume1/amazingdata/data:/volume1/amazingdata/data \
  -v /volume1/amazingdata/sdk_cache:/volume1/amazingdata/sdk_cache \
  -v /volume1/amazingdata/logs:/app/logs \
  --env-file /volume1/amazingdata/.env \
  -e NUMBA_CACHE_DIR=/tmp/numba_cache \
  amazingdata-fetcher:latest \
  python3 scripts/fetch_stock_info.py
```

将 `fetch_stock_info.py` 替换为任意脚本名即可。

### 查看日志

日志通过 loguru 输出到 stdout，在 Airflow UI 中直接可见。NAS 手动运行时输出到终端，也挂载到 `/volume1/amazingdata/logs/`。

---

## Airflow 集成

本项目的 DAGs 挂载进 `qa` 项目的 Airflow 容器（同一台 NAS 上运行）。每个 DAG 用 `BashOperator` 调用 `docker run` 命令来运行 `amazingdata-fetcher:latest` 容器。

### 已配置的 docker-compose 挂载（qa 项目）

```yaml
# /volume1/docker/qa-stack/docker-compose.yml 中已添加：
volumes:
  - /volume1/amazingdata/project/dags:/opt/airflow/dags_ad
  - /var/run/docker.sock:/var/run/docker.sock  # 允许 BashOperator 调用 docker

environment:
  AIRFLOW__CORE__DAGS_FOLDER: '/opt/airflow/dags_qa,/opt/airflow/dags_ad'
```

### NAS 上的 git 同步设置

```bash
# NAS 上 clone amazingdata 项目
ssh 13817878619@192.168.1.4
git clone https://github.com/rchengreat/amazingdata.git /volume1/amazingdata/project

# DSM 任务计划每 10 分钟 pull（与 qa 项目同理）
# 命令: cd /volume1/amazingdata/project && git pull origin main
```

### 重启 Airflow 使挂载生效（首次配置后）

```bash
ssh 13817878619@192.168.1.4
cd /volume1/docker/qa-stack
sudo /var/packages/ContainerManager/target/usr/bin/docker compose down airflow-scheduler airflow-apiserver airflow-dag-processor
sudo /var/packages/ContainerManager/target/usr/bin/docker compose up -d airflow-scheduler airflow-apiserver airflow-dag-processor
```

---

## Docker 镜像维护

镜像 `amazingdata-fetcher:latest` 构建于 NAS 本地（无外网）。

```bash
# 重新构建镜像（在 NAS 上执行）
ssh 13817878619@192.168.1.4
cd /volume1/amazingdata
sudo /usr/local/bin/docker build -t amazingdata-fetcher:latest .

# 上传新脚本后设置权限
ssh 13817878619@192.168.1.4 "chmod 755 /volume1/amazingdata/scripts/*.py"
```

---

## 添加新数据类型

以下是添加一种新数据类型的完整步骤，以 `get_treasury_yield` 为例：

### 第 1 步：在合适的脚本中新增 fetch 函数

选择现有脚本（如 `fetch_index_info.py`）或新建脚本。函数模板：

```python
def fetch_treasury_yield(ido, output_dir: str, sdk_cache_dir: str):
    logger.info("=" * 60)
    logger.info("开始拉取 treasury_yield_history（增量：追加新日期行）")
    out_path = str(Path(output_dir) / "treasury_yield_history.parquet")
    existing = load_existing(out_path)
    max_dt = max_date_str(existing, "DATE_COL") if existing is not None else None

    df = ido.get_treasury_yield(...)   # 替换为实际 SDK 调用
    if df is None or df.empty:
        logger.error("get_treasury_yield 返回空数据")
        return
    if isinstance(df, dict):
        df = pd.concat(list(df.values()), ignore_index=True)

    result = append_new_rows(existing, df, "DATE_COL", max_dt)
    write_parquet(result, output_dir, "treasury_yield_history.parquet")
    logger.info("treasury_yield_history 写入完成")
```

规则：
- **全量覆写**（如复权因子）：直接调用 `write_parquet` 覆写，无需 `append_new_rows`
- **增量追加**（如成分、权重、财务数据）：用 `load_existing` + `max_date_str` + `append_new_rows`
- **始终**使用 `write_parquet` 而非 `df.to_parquet()`

### 第 2 步：在 `main()` 中调用新函数

```python
def main():
    ...
    fetch_treasury_yield(ido, output_dir, sdk_cache_dir)
```

### 第 3 步：更新或新建对应的 Airflow DAG

在 `dags/` 目录中的对应 DAG 文件里，DAG 已自动包含该脚本的调用，无需额外改动。若新建了独立脚本，复制任意现有 DAG 文件并修改：
- `dag_id`
- `schedule`（cron 表达式）
- `bash_command` 中的脚本名

### 第 4 步：更新 CLAUDE.md 的脚本表格

在 `CLAUDE.md` 的 `Scripts and Schedule` 表格中添加新行。

### 第 5 步：推送并验证

```bash
git add scripts/fetch_xxx.py dags/amazingdata_fetch_xxx.py CLAUDE.md README.md
git commit -m "feat: add xxx data fetch"
git push
# NAS 10 分钟内自动 pull，Airflow 自动识别新 DAG
```

---

