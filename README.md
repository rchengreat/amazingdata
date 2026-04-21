# AmazingData 数据采集项目

**用途**：通过中国银河证券星耀数智（AmazingData）SDK，从数据源拉取股票行情、财务、板块等数据，以 Parquet 文件形式落地到 NAS 共享目录，供 qa 项目的 Airflow ETL 流程使用。

**数据流向**：

```
AmazingData SDK（本项目）
        ↓  写 Parquet 文件
NAS /volume1/tgw/（SMB 共享目录）
        ↓  Airflow 容器挂载为 /opt/airflow/tgw_data
qa 项目 Airflow（ETL / 因子计算 / 模型预测）
```

---

## 目录结构

```
amazingdata/
├── README.md                  ← 本文档
├── .env                       ← 凭证与路径（从 .env.template 复制并填写）
├── .env.template              ← 环境变量模板
├── pyproject.toml             ← 依赖管理
├── wheel/                     ← SDK wheel 文件（不上传 git）
│   ├── tgw-1.0.8.7-py3-none-any.whl
│   └── AmazingData-1.1.4-cp312-none-any.whl
├── configs/
│   └── fetch_config.yaml      ← 数据类型、时间窗口、输出路径配置
├── scripts/
│   ├── fetch_kline_daily.py   ← 日K线历史数据拉取
│   ├── fetch_financial.py     ← 财务报表数据拉取
│   ├── fetch_stock_basic.py   ← 股票基本信息拉取
│   ├── fetch_margin.py        ← 融资融券数据拉取
│   ├── fetch_index.py         ← 指数数据拉取
│   ├── fetch_etf.py           ← ETF 数据拉取
│   └── subscribe_quotes.py    ← 实时行情订阅（持久进程，独立运行）
├── src/
│   └── amazingdata_fetcher/
│       ├── __init__.py
│       ├── client.py          ← SDK 登录与连接管理
│       ├── query.py           ← 批量查询封装
│       └── writer.py          ← Parquet 文件写入工具
└── logs/                      ← 本地运行日志
```

---

## 环境信息

### NAS 服务器

| 项目                        | 值                             |
| ------------------------- | ----------------------------- |
| NAS 型号                    | Synology D923+                |
| 局域网 IP                    | `192.168.100.15`               |
| Tailscale IP（外网访问）        | `100.126.211.115`             |
| SSH 用户                    | `13817878619`                 |
| SSH/sudo 密码               | `Half2@100!`                  |
| TGW 数据目录（NAS 上）           | `/volume1/tgw/`               |
| Docker 项目目录（NAS 上）        | `/volume1/docker/qa-stack/`   |
| Airflow Web UI（局域网）       | `http://192.168.100.15:8080`   |
| Airflow Web UI（Tailscale） | `http://100.126.211.115:8080` |

### MacBook Pro（开发机）

| 项目 | 值 |
|------|-----|
| 局域网 IP | `192.168.100.8` |
| NAS SMB 挂载命令 | `open "smb://13817878619@192.168.100.15/qa"` |
| SMB 挂载后本地路径 | `/Volumes/qa/` |
| TGW 目录（挂载后本地路径） | `/Volumes/tgw/`（若已挂载 tgw 共享）或通过 SSH 写入 |

> **外网访问**：MacBook 和 NAS 不在同一局域网时，启动 Tailscale，将所有 IP 地址替换为 Tailscale IP `100.126.211.115`。

### MySQL 数据库（供参考，本项目不直接写 MySQL）

| 项目 | 值 |
|------|-----|
| 局域网地址 | `192.168.100.15:3306` |
| 数据库名 | `stock_data` |
| 用户 | `admin` |
| 密码 | `aqing100` |

### AmazingData SDK 凭证

> ⚠️ 以下凭证请在收到 SDK 安装包时，从开户邮件或销售处确认后填入 `.env`。

| 项目            | 说明                   |
| ------------- | -------------------- |
| `AD_HOST`     | 101.230.159.234      |
| `AD_PORT`     | 8600                 |
| `AD_USERNAME` | 10100214892          |
| `AD_PASSWORD` | rChen2025@tgwAmazing |

---

## 安装步骤

### 第一步：创建项目目录并进入

```bash
mkdir -p ~/Desktop/Files/Projects/amazingdata
cd ~/Desktop/Files/Projects/amazingdata
```

将本 README 移入该目录：

```bash
mv ~/Desktop/Files/Projects/qa/amazingdata_README.md README.md
```

### 第二步：创建虚拟环境

本项目要求 **Python 3.12**（与 AmazingData SDK wheel 文件的 cp312 标签匹配）。

```bash
python3.12 -m venv venv
source venv/bin/activate
```

确认 Python 版本：

```bash
python --version
# 应输出 Python 3.12.x
```

### 第三步：安装 SDK wheel 文件

将从数据公司获得的两个 wheel 文件放入 `wheel/` 目录：

```
wheel/
├── tgw-1.0.8.7-py3-none-any.whl
└── AmazingData-1.1.4-cp312-none-any.whl
```

安装顺序：必须先安装 `tgw`，再安装 `AmazingData`（后者依赖前者）：

```bash
pip install wheel/tgw-1.0.8.7-py3-none-any.whl
pip install wheel/AmazingData-1.1.4-cp312-none-any.whl
```

验证安装：

```bash
python -c "import AmazingData as ad; print(ad.__version__)"
```

### 第四步：安装其他依赖

```bash
pip install pyarrow pandas python-dotenv pyyaml loguru schedule
```

或通过 pyproject.toml（创建后）：

```bash
pip install -e .
```

### 第五步：配置环境变量

复制模板并填写凭证：

```bash
cp .env.template .env
```

编辑 `.env`，填写所有 `CHANGE_ME` 项：

```bash
nano .env   # 或用任意编辑器
```

---

## 配置文件说明

### `.env.template`

```dotenv
# ── AmazingData SDK 凭证 ─────────────────────────────────────
AD_HOST=CHANGE_ME           # 数据服务器地址，例如 data.amazingdata.com.cn
AD_PORT=CHANGE_ME           # 端口，例如 9500
AD_USERNAME=CHANGE_ME       # 账号
AD_PASSWORD=CHANGE_ME       # 密码

# ── 数据输出路径 ─────────────────────────────────────────────
# 局域网环境（MacBook 已挂载 NAS SMB）：
OUTPUT_DIR=/Volumes/tgw

# 若在 NAS 本机或容器内运行：
# OUTPUT_DIR=/volume1/tgw

# 本地测试（不写 NAS，仅本地验证）：
# OUTPUT_DIR=/tmp/tgw_test

# ── 日志 ─────────────────────────────────────────────────────
LOG_LEVEL=INFO
LOG_DIR=./logs
```

### `configs/fetch_config.yaml`

```yaml
# 数据拉取配置

# 历史数据时间窗口
history:
  start_date: "2020-01-01"    # 首次全量拉取起始日期
  end_date: ""                 # 空字符串表示拉取到最新

# 增量模式：每次只拉取最近 N 天（日常增量更新）
incremental:
  days_back: 5

# 各数据类型输出子目录（相对于 OUTPUT_DIR）
output:
  kline_daily:    "kline/daily"         # 日K线
  stock_basic:    "basic"               # 股票基本信息
  financial:      "financial"           # 财务报表
  margin:         "margin"              # 融资融券
  index:          "index"               # 指数
  etf:            "etf"                 # ETF

# 文件格式
format: parquet                          # 输出格式：parquet（qa 项目可直接消费）

# 市场范围
markets:
  - SH    # 上交所
  - SZ    # 深交所
  - BJ    # 北交所（可选）
```

---

## 核心模块说明

### `src/amazingdata_fetcher/client.py` — 登录与连接管理

```python
import os
import AmazingData as ad
from dotenv import load_dotenv

load_dotenv()

def get_client():
    """登录并返回 AmazingData 客户端实例"""
    ad.login(
        username=os.environ["AD_USERNAME"],
        password=os.environ["AD_PASSWORD"],
        host=os.environ["AD_HOST"],
        port=int(os.environ["AD_PORT"]),
    )
    return ad
```

### `src/amazingdata_fetcher/query.py` — 批量查询封装

```python
import AmazingData as ad

def query_kline_daily(stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    拉取日K线数据
    stock_code: 例如 "000001.SZ"
    返回包含 open/high/low/close/volume 等字段的 DataFrame
    """
    req = ad.BaseData()
    req.set_field(["open", "high", "low", "close", "volume", "amount", "turnover"])
    req.set_filter(stock_code=stock_code, start_date=start_date, end_date=end_date)
    return req.get_data()

def query_stock_basic() -> pd.DataFrame:
    """拉取全量股票基本信息"""
    req = ad.BaseData()
    req.set_field(["stock_code", "stock_name", "list_date", "delist_date", "industry"])
    return req.get_data()
```

> 注：以上接口名称和字段名称以 AmazingData 开发手册为准，首次接入时需对照手册确认。

### `src/amazingdata_fetcher/writer.py` — Parquet 文件写入

```python
import os
import pandas as pd
from pathlib import Path

def write_parquet(df: pd.DataFrame, output_dir: str, filename: str):
    """
    将 DataFrame 写为 Parquet 文件
    output_dir: 完整路径，例如 /Volumes/tgw/kline/daily
    filename: 例如 kline_daily_20260420.parquet
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    df.to_parquet(filepath, index=False, compression="snappy")
    print(f"✓ 写入完成: {filepath} ({len(df)} 行)")
```

---

## 脚本使用示例

### 拉取日K线（增量模式，最近 5 天）

```bash
source venv/bin/activate
python scripts/fetch_kline_daily.py --mode incremental --days-back 5
```

### 拉取日K线（历史全量，首次使用）

```bash
python scripts/fetch_kline_daily.py --mode history --start 2020-01-01 --end 2026-04-20
```

### 拉取股票基本信息

```bash
python scripts/fetch_stock_basic.py
```

### 拉取财务数据

```bash
python scripts/fetch_financial.py --mode incremental
```

### 启动实时行情订阅（持久进程）

```bash
# 前台运行（调试用）
python scripts/subscribe_quotes.py

# 后台运行（生产环境）
nohup python scripts/subscribe_quotes.py > logs/subscribe.log 2>&1 &
echo $! > logs/subscribe.pid
```

停止订阅进程：

```bash
kill $(cat logs/subscribe.pid)
```

---

## 数据写入 NAS 的两种方式

### 方式 A：MacBook 直接写（推荐用于开发调试）

MacBook 通过 SMB 挂载 NAS 的 `tgw` 共享目录后，直接写文件：

```bash
# 1. 挂载 NAS（局域网）
open "smb://13817878619@192.168.100.15/tgw"
# 挂载后路径：/Volumes/tgw

# 2. .env 中设置输出路径
OUTPUT_DIR=/Volumes/tgw

# 3. 运行脚本
python scripts/fetch_kline_daily.py --mode incremental
```

> 挂载密码：`Half2@100!`

### 方式 B：SSH 到 NAS 本机运行（推荐用于定时任务）

```bash
# 登录 NAS
ssh 13817878619@192.168.100.15   # 密码：Half2@100!
# 或外网
ssh 13817878619@100.126.211.115

# 进入项目目录（首次需 git clone）
cd /volume1/amazingdata

# 激活虚拟环境并运行
source venv/bin/activate
python scripts/fetch_kline_daily.py --mode incremental
```

NAS 上的 `.env` 中设置：

```dotenv
OUTPUT_DIR=/volume1/tgw
```

---

## 定时任务配置（NAS DSM 任务计划）

在 NAS DSM 中配置每日自动拉取（推荐在 qa Airflow ETL 之前执行）：

### 配置步骤

1. 打开 DSM → **控制面板** → **任务计划**
2. 点击 **新增 → 计划的任务 → 用户定义的脚本**
3. **常规** 标签页：
   - 任务名称：`AmazingData 每日数据拉取`
   - 执行用户：`13817878619`
   - 勾选「已启用」
4. **计划** 标签页：
   - 执行频率：每天
   - 执行时间：**凌晨 03:30**（早于 qa ETL 的 05:00）
5. **任务设置** 标签页，运行命令：

```bash
cd /volume1/amazingdata && \
source venv/bin/activate && \
python scripts/fetch_kline_daily.py --mode incremental >> /volume1/amazingdata/logs/cron_kline.log 2>&1 && \
python scripts/fetch_stock_basic.py >> /volume1/amazingdata/logs/cron_basic.log 2>&1
```

### 执行时间安排建议

| 时间（北京时间） | 任务 | 说明 |
|-----------------|------|------|
| 03:30 | 本项目：增量拉取行情、基本信息 | 在 qa ETL 之前完成数据落地 |
| 05:00 | qa：`daily_etl_factor_market` | 依赖 tgw 数据到位 |
| 16:00 | qa：`daily_etl_stock_basic` | 依赖 tgw 数据到位 |
| 17:00 | qa：`daily_etl_ods` | 依赖 tgw 数据到位 |

---

## 与 qa 项目的对接

本项目写入 `/volume1/tgw/` 的文件，会被 qa 项目 Airflow 容器通过以下挂载自动消费：

```yaml
# qa 项目 nas-deploy/docker-compose.yml 中的挂载配置
volumes:
  - /volume1/tgw:/opt/airflow/tgw_data
```

qa 项目中的 `scripts/import_tgw_data.py` 会扫描 `/opt/airflow/tgw_data/` 中的 Parquet 文件并导入 MySQL。

**文件命名建议**（与 qa 现有格式保持一致）：

| 数据类型 | 目录 | 文件名示例 |
|----------|------|-----------|
| 日K线 | `/volume1/tgw/` | `kline_daily_20260420.parquet` |
| 股票基本信息 | `/volume1/tgw/` | `info_stock_basic.parquet` |
| 财务报表 | `/volume1/tgw/` | `financial_20260420.parquet` |
| 融资融券 | `/volume1/tgw/` | `margin_20260420.parquet` |

> 确认文件命名前，建议先检查 qa 项目中 `import_tgw_data.py` 的 `FILE_TYPE_MAP` 配置，确保文件名与其期望的模式匹配。

---

## 验证安装是否成功

### 测试 SDK 登录

```bash
source venv/bin/activate
python - << 'EOF'
import os
from dotenv import load_dotenv
import AmazingData as ad

load_dotenv()

ad.login(
    username=os.environ["AD_USERNAME"],
    password=os.environ["AD_PASSWORD"],
    host=os.environ["AD_HOST"],
    port=int(os.environ["AD_PORT"]),
)
print("✓ AmazingData 登录成功")
EOF
```

### 测试写入 NAS 路径

```bash
python - << 'EOF'
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
output_dir = os.environ.get("OUTPUT_DIR", "/tmp/tgw_test")
Path(output_dir).mkdir(parents=True, exist_ok=True)
test_file = os.path.join(output_dir, "test_write.txt")
with open(test_file, "w") as f:
    f.write("ok")
os.remove(test_file)
print(f"✓ 写入路径可用: {output_dir}")
EOF
```

---

## 常见问题

**Q：SDK 安装时报 `cp312` 不匹配？**  
A：确认 Python 版本为 3.12。`AmazingData-1.0.0-cp312-none-any.whl` 中 `cp312` 表示 CPython 3.12。

**Q：SMB 挂载后写文件权限拒绝？**  
A：NAS 上确认 `/volume1/tgw/` 目录的所有者和权限，登录用户（`13817878619`）需要有写权限。可通过 DSM 文件站或 SSH 确认：
```bash
ssh 13817878619@192.168.100.15 "ls -la /volume1/tgw"
```

**Q：实时订阅进程意外退出？**  
A：在 DSM 任务计划中加入守护重启逻辑，或使用 `supervisor` 管理进程。

**Q：如何确认数据文件已被 qa 项目识别？**  
A：在 Airflow Web UI（`http://192.168.100.15:8080`）手动触发 `daily_etl_ods` DAG，观察日志中 "检测到新文件" 的输出。

---

## 注意事项

1. **wheel 文件不上传 git**：`wheel/` 目录已加入 `.gitignore`，SDK 文件属于商业软件，不得公开。
2. **`.env` 不上传 git**：凭证文件已加入 `.gitignore`。
3. **实时订阅与批量拉取分开运行**：`subscribe_quotes.py` 是长连接进程，不要在批量拉取脚本中同时调用订阅接口。
4. **首次全量拉取时间较长**：建议分批次、分数据类型逐步拉取，避免长时间占用连接。

---

**创建时间**：2026-04-20  
**版本**：v1.0  
**作者**：rollandchen
