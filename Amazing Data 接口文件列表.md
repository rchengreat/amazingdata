

## Amazing Data需要获取的数据接口配置

- 以下表格包含了哪些数据需要从Amazing Data获取， 定义了文件命名规则，获取数据量，Amazing Data数据接口名称，自动获取运行时间。
- 数据获取后，应保存在NAS的Volume1/amazingdata/data目录，供QA项目的脚本读取。表格内的`QA项目导入命令`为QA项目的读取脚本命令，供参考。
- 文件命名规则和现有的tgw保持一致，减少QA项目代码修改。

| 文件名称                                  | 命名规则                         | 文件内容        | 数据数量 | AD接口名称                       | 运行时间      | QA项目导入命令                                                                                             |
| ------------------------------------- | ---------------------------- | ----------- | ---- | ---------------------------- | --------- | ---------------------------------------------------------------------------------------------------- |
| info_stock_basic.parquet              | 固定文件名                        | 证券基础信息      | 全量   | get_stock_basic              | 工作日 03:00 | python scripts/import_tgw_data.py --type stock_basic --file info_stock_basic.parquet                 |
| info_stock_factor.parquet             | 固定文件名                        | 后复权因子       | 全量   | BaseData.get_backward_factor | 工作日 03:15 | python scripts/import_tgw_data.py --type stock_factor --file info_stock_factor.parquet               |
| info_industry_basic_history.parquet   | 固定文件名                        | 行业指数基本信息    | 全量   | get_industry_base_info       | 工作日 03:30 | python scripts/import_tgw_data.py --type industry_basic --file info_industry_basic.parquet           |
| info_industry_detail_history.parquet  | 固定文件名                        | 行业指数成分股     | 全量   | get_industry_constituent     | 工作日 03:34 | python scripts/import_tgw_data.py --type industry_detail --file info_industry_detail_history.parquet |
| info_index_detail_history.parquet     | 固定文件名                        | 交易所指数成分股    | 全量   | get_index_constituent        | 工作日 04:00 | python scripts/import_tgw_data.py --type index_stock_detail --file info_index_detail.parquet         |
| info_index_weight_history.parquet     | 固定文件名                        | 交易所指数成分股日权重 | 全量   | get_index_weight             | 工作日 04:15 | python scripts/import_tgw_data.py --type index_weight --file info_index_weight_history.parquet       |
| equity_structure_history.parquet      | 固定文件名                        | 股本结构        | 全量   | get_equity_structure         | 工作日 04:30 | python scripts/import_tgw_data.py --type equity_structure --file equity_structure_history.parquet    |
| equity_dividend_history.parquet       | 固定文件名                        | 分红数据        | 全量   | get_dividend                 | 工作日 04:45 | python scripts/import_tgw_data.py --type equity_dividend --file equity_dividend_history.parquet      |
| finance_balance_sheet_history.parquet | 固定文件名                        | 资产负债表       | 全量   | get_balance_sheet            | 工作日 05:00 | python scripts/import_tgw_data.py --type balance_sheet --file finance_balance_sheet_history.parquet  |
| finance_cash_flow_history.parquet     | 固定文件名                        | 现金流量表       | 全量   | get_cash_flow                | 工作日 06:00 | python scripts/import_tgw_data.py --type cashflow_statement --file finance_cash_flow_history.parquet |
| finance_income_history.parquet        | 固定文件名                        | 利润表         | 全量   | get_income                   | 工作日 07:00 | python scripts/import_tgw_data.py --type income_statement --file finance_income_history.parquet      |
| extra_stock_20260420.parquet          | `extra_stock_{date}.parquet` | 股票价格        | 增量   | query_kline                  | 工作日 15:15 | python scripts/import_tgw_data.py --type price_data --file extra_stock_20260420.parquet              |
| extra_index_20260420.parquet          | `extra_index_{date}.parquet` | 指数          | 增量   | query_kline                  | 工作日 15:45 | python scripts/import_tgw_data.py --type index_data --file extra_index_20260420.parquet              |
| extra_etf_20260420.parquet            | `extra_etf_{date}.parquet`   | ETF         | 增量   | query_kline                  | 工作日 16:00 | python scripts/import_tgw_data.py --type etf_data --file extra_etf_20260420.parquet                  |
| margin_summary_history.parquet        | 固定文件名                        | 融资融券汇总      | 全量   | get_margin_summary           | 工作日 16:15 | python scripts/import_tgw_data.py --type margin_summary --file margin_summary_history.parquet        |
| margin_detail_history.parquet         | 固定文件名                        | 融资融券明细      | 全量   | get_margin_detail            | 工作日 16:30 | python scripts/import_tgw_data.py --type margin_detail --file margin_detail_history.parquet          |

## 月度数据清理

由于股票价格、指数、ETF数据为每日增量获取，所以在每个月的2日，需要对上个月获取的数据进行清理合并。合并后的文件为extra_etf_history.parquet、extra_index_history.parquet、extra_stock_history.parquet，包含对应的股票价格、指数、ETF的所有历史数据，同时删除上个月的每日文件。
