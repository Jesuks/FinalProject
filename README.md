# 高频交易快照量化因子 MapReduce 计算

本项目基于 Hadoop MapReduce，对多交易日、多股票的 LOB（限价订单簿）快照数据计算 20 个量化因子，并在同一交易日同一时刻做横截面均值聚合，输出按交易日拆分的 CSV 结果。

对应课程报告：`dfpsProject.pdf`

## 1. 任务目标

输入为多个交易日的股票快照 CSV。每条快照记录对应一个时刻，包含交易日、时间戳、全市场累计量价与前 5 档盘口数据。

需要完成：
- 对每条快照计算 20 个 LOB 因子（`alpha_1` ~ `alpha_20`）。
- 对同一交易日、同一时刻（如 `20240102,093000`）来自不同股票的因子向量做横截面均值。
- 输出 CSV，表头为 `tradeTime,alpha_1,...,alpha_20`，数值保留 6 位小数。

## 2. 数据规模与输出组织

报告中的实验规模：
- 5 个交易日。
- 每天 60+ 支股票。
- 每支股票一个 CSV，约 4900 行 × 30 列。
- 运行环境为 Docker 内 Hadoop 本地/伪分布式模式。

输出组织：
- 使用 `MultipleOutputs` 按交易日拆分文件。
- 输出文件命名为 `MMDD.csv`（例如 `0102.csv`）。
- 每个文件只写一次表头。

## 3. 整体技术方案（One-stage Aggregation）

数据流：
1. `Mapper` 解析快照并计算 20 因子。
2. 以 `tradingDay,HHMMSS` 作为 key 聚合同时刻记录。
3. `Partitioner` 按交易日分区，保证同一天数据进入同一 reducer。
4. `Reducer` 汇总求均值并按天写出 CSV。

关键点：
- 时间过滤：`tradeTime < 092957` 丢弃，`tradeTime == 092957` 仅初始化上一时刻快照，`> 092957` 才输出。
- 数值稳定：分母统一加 `epsilon=1e-7`，降低 `NaN/Inf` 风险。
- 多档计算：使用前 `N=5` 档盘口数据。

## 4. 报告中识别的性能瓶颈与优化方向

1. 小文件过多导致 Mapper 过多。
- 采用 `CombineTextInputFormat` 合并 split，减少调度与启动开销。

2. Shuffle/Sort 阶段 spill/merge 引发本地 IO 放大。
- 通过可聚合中间结果与（报告设计中的）Combiner 降低进入 reduce 的数据量。
- 指标上，`Reduce shuffle bytes` 显著下降（报告给出优化前后对比）。

3. 输出按天写表头与 reducer 并行度冲突。
- 使用 `TradingDayPartitioner` + `MultipleOutputs`，兼顾格式要求与并行性。

4. `split(",")` 与对象创建导致 GC/CPU 压力。
- 报告建议用轻量解析与缓冲区复用减少对象分配。

## 5. 代码结构（当前仓库）

- `src/main/java/aqua/CalcDriver.java`
  - 作业入口，参数配置与 `autoTune`。
  - 设置 `CombineTextInputFormat`、`Partitioner`、`Mapper/Reducer`。
  - 作业完成后重命名 `0102.csv-r-00000 -> 0102.csv`，并清理 `_SUCCESS`。

- `src/main/java/aqua/FactorMapper.java`
  - 解析输入行，做时间过滤。
  - 调用 `FactorCalculator` 计算 20 因子。
  - 输出 key=`tradingDay,HHMMSS`，value=20 因子 CSV 字符串。

- `src/main/java/aqua/FactorCalculator.java`
  - 核心因子计算逻辑。
  - 常量：`N=5`、`EPSILON=1e-7`。
  - 使用当前快照与上一快照计算静态/增量因子。

- `src/main/java/aqua/Snapshot.java`
  - 快照字段解析与存储（`tradingDay/tradeTime`、全市场字段、前 5 档 `bp/bv/ap/av`）。

- `src/main/java/aqua/TradingDayPartitioner.java`
  - 从 key 解析 `YYYYMMDD`，按 `day % numPartitions` 分区。

- `src/main/java/aqua/FactorReducer.java`
  - 对同 key 的因子向量求均值。
  - 按交易日写表头和结果到 `MMDD.csv`。

## 6. 构建与运行

### 6.1 打包

```bash
mvn -DskipTests clean package
```

产物默认在：
- `target/FinalProject-1.0-SNAPSHOT-job.jar`

### 6.2 运行示例

```bash
hadoop jar target/FinalProject-1.0-SNAPSHOT-job.jar aqua.CalcDriver <input_path> <output_path>
```

可选参数（`-D` 覆盖）：
- `aqua.reducers`
- `aqua.combine.max.mb`
- `aqua.combine.min.mb`
- `mapreduce.local.map.tasks.maximum`
- `mapreduce.local.reduce.tasks.maximum`
- `mapreduce.task.io.sort.mb`
- `mapreduce.task.io.sort.factor`

示例：

```bash
hadoop jar target/FinalProject-1.0-SNAPSHOT-job.jar aqua.CalcDriver \
  -Daqua.reducers=4 \
  -Daqua.combine.max.mb=64 \
  -Daqua.combine.min.mb=32 \
  /path/to/input /path/to/output
```

## 7. 输出格式

按天一个文件（如 `0102.csv`），内容示例：

```csv
tradeTime,alpha_1,alpha_2,...,alpha_20
093000,0.123456,-0.002341,...,1.234567
093001,0.124001,-0.002120,...,1.228800
```

## 8. 评估建议（与报告一致）

建议重点关注 Hadoop Counters：
- `Reduce shuffle bytes`
- `FILE: Number of bytes read/written`
- `Spilled Records`
- `Merged Map outputs`
- `Combine input/output records`

通过这些指标定位瓶颈（计算、GC、IO、Shuffle），再针对性优化。
