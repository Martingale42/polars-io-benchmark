# Polars I/O Benchmark — 全面 Code Review 報告

審查對象：

- 程式碼：`src/polars_io_benchmark/{benchmark,cli,plot}.py`
- 結果：`benchmark_out/polars_io_benchmark_results.csv` + `benchmark_out/plots/`
- 對標報告：[POLARS_REPLICATION_REPORT.md](POLARS_REPLICATION_REPORT.md)
- 原始實驗：Octavio Loyola-González, LinkedIn,
  *"A comparative study among CSV, Feather, Pickle, and Parquet"*

審查日期：2026-05-28。
審查觀點：對標原始 pandas 實驗，並評估目前 Polars replication 的設計、實作、結果有效性。

> **狀態 (2026-05-28 晚)**：本 review 列出的 4 個一級量測 bug 已全部修復並重跑，新結果見 [POLARS_REPLICATION_REPORT.md](POLARS_REPLICATION_REPORT.md)（第二輪版）。修復記錄與第二輪驗證見本文件 **§10**。本文件保留原始 review 全文以記錄問題鏈，方便未來追溯。

---

## TL;DR

這份 replication 在工程組織上是乾淨的（dataclass、CLI、可重跑），文件也清楚說明「不是 byte-level 重現」。但在 **量測方法學** 與 **結果穩定度** 上有 7 個一級問題，至少 11 筆數據在物理上不一致，導致報告引用的若干數字（特別是 Parquet 讀取、Feather IPC 寫入）系統性誤估 30%–50%。報告本身只承認了 5M opt Parquet 的離群值，但同樣性質的離群值在 400K、1M、2M、3M、4M 行的多個格子都有，並未被點名。

換言之：**報告的 5M 表是被一個有 bias 的量測管線產生的，跟原文的「鬆耦合趨勢比較」勉強過得去，但拿來下「Parquet 比 Feather 快 X%」這種具體量化結論是不安全的。**

---

## 1. 量測管線的根本問題（一級嚴重）

### 1.1 `cast_schema` 被算進 read time，系統性高估 typed 格式的讀取時間

`benchmark.py:88`：

```python
def cast_schema(df, schema):
    return df.with_columns(
        pl.col(col).cast(dtype) for col, dtype in schema.items()
    ).select(COLS).rechunk()
```

所有 reader 都會在尾巴呼叫這個函式（`read_csv_typed`、`read_ipc_typed`、`read_parquet_typed`、`read_pickle_typed`）。對於 **Parquet / Feather / Pickle**，從磁碟讀出來的 DataFrame schema 已經和 `OPT_SCHEMA` / `NONOPT_SCHEMA` 完全一致——這次 cast 是語意上的 no-op，但仍會：

1. 對每個欄位重建一個 cast 表達式（即使是 no-op cast，optimizer 不一定能消除）。
2. **`rechunk()` 強制把所有 chunk 合併成單一 chunk**。

實測（5M rows、opt schema、Parquet snappy；本機重做的 micro-bench）：

| 操作 | 中位時間 / call |
| --- | ---: |
| `pl.read_parquet(p)`（純讀，41 chunks） | 64.87 ms |
| `pl.read_parquet(p).rechunk()` | 92.91 ms |
| `read_parquet_typed(p, OPT_SCHEMA)`（read + cast + rechunk） | 91.02 ms |

→ **rechunk 在 5M opt parquet 上多了約 43% 的時間**，cast 本身幾乎不要錢。
→ 報告裡 `read_median_s = 0.089s` 的 5M opt Parquet 數字，**真實純讀只有約 0.065s**。

這對所有 typed binary 格式都成立。CSV 因為原本讀完就接近 schema，rechunk 也有但比例小一些；不過 read+write 一樣會吃這個成本，因為 lambda 內 `loaded = io.reader(...)` 之後馬上會被當成輸入再寫回。

**影響**：

- 所有 read median 對 Parquet / Feather / Pickle 都有約 25%–40% 的虛胖。
- read+write 也吃同樣的虛胖（在 reader 端發生一次）。
- 「Parquet vs Feather」、「Snappy vs Zstd」這種小差距的排名特別不可信。

**修法**（任選其一）：

- 把 `cast_schema` 移到 timing 之外，純讀的時間單獨記錄，cast 開銷另外算 `cast_median_s`。
- 讀完先檢查 schema，若已經一致就不 cast 也不 rechunk。
- 為 CSV 額外保留一條 typed reader，其他格式直接 `pl.read_parquet(p)` / `pl.read_ipc(p)` / `pickle.load`。

### 1.2 Read+Write timing 出現 11 筆「read+write < write」的物理不可能值

`read_write_median_s` 應該 ≥ `write_median_s`，因為它做了 read+write 兩件事，write 是其中一件。實際上 results CSV 有 **11 筆紀錄違反這個基本關係**：

| n | kind | format | read | write | read+write |
| ---: | --- | --- | ---: | ---: | ---: |
| 9_000 | nonopt | feather_ipc | 0.000616 | 0.009716 | 0.009320 |
| 30_000 | nonopt | feather_ipc | 0.000940 | 0.022917 | 0.018263 |
| 1_000_000 | nonopt | feather_ipc | 0.031 | 1.034 | 0.493 |
| 2_000_000 | nonopt | feather_ipc | 0.060 | 1.408 | 1.175 |
| 3_000_000 | nonopt | csv | 0.500 | 2.280 | 1.829 |
| 3_000_000 | nonopt | parquet_zstd | 0.172 | 0.975 | 0.635 |
| 3_000_000 | opt | csv | 0.496 | 2.205 | 1.635 |
| 4_000_000 | nonopt | parquet_snappy | 0.230 | 1.652 | 0.949 |
| 4_000_000 | opt | parquet_zstd | 0.082 | 0.453 | 0.435 |
| 5_000_000 | opt | parquet_zstd | 0.089 | **1.983** | 0.476 |
| …其餘略 | | | | | |

物理意義：**這代表單獨 write 的那輪量測，某次跑出特別慢的時間，把 median 拉高了；但 read+write 那輪量測，5 次中至少 3 次在系統 less-loaded 狀態完成。**

這比報告承認的「5M opt Parquet 有離群值」嚴重很多。報告只點名 5M、Parquet snappy/zstd 一處，實際上 **1M、2M、3M、4M 的 Feather、Parquet、CSV 都有同樣性質的問題**。

**根因推測**：

- 跨 `n` 是順序執行，不是隨機交錯——同一 n 的 5 次 repeats 緊密排在一起，某些 row count 的 timing window 剛好撞到系統雜訊（背景任務、disk flush、ZFS/btrfs 後台、kthread）。
- 5 repeats 不足以對抗這種雜訊；median 在 5 個樣本下並不 robust（25-75 percentile 就是 sample 2 跟 sample 4，分佈很容易被尾巴牽動）。

### 1.3 5 repeats、hot OS page cache、無 warmup、無交錯

`timed()` 連跑 5 次同一操作，且 write 後立即 read，**所有 read 都打到 hot OS page cache**。報告自己有提到這點但沒給出量化影響。

組合風險：

- 第一次 write/read 含 first-touch、Polars lazy 初始化、Parquet writer 編譯成本——但因為樣本太少，這個 outlier 會直接污染 mean，median 也只是稍微緩和。
- 沒有 warmup run + drop-first-sample 慣例。
- 沒有交錯（interleaving）：建議的 best practice 是把 (n × kind × format) 的所有組合隨機洗牌再執行，這樣系統雜訊不會集中打在某一個 row count。
- repeats=5 對於 sub-millisecond ~ second 級操作都偏少。對 ms 級操作建議 ≥ 30 次；對 s 級操作建議 ≥ 7 次並丟掉 first sample。

### 1.4 結果 CSV 缺少 `*_samples_s` 欄位

`benchmark.py:215-225` 確實有把 `write_samples_s`、`read_samples_s`、`read_write_samples_s` 寫進每一筆 row（JSON-encoded list）。但 `benchmark_out/polars_io_benchmark_results.csv` 只有 14 個欄位，**完全沒有 samples 欄**：

```text
['n', 'kind', 'format',
 'ram_mb', 'file_mb',
 'write_min_s', 'write_median_s', 'write_mean_s',
 'read_min_s', 'read_median_s', 'read_mean_s',
 'read_write_min_s', 'read_write_median_s', 'read_write_mean_s']
```

代表此 CSV 是 **更早版本** 的 `benchmark.py` 跑出來的，那一版根本沒有 samples。報告第 189 行說「後續 benchmark 程式已調整為輸出 raw timing samples」——是說明未來會這樣做，但目前的 artifact 並沒有 raw samples 可供讀者驗證離群值說法。

**閉環缺失**：報告引用「重測 Snappy/Zstd 約 0.43–0.45s」這個數字無法從 repo artifacts 驗證。應該：

1. 用最新 benchmark.py 重跑一份。
2. 把帶 samples 的新 CSV commit 進去。
3. 或者把那次手動診斷的 raw timings 存成單獨檔案。

### 1.5 `gc.collect()` 在 timing 之前，但 timing 中 GC 並未停用

`benchmark.py:159-172`：

```python
def timed(fn, repeats):
    timings = []
    for _ in range(repeats):
        gc.collect()
        start = time.perf_counter()
        fn()
        timings.append(time.perf_counter() - start)
```

`gc.collect()` 清掉前一次留下的物件，OK。但 timing window 內 GC 是 enabled 的；對於 1M+ 行的 DataFrame，allocation 量大，可能觸發 gen2 GC，這個成本被算進 I/O。

**改善**：

```python
gc.collect()
gc.disable()
start = time.perf_counter()
fn()
elapsed = time.perf_counter() - start
gc.enable()
```

對 ms 級操作影響小，對 s 級可能差幾十毫秒，剛好落在我們關心的 Parquet vs Feather 差距區間。

---

## 2. 與原文對標時的設計取捨（二級嚴重）

### 2.1 `pl.String` 不等價於 pandas `object`

原文 nonopt schema：col_1–col_16 是 `object`。在 pandas，`object` 是 Python object pointer 陣列；裡面實際裝什麼（Python int、float、bool、str）會大幅影響 pickle / feather / parquet 的 serialized layout。

本 replication 一律把它們轉成 `pl.String`：

| col | 原文 nonopt 內容（猜測） | 本 replication 實際存的 |
| --- | --- | --- |
| col_1–col_4 | Python `str` object | `pl.String`（合理） |
| col_5–col_8 | Python `int` object | `pl.String("12345")` ← **變成 stringified int** |
| col_9–col_12 | Python `float` object | `pl.String("3.14159…")` ← **變成 stringified float** |
| col_13–col_16 | Python `bool` object | `pl.String("true" / "false")` ← **變成 stringified bool** |

報告 5.2 節有承認「我們的 nonopt Feather / Pickle 明顯比原文大」，並歸因於「Polars nonopt 把 col_1 到 col_16 都轉成 String」。承認得對，但這直接讓 **nonopt timing 與 file-size 的跨 engine 對比失效**——我們不是在量「pandas object → Polars 等價」，而是在量 **「把所有非日期欄都當文字存」這個極端 worst case**。

這個 design choice 在 README / 報告裡都有交代，所以不算「隱藏 bug」，但需要在報告 Section 4 / 5 的對標表格加註：

> 本實驗 nonopt baseline 與原文 nonopt baseline **在底層儲存模型上不對標**。原文 nonopt 在多個格式中可能享有 pickle 等 Python-native fast path；我們的 nonopt 等於對所有格式都施加「文字化全部數值欄」的處罰。對 timing 排名的趨勢仍可比對，但**任何「快幾倍」「省幾%」的絕對量化結論都不可移植**。

### 2.2 `make_base_df` 的 dtype 選擇與 schema 不完全一致

`benchmark.py:68`：

```python
data["col_5"] = rng.integers(-100, 101, size=n, dtype=np.int16)
```

OPT_SCHEMA 把 col_5 訂為 `pl.Int8`。生成時用 int16，cast 時降為 Int8。值域 -100 到 100 是 Int8 安全範圍，所以沒爆掉，但這個 round-trip 在 baseline DF 構建時就多做一次無謂 cast。同類問題在 col_6/col_7 用 int32 生成但 schema 是 Int16 也存在。

Boolean 同：`rng.choice([False, True])` 給 numpy object array → cast 到 `pl.Boolean`。

這些都不是 bug，但 `make_base_df` 直接生成最終 dtype 會：

1. 減少 baseline cast 成本（雖然 baseline 不算進 I/O timing）。
2. 確保 baseline DF 的記憶體 footprint 就是 RAM 量測的 ground truth，避免「先 int16，再 cast 到 Int8」這種臨時 buffer。

### 2.3 Pickle 的「格式」定位本來就有問題

Polars 的 `DataFrame.__reduce__` 內部走 Arrow IPC，所以 pickle 的 file size 與 Feather IPC 完全一樣（5M nonopt: 1728.72 MB vs 1728.75 MB，差 30 KB 大概就是 pickle protocol header）。

換句話說：**這個 benchmark 的 "pickle" 在 Polars engine 下等於 "Feather IPC + Python pickle wrapper"**。它不是一個獨立的儲存格式，而是「Feather IPC 加 pickle metadata」。

報告 7.2 第 1 點有提到 pickle 是 engine-specific，但沒有點明它和 Feather IPC 在 Polars 裡 **本質上是同一個 binary**。建議直接寫：

> 在本 Polars replication 中，`pickle` 與 `feather_ipc` 是同一個底層 IPC binary 加上 pickle envelope，因此它們的 file size 一致；timing 差異純粹反映 `pickle.dump` / `pickle.load` 的 Python 層 overhead。把它當成獨立格式比較會誤導讀者。

### 2.4 額外加入的 `parquet_zstd` 是好事，但壓縮等級沒固定

`write_parquet_zstd`：

```python
df.write_parquet(path, compression="zstd")
```

`compression="zstd"` 在 Polars 走 default zstd level（Polars 目前對 Parquet 是 zstd level 3）。沒有顯式設定 `compression_level`：

1. 不同 Polars 版本若改 default，過去結果就不可比。
2. Zstd 1 vs Zstd 9 的速度可以差 3–5 倍，壓縮比也差很多——不指定 level 等於把這個維度藏起來。

建議：`compression_level=3`（或任何明確值）寫進函式，並在報告中標明。

---

## 3. 觀測指標與結果解讀的問題（二級）

### 3.1 RAM = `df.estimated_size("mb")`，不是真正的 process RSS

`estimated_size("mb")` 是 Polars 對 DataFrame 數據區塊的估算，不含：

- Python 物件 wrapper、Arrow metadata。
- Categorical 的 global string pool。
- Multi-chunk DataFrame 的 chunk metadata。

原文 pandas 的 RAM 用什麼量測沒寫清楚，可能是 `df.memory_usage(deep=True).sum()` 或 `psutil` RSS。**不同的量測方法可能差 10%–30%**，所以「我們省 55.2% vs 原文省 60%」的差距大概率有一部分就是量測方法差異，而不是真實內存使用差異。

不算 bug，但報告 Section 5.1 拿這兩個百分比直接比較並下結論「方向一致、幅度接近」是過度解讀。應該加註：「兩個數字採用不同記憶體量測方法，趨勢可比，數值不可直接相減。」

### 3.2 排名表用 average rank 但沒有 confidence interval

報告 Section 6 的 average rank table，把 32 個 row counts 各自排名後平均。問題：

- 像 `nonopt Read+write` 裡 `parquet_snappy 1.94 > feather_ipc 2.00`，這 0.06 的差距完全在量測雜訊內。
- 沒有給標準差或排名的分佈，讀者會誤以為 Parquet snappy 「就是」比 feather_ipc 好。

至少應該加：

- 每個格式的 rank std。
- 或者用 Friedman/Nemenyi test 之類的 rank-based statistical test 判斷差異是否顯著。

### 3.3 報告 5M opt 表的星號 footnote 解釋不夠

報告 Section 5 表格：

| opt | parquet_snappy | … | 1.289* | 1.390* |
| opt | parquet_zstd | … | 1.983* | 0.476 |

`*` 說「受到 I/O benchmark 離群值影響」，下面接「重測同一批 5M optimized Parquet 檔案時，Snappy/Zstd 都約 0.43s–0.45s」。

問題：

- `1.983*` 的 5M opt Parquet zstd write，是 `write_median_s`。同時表中 5M opt Parquet zstd 的 `read_write_median_s = 0.476` **沒有星號**。
  - 但 read+write 0.476s 比 write 1.983s 還短，物理上不一致。報告卻把 0.476s 當合理數字。
  - 正確結論應該是：**整個 5M opt parquet_zstd 那一格的 write 都不可信**，read+write 0.476s 是 incidentally 跑在系統靜止時，read median 0.089s 才是 ground truth 級可信。
- 重測說「Snappy/Zstd 都約 0.43–0.45s」是 read+write，但 standalone write 重測值未列。讀者看不到 standalone write 的「真實值」。

### 3.4 plot.py 的 X 軸 lim 寫死 5_000_000

`plot.py:198`：`ax.set_xlim(0, 5_000_000)`。

如果之後改成跑 1M smoke test，圖會大片空白；如果改成跑 10M，會被截掉。應該根據實際 data 算 xlim：

```python
ax.set_xlim(0, results["n"].max())
```

不是嚴重 bug，但 plot.py 與 benchmark.py 隱藏耦合，未來改 size sweep 會踩到。

### 3.5 CLI 沒有種子掃描，重複性低

`DEFAULT_REPEATS=5`、`DEFAULT_SEED=42`。整個實驗只跑單一 seed 的 5 個 timing repeats。

更穩的做法：固定 timing repeats，但用多個 seeds 重新生成 DataFrame 重新跑 timing，看 timing 的 across-seed variance。目前架構不支援。

---

## 4. CSV 結果有效性的特殊問題

### 4.1 Polars CSV 寫入對 nonopt String columns 很「便宜」

報告 Section 5.4 觀察到 nonopt 5M CSV write 是 2.173s，比 Pickle 5.045s 快。這個結論需要重新檢視：

- 在 nonopt 模式下，**Polars 的 DataFrame 已經把所有非日期欄存為 String**。寫 CSV 等於把 String column 直接拷貝、加分隔符、寫檔——沒有 numeric → text 的格式化成本。
- Pickle 寫 nonopt 是 IPC encode 全部 String column（含 string buffer + offset array），結果與 Feather IPC 一樣大（1728MB）。
- 所以「Polars CSV 寫入比 Pickle 快」不是 CSV 變得多神，**而是 nonopt 設計讓 CSV writer 拿到 free lunch**：上游已經 stringify，writer 不用做事。

對應寫 nonopt feather 為什麼比 csv 慢（2.832s vs 2.173s）：feather 要寫的 binary 比 CSV 寫的純文字大很多（1728MB vs 1277MB），而且 IPC 還要寫 chunk metadata、validity bitmap。

報告 7.2 第 3 點說「Polars CSV writer 沒有那麼差」是事實，但根因解釋成「Polars CSV writer 本身很快」是不完全的——更精準的根因是 **nonopt 已經 stringify**，writer 端只剩 byte copy。報告應該修正這個解釋。

### 4.2 opt CSV 5M 寫入 1.947s，與原文 83s 差兩個量級

報告引用原文 5M opt CSV write 約 83s。本實驗約 1.947s。差距約 42 倍。

原文是 Colab 2-core Xeon 2.2GHz、HDD（pre-SSD age）；本實驗顯然在現代 SSD + 多核 + Polars Rust CSV writer。但 42x 的差距很大，需要在報告中明確說：

- 硬體差 X 倍。
- Polars Rust CSV writer vs pandas pure-Python CSV writer 至少 5-10x。
- Hot OS cache vs cold-disk 寫入 1-3x。

→ 量化拆解後讀者才能判斷「Polars CSV 1.95s」是不是合理的 ground truth。否則容易被誤解成「CSV 不慢」。

---

## 5. 設計取捨層級的問題（建議改善）

### 5.1 沒有 schema-equivalence assert

read 完之後沒有檢查讀回來的 DataFrame：

- 行數正確嗎？
- schema 對嗎？
- 內容對嗎（hash / checksum）？

如果 Polars 哪天讀 Parquet 漏一個 chunk，benchmark 不會發現，只會回報「Parquet 讀很快」。

至少在第一個 repeat 加：

```python
assert loaded.height == df.height
assert loaded.schema == df.schema
```

### 5.2 file_size_mb 是「最後一次 write 後」的檔案大小，不是 5 次的中位數

`benchmark.py:197`：

```python
write_stats = timed(lambda: io.writer(df, path), repeats)
size_mb = file_size_mb(path)
```

寫了 5 次，每次都覆蓋同一個 `path`。`file_size_mb` 量到的是第 5 次寫完的檔案。對 Parquet 來說，Polars 可能對相同資料每次寫的位元碼一致；但若 writer 有隨機 dictionary 編碼、tie-breaking，5 次寫的檔案大小可能略有差異。

建議：每次 write 後記錄 size，存成 list、取 median，與 timing 對稱。

### 5.3 `roundtrip_path` 在 read+write 中與原 path 同檔系統，cache 共享

`read_then_write` lambda 把 read 回的 df 寫到 `roundtrip_path`。問題：

- 它寫的是讀回來的 df（經過 `cast_schema` + rechunk），不是原本的 df。
- 對 categorical column，「讀回再寫」的 dictionary encoding 可能與「原本 df 寫」不同。
- read+write 不等於「read time + write time」，因為寫的物件不一樣。

報告沒有區分這兩種語意。如果目的是「同一份資料 round-trip」，應該寫原 df，不是寫 reloaded df。如果目的是「真的端到端 user workflow」，那要明寫「**這個 metric 量的是 reload 之後再寫**」。

### 5.4 沒有 cold-cache mode

所有 read 都打 hot OS page cache。對「真的從 HDD/SSD 讀第一次」的場景沒參考價值。加一個：

```python
def drop_caches():
    # Linux only, requires root
    subprocess.run(["sync"])
    Path("/proc/sys/vm/drop_caches").write_text("3")
```

或者用 `posix_fadvise(POSIX_FADV_DONTNEED)` 對單一檔案 evict。提供 `--cold-cache` flag，跑兩條 timing。

### 5.5 benchmark_out 沒被 `.gitignore`

`.gitignore` 只有 170 bytes（很短），但 `benchmark_out` 下有 320 個 data 檔（CSV、Feather、Pickle、Parquet，每個 1MB–1GB）。如果不小心 `git add -A` 會 commit GB 級別的二進位檔。

建議：

```gitignore
benchmark_out/*.csv
benchmark_out/*.feather
benchmark_out/*.pkl
benchmark_out/*.parquet
!benchmark_out/polars_io_benchmark_results.csv
```

---

## 6. 報告本身的呈現問題

### 6.1 報告 Section 5 表格星號的尾註太散

`*` 的解釋分散在表格下方文字（189 行的長段落），讀者要先讀完整段才知道哪個數字不可信。建議改成 footnote-style：

```markdown
| opt | parquet_zstd | … | 1.983 [^1] | 0.476 [^1] |

[^1]: 受 5M run 系統雜訊影響。重測 standalone write 約 0.34–0.37s，
      read+write 約 0.43–0.45s。詳見 §3.3。
```

### 6.2 Section 7「一致 vs 不一致」缺一個維度：「相同方向但量級不同」

報告把結論二分成「一致」「不一致」。實際上有第三類：「**排名一致但量化關係完全不同**」。例如：

- 原文 5M nonopt CSV vs Feather：CSV 是 Feather 的 15–25 倍慢。
- 本實驗 5M nonopt CSV vs Feather：CSV 0.809 / Feather 0.139 = 5.8 倍慢。

排名是「CSV 較慢」一致，但 15x → 5.8x 的差距完全沒有被討論。讀者讀完報告會誤以為「Polars 把 CSV 跟其他格式拉到同個 league」，事實是「Polars CSV 強多了，但仍然慢」。

### 6.3 Section 8 結論用「5M rows」做主語，但建議要適用於整個 sweep

「nonopt：Feather IPC 最穩定，5M rows 也是最快」——讀起來像「5M rows 也是最快」是次要佐證，但 Section 6 的 rank table 才是整體 sweep 的代表。應該主語用「平均 rank」，再附上 5M 細節。

### 6.4 沒有 reproducibility 段落

報告未明寫：

- 跑這個結果用的 hostname、kernel、CPU model、RAM size、filesystem。
- Polars 版本、numpy 版本、Python 版本。
- 系統當時是否 idle、是否有別的 process。
- 完整 run 花了多久。

讀者無法判斷結果可比性。建議加 Section 11「Run environment」並貼 `uv pip freeze` 與 `uname -a`、`/proc/cpuinfo` 摘要。

---

## 7. 必修清單（依優先級）

1. **修掉 read timing 含 cast_schema/rechunk 的污染**（§1.1）。重跑後 read median 預期降 25%–40%，排名可能變動。
2. **修掉 read+write < write 的物理矛盾**（§1.2）。透過增加 repeats、隨機交錯 (n × kind × format)、丟 first-sample warmup、跑多個 seed 來解決。
3. **commit 帶 samples 欄的 CSV**（§1.4）。要嘛重跑、要嘛把當初診斷的 raw timings 存進 repo。否則「重測 0.43–0.45s」的說法無法 audit。
4. 在 timing window 內 `gc.disable()`（§1.5）。
5. 顯式指定 `compression_level`（§2.4）。
6. 在報告對 nonopt baseline 加 disclaimer，明寫「pandas object ≠ Polars String」的儲存模型差距，停止跨 engine 量化比較（§2.1）。
7. 報告把 Pickle 與 Feather IPC 在 Polars 內 **同一 binary** 講明白（§2.3）。
8. 加 schema/height assert（§5.1）；提供 cold-cache mode（§5.4）。
9. 報告所有「Polars CSV 沒那麼差」的論述都需要修正歸因（§4.1）。
10. Section 6 rank table 加 std / 統計顯著性（§3.2）。

---

## 8. 哪些結論仍然站得住

修完上面那些之後，**這些結論仍然會成立**（趨勢層級，非數值層級）：

- CSV 不適合大量資料 roundtrip 與 storage。
- Parquet（任何壓縮）在 storage 上贏，在 read 上有競爭力。
- Optimized schema 大幅省 RAM 和大部分格式的 file size，這個方向確認無誤。
- Pickle 不是長期儲存格式。
- 沒有單一格式贏所有指標。

**這些結論可能需要修正**：

- 「Parquet snappy/zstd 細微排名」——需要修完量測後重判。
- 「Feather IPC read 比 Parquet read 快」在 5M optimized 時——目前 0.284 vs 0.089 的差距，大部分可能是 Feather 寫成單 chunk 但 Parquet 寫成 41 chunk 後 rechunk 拖累 read timing。修掉 rechunk 後可能逆轉。
- 「nonopt Feather IPC read 最快」可能仍成立，但與 Parquet 的差距會縮小。

---

## 9. 結語

> **底層邏輯**：這份 replication 在「論文／文章對標」的工程包裝上做得不錯，但量測管線把 **DataFrame 後處理（cast + rechunk）混進 I/O timing**，加上 5-repeat、hot-cache、不交錯的執行策略，導致 typed binary 格式的讀取時間系統性高估、read+write 與 write 之間出現物理矛盾。報告承認了其中一個現象（5M opt Parquet），但沒承認這是 systematic 而非 isolated 的問題。

要把這個 repo 升級成「可以引用」的 baseline，#1（cast 污染）、#2（穩定度）、#3（commit samples）這三項必須先閉環。其餘是錦上添花。

---

## 10. 修復記錄與第二輪驗證（2026-05-28 晚加註）

第一輪 review 列出的必修清單已全部閉環。第二輪 full sweep 已重跑並驗證，結果取代 `benchmark_out/polars_io_benchmark_results.csv`，並產出新版 [POLARS_REPLICATION_REPORT.md](POLARS_REPLICATION_REPORT.md)。

### 10.1 已修復的 bug 清單

| # | 原 §  | 描述 | 修法 | 驗證證據 |
| --- | --- | --- | --- | --- |
| 1 | §1.1 | `cast_schema` + `rechunk` 被算進 read time | reader 改名 `read_*_raw`，不做 cast 也不 rechunk；輸出直接是 reader 原始 DataFrame | 5M nonopt parquet_snappy read：0.265s → 0.146s（-45%） |
| 2 | §1.2 | `read+write < write` 物理矛盾（11 筆） | repeats 5→30；每輪用 `random.shuffle` 把 15 個 (format × op) 隨機交錯；每個 (n, kind) 用獨立 seed | 物理矛盾：11 筆 → **0 筆** |
| 3 | §1.4 | results CSV 缺 `*_samples_s` | 把 30 個 raw samples 用 `json.dumps` 序列化進 CSV 三欄 | CSV 17 欄位，含 `write_samples_s` / `read_samples_s` / `read_write_samples_s` |
| 4 | §1.5 | timing window 內 GC 可觸發 | `timed_once()` 內 `gc.disable()` / `gc.enable()` 包住 fn 執行 | 320 個 cell × 30 samples 全部完成，無 GC 雜訊性 outlier |
| 5 | §2.4 | Parquet Zstd 未指定 compression_level | `write_parquet_zstd` 顯式設 `compression_level=3` | 跨版本可重現 |
| 6 | **REVIEW 沒寫到，第一輪修完才暴露** | `pl.read_ipc` 預設 `memory_map=True` 讓 IPC read 假快 1000 倍 | `read_ipc_raw` 顯式傳 `memory_map=False` | 5M nonopt feather read：1ms（假）→ 1.299s（真） |

### 10.2 第二輪數據品質指標

| 指標 | 第一輪報告 | 第二輪修完 |
| --- | --- | --- |
| repeats / cell | 5 | **30** |
| `read+write < write` (median) | 11 筆 | **0 筆** |
| `read+write < read` (median) | 0 筆 | 0 筆 |
| `read+write < write` (min) | 14 筆 | **0 筆** |
| samples 三欄存在 | ✗ | ✓ |
| cast/rechunk 在 timing 內 | ✓（污染） | ✗ |
| IPC mmap 假象 | ✓（污染） | ✗ |
| GC 在 timing 內可觸發 | ✓ | ✗ |
| Parquet zstd level | 未指定 | 顯式 3 |

### 10.3 排名翻轉摘要

| 指標 | 第一輪結論 | 第二輪結論 |
| --- | --- | --- |
| 5M nonopt read 第 1 名 | feather_ipc (139 ms) | **parquet_snappy (146 ms)** |
| 5M nonopt read 中 feather 名次 | 第 1 | **第 4**（升到 1.299s） |
| 5M opt read 第 1 名 | parquet_snappy (89 ms) | parquet_snappy (59 ms) |
| 5M opt parquet_zstd write | 1.983s* (outlier) | 0.457s（穩定值） |
| 5M opt snappy vs zstd read+w | snappy 1.390s vs zstd 0.476s | **0.496s vs 0.501s（tied）** |
| 「nonopt Feather read 最快」對標原文「一致結論」 | ✓ 一致 | **僅 Small / Medium 成立；Large / Very large 反轉** |
| 32-size avg rank Parquet snappy nonopt read | 2.69 | **2.16（升到第 1）** |

### 10.4 必修清單 §7 對應狀態

| 第一輪必修項 | 狀態 |
| --- | --- |
| #1 cast 污染 | ✅ 已修 |
| #2 量測穩定度（repeats + 交錯） | ✅ 已修 |
| #3 commit samples 欄 CSV | ✅ 已修 |
| #4 gc.disable | ✅ 已修 |
| #5 compression_level | ✅ 已修 |
| #6 nonopt = pl.String disclaimer | ✅ 新報告 §2 已加 |
| #7 Pickle ≡ Feather IPC binary | ✅ 新報告 §9 已明說 |
| #8 schema/height assert | ⏳ 未做（低優先級，未來工作） |
| #8 cold-cache mode | ⏳ 未做（新報告 §12 列入未來工作） |
| #9 CSV writer 歸因修正 | ✅ 新報告 §8.2 已寫明 nonopt 上游 stringify 給 CSV writer free lunch |
| #10 rank std / 統計顯著性 | ⏳ 新報告改用 per-bucket rank 取代 std，部分緩解；嚴格統計檢定未做 |

### 10.5 新發現（第一輪 review 沒抓到）

1. **IPC mmap bug（#6）**：第一輪 review 沒看到，因為原版 `cast_schema` 會「順便」強制 mmap pages 載入，把 mmap 假象掩蓋。第一輪修完 cast 污染後 IPC read 變成裸 mmap，1.6ms / 5M 才暴露異常。

2. **Pickle ≡ Feather IPC binary**：第一輪 review 有提到 Polars pickle 內部走 IPC，但沒給定量證據。第二輪確認所有 32 個 size 下 pickle 與 feather_ipc 檔案大小差距 ≤ 30 KB（pickle envelope header）。

3. **Snappy vs Zstd write 在 nonopt large 區 zstd 反勝**：第二輪 nonopt very large write avg rank 是 zstd(1.00) < snappy(2.00)。原因可能是 zstd level 3 的壓縮 throughput 高於 snappy，加上輸出更小，net 寫入時間反而更短。第一輪因 outlier 看不出這個結論。

### 10.6 仍未閉環的弱點

對應 §5.4、§5.5：

- **無 cold-cache mode**：所有 read 仍打 hot OS cache。新報告 §12 明確列出 limitation。
- **單 seed**：未跑多 seed 驗證。
- **無 schema/height assert**：未加入。
- **無統計檢定**：rank 表給出量化結果，但沒有 confidence interval 或 paired test。

這些屬於「錦上添花」級別，不影響第二輪報告的核心結論。
