# Polars 對標 Pandas I/O 實驗報告

本報告對照 Octavio Loyola-González 在 LinkedIn 發表的 pandas 實驗：

<https://www.linkedin.com/pulse/comparative-study-among-csv-feather-pickle-parquet-loyola-gonz%C3%A1lez>

本文使用本 repo 已完成的 Polars 結果：

```text
benchmark_out/polars_io_benchmark_results.csv
benchmark_out/plots/
```

這份報告的重點不是把 pandas 秒數和 Polars 秒數做硬性比較。原文是在 Google Colab 的 2-core Xeon 2.2GHz、13GB RAM、33GB HDD 環境執行；本次 Polars 結果是在目前 `cy-server` 環境執行，硬體、檔案系統、OS page cache、Polars runtime、Parquet codec 都不同。因此比較重點是：

- 實驗流程是否對標。
- 觀測指標是否對標。
- 趨勢、排名、格式特性是否對標。
- Polars engine 下哪些結論和 pandas 原文一致，哪些不同。

## 1. 原始 pandas 實驗流程

原文研究問題是比較 CSV、Feather、Pickle、Parquet 這些檔案格式在 DataFrame I/O 上的表現。原文觀測：

- loading time：讀取檔案成 DataFrame 的時間。
- saving time：DataFrame 寫出成檔案的時間。
- loading + saving time：讀取後再寫出的總時間。
- storage usage：檔案落地後佔用的 HDD 空間。
- RAM usage：DataFrame 在記憶體中的用量。

原文資料設計：

| 欄位 | 邏輯資料 | optimized pandas dtype |
| --- | --- | --- |
| `col_1` - `col_4` | 字串 / 類別 | `category` |
| `col_5` | 整數 | `int8` |
| `col_6` - `col_7` | 整數 | `int16` |
| `col_8` | 整數 | `int32` |
| `col_9` - `col_12` | 浮點數 | `float32` |
| `col_13` - `col_16` | 布林值 | `bool` |
| `col_17` - `col_20` | 日期時間 | `datetime64[ns]` |

原文的 non-optimized DataFrame 在 experimental setup 裡定義為：

| 欄位 | non-optimized pandas dtype |
| --- | --- |
| `col_1` - `col_16` | `object` |
| `col_17` - `col_20` | `datetime64[ns]` |

原文 row count sweep：

| 區間 | row count |
| --- | --- |
| Interval 1 | `1_000` 到 `10_000`，step `1_000` |
| Interval 2 | `20_000` 到 `100_000`，step `10_000` |
| Interval 3 | `200_000` 到 `1_000_000`，step `100_000` |
| Interval 4 | `2_000_000` 到 `5_000_000`，step `1_000_000` |

合計 32 個 row counts。每個 row count 都分別測 non-optimized 與 optimized DataFrame。

## 2. 我們的 Polars 復現流程

我們的目標是做 Polars-native replication，而不是把 pandas 的 `object` 行為逐 byte 複製過來。原因是 Polars 的核心是 typed columnar engine；`pl.Object` 是 Python object escape hatch，不適合拿來代表 Polars 的一般 I/O 路徑。

因此我們使用：

| 原文 pandas schema | Polars replication schema |
| --- | --- |
| `object` non-optimized | `pl.String` |
| `category` | `pl.Categorical` |
| `int8` / `int16` / `int32` | `pl.Int8` / `pl.Int16` / `pl.Int32` |
| `float32` | `pl.Float32` |
| `bool` | `pl.Boolean` |
| `datetime64[ns]` | `pl.Datetime("ns")` |

我們的 Polars schema：

| 欄位 | nonopt Polars dtype | opt Polars dtype |
| --- | --- | --- |
| `col_1` - `col_4` | `String` | `Categorical` |
| `col_5` | `String` | `Int8` |
| `col_6` - `col_7` | `String` | `Int16` |
| `col_8` | `String` | `Int32` |
| `col_9` - `col_12` | `String` | `Float32` |
| `col_13` - `col_16` | `String` | `Boolean` |
| `col_17` - `col_20` | `Datetime(ns)` | `Datetime(ns)` |

本 repo 的 `--full` sweep 和原文 row count 對齊：

```text
1_000, 2_000, ..., 10_000
20_000, 30_000, ..., 100_000
200_000, 300_000, ..., 1_000_000
2_000_000, 3_000_000, 4_000_000, 5_000_000
```

本次結果 CSV 有 320 筆資料：

```text
32 row counts x 2 schema kinds x 5 formats = 320 rows
```

我們測的格式：

| format | 說明 |
| --- | --- |
| `csv` | Polars CSV |
| `feather_ipc` | Polars Arrow IPC / Feather |
| `parquet_snappy` | Parquet with Snappy compression |
| `parquet_zstd` | Parquet with Zstandard compression |
| `pickle` | Python pickle 序列化 Polars DataFrame |

和原文相比，多出一個 `parquet_zstd`。這是刻意保留的 Polars 實務比較，因為 Zstandard 常是現代 columnar storage 的好選項。對標原文時，`parquet_snappy` 比較接近常見 pandas / pyarrow Parquet 情境。

## 3. 觀測數據對標

| 原文指標 | 我們的欄位 | 說明 |
| --- | --- | --- |
| loading time | `read_median_s` | 讀取檔案並 cast 回指定 schema 的中位數時間 |
| saving time | `write_median_s` | 寫出檔案的中位數時間 |
| loading + saving time | `read_write_median_s` | 讀取後再寫出 roundtrip 的中位數時間 |
| storage usage | `file_mb` | 實際檔案大小，MB |
| RAM usage | `ram_mb` | `df.estimated_size("mb")` |

本 repo 同時保留 `min`、`median`、`mean`，但報告主要使用 `median`，因為 I/O benchmark 容易受短暫系統負載與 OS cache 影響。

## 4. 原文結果摘要

原文在 pandas 中得到的主要結果：

| 情境 | 原文結果摘要 |
| --- | --- |
| non-optimized loading | Feather 最快，Parquet 類似，Pickle 也可用，CSV 最差 |
| non-optimized saving | Feather 最快，Parquet 接近，Pickle 較慢，CSV 最差 |
| non-optimized loading + saving | Feather 最快，Parquet 接近，Pickle 再後，CSV 最差 |
| non-optimized storage | Parquet 最省空間，Feather 第二，Pickle 第三，CSV 最大 |
| optimized loading | Pickle 最快，Feather 第二，Parquet 第三，CSV 最差 |
| optimized saving | Pickle 最快，Feather 第二，Parquet 第三，CSV 最差 |
| optimized loading + saving | Pickle 最快，Feather 第二，Parquet 第三，CSV 最差 |
| optimized storage | 大資料量時 Parquet 最省，Feather 接近；CSV 仍差 |
| RAM usage | 格式不影響 RAM；schema optimized 才影響 RAM |

原文的 5M rows 重點數字：

| 指標 | 原文 5M rows 結果 |
| --- | --- |
| non-optimized loading | Feather 約 2.3 秒，CSV 超過其他格式 15 倍以上 |
| non-optimized saving | Feather 約 3.98 秒，CSV 超過 Feather 25 倍以上 |
| non-optimized loading + saving | Feather 約 6.28 秒，CSV 超過 Feather 22 倍以上 |
| non-optimized storage | Parquet 約 370MB，Feather 約 594MB，Pickle 約 714MB，CSV 超過 1106MB |
| optimized loading | Pickle / Feather / Parquet 約 1 秒級，CSV 約 28 秒 |
| optimized saving | Pickle / Feather 約 1 秒，Parquet 約 2.7 秒，CSV 約 83 秒 |
| optimized loading + saving | Pickle 約 1 秒，Feather 約 1.5 秒，Parquet 約 3.7 秒，CSV 約 112 秒 |
| optimized storage saving | Pickle 約省 56%，Feather 約省 51%，Parquet 約省 23%，CSV 約省 11% |
| optimized RAM saving | 5M rows 約省 60% RAM |

原文最後建議：

- 避免 CSV，尤其在大量資料與反覆 I/O 場景。
- non-optimized pandas DataFrame：Feather、Parquet、Pickle 較佳。
- optimized pandas DataFrame：Pickle、Feather、Parquet 較佳。
- 沒有單一格式在所有指標都勝出。
- 正確 dtype optimization 對 RAM 與 storage 很重要。

## 5. 我們的 Polars 結果總覽

本次 Polars full run 的 5M rows 結果：

| kind | format | RAM MB | File MB | Read median s | Write median s | Read+write median s |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| nonopt | csv | 781.6 | 1277.5 | 0.809 | 2.173 | 4.563 |
| nonopt | feather_ipc | 781.6 | 1728.7 | 0.139 | 2.832 | 2.949 |
| nonopt | parquet_snappy | 781.6 | 546.7 | 0.265 | 1.032 | 1.297 |
| nonopt | parquet_zstd | 781.6 | 384.4 | 0.273 | 0.820 | 1.141 |
| nonopt | pickle | 781.6 | 1728.7 | 2.293 | 5.045 | 11.437 |
| opt | csv | 350.5 | 1111.1 | 0.805 | 1.947 | 2.785 |
| opt | feather_ipc | 350.5 | 350.5 | 0.284 | 0.544 | 0.826 |
| opt | parquet_snappy | 350.5 | 291.6 | 0.089 | 1.289* | 1.390* |
| opt | parquet_zstd | 350.5 | 263.2 | 0.089 | 1.983* | 0.476 |
| opt | pickle | 350.5 | 350.5 | 0.685 | 1.021 | 1.734 |

`*` 表示這次 full run 在 5M optimized Parquet 的 standalone write / roundtrip 指標出現不一致，後續診斷顯示這些欄位受到 I/O benchmark 離群值影響，不適合直接拿來做 Parquet Snappy vs Zstd 的結論。診斷重測同一批 5M optimized Parquet 檔案時：

| format | read samples | write-loaded samples | read+write samples |
| --- | --- | --- | --- |
| parquet_snappy | 約 `0.085s` - `0.099s` | 約 `0.340s` - `0.367s` | 約 `0.432s` - `0.452s` |
| parquet_zstd | 約 `0.091s` - `0.153s` | 約 `0.339s` - `0.367s` | 約 `0.433s` - `0.446s` |

因此更合理的解讀是：在目前機器與熱 cache 條件下，5M optimized Parquet Snappy / Zstd 的 roundtrip 都約在 `0.43s` - `0.45s` 區間；full run 表中的 `1.289s`、`1.390s`、`1.983s` 不應被當成穩定值。後續 benchmark 程式已調整為輸出 raw timing samples，方便追蹤這類離群值。

### 5.1 RAM usage

Polars 結果和原文一致：RAM usage 與檔案格式無關，主要由 DataFrame schema 決定。

在 5M rows：

| schema | RAM MB |
| --- | ---: |
| nonopt | 781.6 |
| opt | 350.5 |

optimized schema 約省下：

```text
55.2% RAM
```

這和原文「optimized DataFrame 大幅節省 RAM」的結論一致。差異是原文 pandas 約省 60%，我們 Polars 約省 55.2%。差異合理，因為 pandas `object` 與 Polars `String` 的記憶體模型不同；Polars nonopt baseline 已經是 typed string column，不是 pandas Python object pointer container。

![Non-optimized RAM usage](benchmark_out/plots/nonopt_ram_mb.png)

![Optimized RAM usage](benchmark_out/plots/opt_ram_mb.png)

### 5.2 Storage usage

5M rows 時，optimized 對 nonopt 的檔案大小改善：

| format | nonopt MB | opt MB | opt file saving |
| --- | ---: | ---: | ---: |
| feather_ipc | 1728.7 | 350.5 | 79.7% |
| pickle | 1728.7 | 350.5 | 79.7% |
| parquet_snappy | 546.7 | 291.6 | 46.7% |
| parquet_zstd | 384.4 | 263.2 | 31.5% |
| csv | 1277.5 | 1111.1 | 13.0% |

對標原文：

- 一致：CSV 在 storage 上仍然差，optimized 對 CSV 的改善最小。
- 一致：Parquet 是最省空間的主力格式。
- 一致：Feather / Pickle 對 schema optimization 非常敏感，因為 typed/categorical schema 會大幅縮小輸出。
- 不同：我們的 nonopt Feather / Pickle 明顯比原文大。主因是 Polars nonopt 把 `col_1` 到 `col_16` 都轉成 `String`，數字和布林也變成文字；這會讓 IPC / Pickle 儲存大量字串資料。原文 pandas nonopt 使用 `object`，但實際 serialized layout 與 pandas/pyarrow 版本、object 內容、寫入 API 有關，不能和 Polars 字串檔案大小直接等價。

![Non-optimized file size](benchmark_out/plots/nonopt_file_mb.png)

![Optimized file size](benchmark_out/plots/opt_file_mb.png)

### 5.3 Loading time

5M rows loading 排名：

| kind | 排名 |
| --- | --- |
| nonopt | `feather_ipc` < `parquet_snappy` < `parquet_zstd` < `csv` < `pickle` |
| opt | `parquet_snappy` < `parquet_zstd` < `feather_ipc` < `pickle` < `csv` |

5M rows optimized / nonopt loading 改善：

| format | nonopt s | opt s | nonopt / opt |
| --- | ---: | ---: | ---: |
| pickle | 2.293 | 0.685 | 3.35x |
| parquet_zstd | 0.273 | 0.089 | 3.06x |
| parquet_snappy | 0.265 | 0.089 | 2.99x |
| csv | 0.809 | 0.805 | 1.00x |
| feather_ipc | 0.139 | 0.284 | 0.49x |

對標原文：

- nonopt：原文 Feather loading 最快；我們也是 Feather IPC loading 最快。
- optimized：原文 Pickle 最快；我們在 5M rows 是 Parquet 最快，Pickle 不是最快。
- CSV：原文 CSV loading 最差；我們 optimized CSV 也是最差，nonopt CSV 不是最後一名但仍明顯落後 Feather / Parquet。

解釋：

- Polars 對 Parquet 的讀取路徑非常強，且 typed optimized schema 對 Parquet 解碼非常有利。
- CSV 在讀取時仍需 parse text，因此即使 Polars CSV 很快，typed binary formats 還是更適合大量資料。
- Pickle 在 pandas 原文中很受益於 optimized pandas object serialization；但在本實驗中是 pickle Polars DataFrame，不代表 pandas Pickle 的同一條 fast path。

![Non-optimized read median](benchmark_out/plots/nonopt_read_median_s.png)

![Optimized read median](benchmark_out/plots/opt_read_median_s.png)

### 5.4 Saving time

5M rows saving 排名：

| kind | 排名 |
| --- | --- |
| nonopt | `parquet_zstd` < `parquet_snappy` < `csv` < `feather_ipc` < `pickle` |
| opt | `feather_ipc` < `pickle` < `parquet_snappy` < `csv` < `parquet_zstd`，但 5M optimized Parquet write 有離群值，這個排序不宜過度解讀 |

5M rows optimized / nonopt saving 改善：

| format | nonopt s | opt s | nonopt / opt |
| --- | ---: | ---: | ---: |
| feather_ipc | 2.832 | 0.544 | 5.20x |
| pickle | 5.045 | 1.021 | 4.94x |
| csv | 2.173 | 1.947 | 1.12x |
| parquet_snappy | 1.032 | 1.289* | 0.80x* |
| parquet_zstd | 0.820 | 1.983* | 0.41x* |

對標原文：

- 原文 nonopt saving 是 Feather 最快，CSV 最差。
- 我們 nonopt saving 是 Parquet 最快，Pickle 最差；CSV 在 Polars 裡不再是最慢。
- 原文 optimized saving 是 Pickle 最快，Feather 第二。
- 我們 optimized saving 是 Feather 最快，Pickle 第二。

解釋：

- Polars 的 CSV writer 很快，所以 CSV 在 saving 指標上不像 pandas 原文那樣災難。
- Polars Parquet writer 對 nonopt `String` columns 的處理很有效，尤其 compression 後寫入時間和檔案大小都不差。
- 5M optimized Parquet 的 standalone write median 在這次 full run 中受到離群值影響。針對同一批檔案重測時，Snappy 與 Zstd 的 write-loaded 都約 `0.34s` - `0.37s`，因此不能用表中的 `1.289s` 與 `1.983s` 斷言 Zstd write 比 Snappy 慢很多。

![Non-optimized write median](benchmark_out/plots/nonopt_write_median_s.png)

![Optimized write median](benchmark_out/plots/opt_write_median_s.png)

### 5.5 Loading + saving time

5M rows read + write 排名：

| kind | 排名 |
| --- | --- |
| nonopt | `parquet_zstd` < `parquet_snappy` < `feather_ipc` < `csv` < `pickle` |
| opt | `parquet_zstd` < `feather_ipc` < `parquet_snappy` < `pickle` < `csv` |

5M rows optimized / nonopt read+write 改善：

| format | nonopt s | opt s | nonopt / opt |
| --- | ---: | ---: | ---: |
| pickle | 11.437 | 1.734 | 6.60x |
| feather_ipc | 2.949 | 0.826 | 3.57x |
| parquet_zstd | 1.141 | 0.476 | 2.40x |
| csv | 4.563 | 2.785 | 1.64x |
| parquet_snappy | 1.297 | 1.390* | 0.93x* |

對標原文：

- 原文 nonopt read+write 是 Feather 最快；我們是 Parquet Zstd / Snappy 最快。
- 原文 optimized read+write 是 Pickle 最快；我們的 full run 表面上是 Parquet Zstd 最快，但 5M optimized Parquet Snappy 的 roundtrip 欄位也受到離群值影響。重測顯示 Snappy / Zstd roundtrip 都約 `0.43s` - `0.45s`。
- 一致的是：optimized schema 通常改善 read+write，CSV 仍不適合做大量資料 roundtrip。

這個差異是本次 replication 最重要的結果之一：在 Polars engine 下，Parquet 的 read/write roundtrip 表現比 pandas 原文更突出，尤其是 optimized typed schema。不過 Snappy 與 Zstd 的相對名次需要用 raw samples 或重新多輪測試確認，不能只看這次 full run 的單一 median 表。

![Non-optimized read+write median](benchmark_out/plots/nonopt_read_write_median_s.png)

![Optimized read+write median](benchmark_out/plots/opt_read_write_median_s.png)

## 6. 32 個 row counts 的整體排名

以下排名不是只看 5M rows，而是對 32 個 row counts 分別排序後取平均 rank。數字越低越好。

| schema | metric | best to worst by average rank across 32 row counts |
| --- | --- | --- |
| nonopt | Read | feather_ipc (1.00) > parquet_snappy (2.69) > pickle (3.28) > parquet_zstd (3.59) > csv (4.44) |
| nonopt | Write | parquet_snappy (1.47) > parquet_zstd (2.03) > csv (3.50) > feather_ipc (3.56) > pickle (4.44) |
| nonopt | Read+write | parquet_snappy (1.94) > feather_ipc (2.00) > parquet_zstd (2.59) > pickle (4.22) > csv (4.25) |
| nonopt | File size | parquet_zstd (1.00) > parquet_snappy (2.00) > csv (3.00) > pickle (4.00) > feather_ipc (5.00) |
| opt | Read | feather_ipc (1.78) > parquet_snappy (2.22) > pickle (2.66) > parquet_zstd (3.34) > csv (5.00) |
| opt | Write | pickle (2.12) > feather_ipc (2.53) > parquet_snappy (2.59) > parquet_zstd (2.81) > csv (4.94) |
| opt | Read+write | feather_ipc (1.94) > pickle (2.31) > parquet_snappy (2.59) > parquet_zstd (3.16) > csv (5.00) |
| opt | File size | parquet_zstd (1.00) > parquet_snappy (2.00) > pickle (3.12) > feather_ipc (3.88) > csv (5.00) |

這張表說明一件事：單看 5M rows 和看整個 sweep，排名可能不同。小資料時固定 overhead 會主導，大資料時 throughput、壓縮、encoding 才更明顯。

## 7. 與原文結論的對照

### 7.1 一致的結論

1. 沒有單一格式在所有指標都勝出。

原文說不同格式在不同 metric / interval 會交換排名。我們 Polars 結果也一樣：讀取、寫入、roundtrip、檔案大小的最佳格式不完全相同。

2. optimized schema 大幅降低 RAM。

原文 5M rows 約省 60% RAM；我們約省 55.2%。方向一致，幅度接近，但 Polars 的 nonopt baseline 不是 pandas `object`，因此不應期待完全相同。

3. CSV 不適合大量資料 roundtrip。

原文 CSV 在所有時間指標都很差。我們的 Polars CSV writer 比 pandas 原文強很多，但 CSV 仍有明顯限制：檔案大、讀取要 parse text、不保留 typed binary schema、optimized storage saving 很小。

4. Parquet 是 storage 強項。

原文 nonopt storage 是 Parquet 最小；我們也是 Parquet 最小，且 Zstd 比 Snappy 更省。

### 7.2 不一致或需要重新詮釋的地方

1. pandas 原文 optimized Pickle 很強；Polars replication 中 Pickle 沒有同樣統治力。

原因是這裡 pickle 的物件是 Polars DataFrame，不是 pandas DataFrame。Pickle 不是跨 engine 的穩定資料格式 benchmark；它更像「把目前 Python 物件序列化」的工具。因此 Pickle 結果高度依賴 DataFrame implementation。

2. 原文 Feather 在 nonopt time performance 很強；Polars 中 Feather IPC 讀取仍強，但寫入與 roundtrip 不一定最佳。

在我們的 nonopt 5M rows，Feather IPC read 最快，但 write 和 roundtrip 輸給 Parquet。這表示 Polars 的 Parquet 寫入與壓縮路徑很有競爭力。

3. 原文 CSV 幾乎時間面全面最差；Polars CSV saving 沒有那麼差。

這不代表 CSV 變成好選項，而是 Polars CSV writer 本身很快。CSV 仍然有 text parsing、schema loss、檔案偏大、datetime/category/type preservation 不佳等問題。

4. Parquet Zstd 是原文沒有的額外分支。

Zstd 在我們的結果裡檔案最小。5M optimized roundtrip 的 full run 表面上 Zstd 最快，但診斷重測顯示 Snappy / Zstd 非常接近，所以這裡應該保守解讀為：Zstd 在 storage 上勝出，時間表現需依硬體、cache、壓縮設定與多輪測試判斷。

## 8. 本次 Polars 結論

如果用 Polars engine 重新回答原文問題：

### 哪個格式讀取最快？

- nonopt：Feather IPC 最穩定，5M rows 也是最快。
- opt：整體平均 Feather IPC 最好，但 5M rows 時 Parquet Snappy / Zstd 最快。

### 哪個格式寫入最快？

- nonopt：Parquet Snappy / Zstd 較強。
- opt：Feather IPC 與 Pickle 較強；但 Pickle 不建議當長期資料交換格式。

### 哪個格式 read+write roundtrip 最好？

- nonopt：Parquet Snappy / Zstd 最好。
- opt：5M rows 時 Parquet Snappy / Zstd 都很強；整體 sweep 平均 Feather IPC 最好。Snappy / Zstd 的相對排名需要更多 raw samples 才能穩定判斷。

### 哪個格式最省空間？

- Parquet Zstd 最省。
- Parquet Snappy 第二。
- CSV 通常偏大。
- Feather IPC / Pickle 對 optimized schema 很敏感，optimized 後可以大幅縮小，但 nonopt 字串 schema 下會很大。

### optimized schema 是否值得？

值得。5M rows 時：

- RAM 從 781.6MB 降到 350.5MB，省約 55.2%。
- Feather IPC / Pickle 檔案大小省約 79.7%。
- CSV 檔案大小只省約 13.0%，再次說明 CSV 不適合保存 typed schema 的優勢。

## 9. 實務建議

如果目標是 Polars workflow：

| 使用場景 | 建議 |
| --- | --- |
| 長期儲存、分析資料集、跨工具交換 | Parquet，優先考慮 Zstd；若重視寫入速度可用 Snappy |
| 短期快取、同一 pipeline 快速讀回 | Feather IPC 可考慮 |
| Python/Polars 內部短期 object dump | Pickle 可用，但不建議當長期格式 |
| 人類檢查、小資料、通用交換 | CSV 可用 |
| 大量資料 roundtrip 或保留 dtype | 避免 CSV |

最重要的不是只選格式，而是先把 schema 做對。這一點和原文 pandas 結論完全一致。

## 10. 目前 repo 行為

現在 benchmark 指令會在跑完後自動畫圖：

```bash
uv run polars-io-benchmark --full
```

輸出：

```text
benchmark_out/polars_io_benchmark_results.csv
benchmark_out/plots/
```

如果已經有結果 CSV，只想重新畫圖：

```bash
uv run polars-io-plot
```

如果只想跑 benchmark 不畫圖：

```bash
uv run polars-io-benchmark --full --no-plot
```
