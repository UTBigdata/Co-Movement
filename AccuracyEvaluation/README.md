# Accuracy Analysis — Precision Evaluation of the Bus Co-Movement Detection Algorithm

This toolkit is designed for evaluating the detection accuracy of the OA-MBR-based bus co-movement detection algorithm using LCSS trajectory similarity. It includes the main detection program, precision/recall comparison analysis, and visualization scripts.

## Directory Structure

```text
Accuracy Analysis/
├── source code/
│   ├── OA-MBR.py                              # Main OA-MBR bus co-movement detection program (PySpark)
│   ├── compare_summary_base_vs_oa_mbr.py       # Precision/recall analysis: OA-MBR predictions vs. manual annotations
│   └── plot_metrics.py                         # Bar chart visualization of Precision/Recall/F1 for three datasets
├── samples/
│   ├── 1_1/sampled.csv                         # Manually annotated data with a 1:1 positive-to-negative ratio
│   ├── 1_2/sampled.csv                         # Manually annotated data with a 1:2 positive-to-negative ratio
│   ├── 2_1/sampled.csv                         # Manually annotated data with a 2:1 positive-to-negative ratio
│   ├── gps1_1/gps_data.csv                     # Raw GPS trajectories for the 1:1 dataset
│   ├── gps1_2/gps_data.csv                     # Raw GPS trajectories for the 1:2 dataset
│   └── gps2_1/gps_data.csv                     # Raw GPS trajectories for the 2:1 dataset
├── result/                                     # Output directory generated after execution
│   ├── accuracy1_1/
│   │   ├── precision_recall_result.csv
│   │   └── oa_mbr/                             # Raw OA-MBR detection output for the 1:1 dataset
│   ├── accuracy1_2/
│   │   ├── precision_recall_result.csv
│   │   └── oa_mbr/                             # Raw OA-MBR detection output for the 1:2 dataset
│   ├── accuracy2_1/
│   │   ├── precision_recall_result.csv
│   │   └── oa_mbr/                             # Raw OA-MBR detection output for the 2:1 dataset
│   └── plot/
│       ├── metrics_comparison.pdf
│       └── metrics_comparison.svg
└── README.md
```

---

## Dataset Description

### Annotated Sample Data: `samples/{1_1, 1_2, 2_1}/sampled.csv`

The manually annotated ground-truth data consist of bus co-movement events and are divided into three groups according to different positive-to-negative sample ratios.

| Dataset | Positive-to-Negative Ratio | Description |
|--------|-----------------------------|-------------|
| `1_1` | 1:1 | Balanced positive and negative samples, involving four bus routes: M2333, M2503, M3353, and M5583 |
| `1_2` | 1:2 | More negative samples than positive samples, simulating sparse positive events in real-world scenarios |
| `2_1` | 2:1 | More positive samples than negative samples, used for evaluating dense positive-event scenarios |

**Field Description:**

| Field | Type | Description |
|------|------|-------------|
| `lineName` | str | Bus route name, e.g., M2333 |
| `direction` | float | Travel direction, i.e., 1.0 or 0.0 |
| `component` | str | License plate IDs involved in the co-movement event, separated by commas, e.g., `BS00077D,BS01899D` |
| `vehicle_count` | int | Number of vehicles involved in the event |
| `start_time` | datetime | Start time of the co-movement event |
| `end_time` | datetime | End time of the co-movement event |
| `duration` | float | Event duration in seconds |
| `label` | int | Sample label: `1` = positive sample, i.e., true co-movement event; `0` = negative sample, i.e., non-co-movement event |

### Raw GPS Data: `samples/{gps1_1, gps1_2, gps2_1}/gps_data.csv`

The raw GPS trajectory data of bus vehicles are used as the input of the OA-MBR algorithm.

**Field Description:**

| Field | Type | Description |
|------|------|-------------|
| `idx` | str | Vehicle license plate identifier |
| `opath` | float | Operating path ID |
| `lineName` | str | Bus route name |
| `direction` | float | Travel direction |
| `t_flag` | int | Trip flag |
| `time` | datetime | GPS timestamp |
| `lng` | float | Longitude |
| `lat` | float | Latitude |
| `distance_of_gpspoint` | float | Distance between adjacent GPS points, in meters |
| `gps_normalized_distance_length` | float | Normalized route distance in the range [0, 1) |

### Evaluation Result Data: `result/accuracy*/precision_recall_result.csv`

This file contains event-level classification results obtained by comparing OA-MBR predictions with manually annotated ground truth.

**Field Description:**

| Field | Type | Description |
|------|------|-------------|
| `lineName` | str | Bus route name |
| `direction` | float | Travel direction |
| `component` | str | Set of vehicle license plate IDs |
| `vehicle_count` | int | Number of vehicles |
| `start_time` | datetime | Event start time |
| `end_time` | datetime | Event end time |
| `duration` | float | Event duration in seconds |
| `result_type` | str | Classification result: TP, FP, or FN |

---

## Module Description

### 1. `OA-MBR.py` — Bus Co-Movement Detection Algorithm

This module implements a multi-order bus co-movement detection algorithm based on LCSS, i.e., Longest Common Subsequence, trajectory similarity. The algorithm is implemented using PySpark.

**Overall Workflow:**  
Read GPS data → segment trajectories by time windows → compute pairwise trajectory similarity using LCSS with MBR pre-filtering and angle filtering → merge events across adjacent windows → extract multi-vehicle co-movement events using graph connected components → output detection results.

**Core Parameters:**

| Parameter | Default Value | Description |
|----------|---------------|-------------|
| `ANGLE_THRESHOLD` | 3° | Threshold for the angle difference between the movement directions of two vehicles |
| `EPSILON_T` | 30 s | Temporal tolerance for LCSS |
| `SIGMA` | 0.6 | LCSS similarity threshold |
| `WIN_SIZE` | 4 min | Time-window size |
| `TAU_MINUTES` | 3 min | Minimum duration of a co-movement event |

**Function Interfaces:**

| Function | Input | Output | Description |
|---------|-------|--------|-------------|
| `build_spark()` | — | `SparkSession` | Builds a local Spark environment using `local[32]` |
| `prepare_input_data(spark, data_path, win_size)` | SparkSession, GPS data path, window size | `(sdf, time_window_udf)` | Reads the CSV file and generates the time-window column |
| `make_time_window_udf(win_size)` | Window size in minutes | PySpark UDF | Maps a timestamp to the starting minute of the corresponding time window |
| `lcss_similarity(points_i, points_j, epsilon_t, epsilon_d)` | Two lists of trajectory points, temporal tolerance, distance tolerance | `float` in [0, 1] | Computes the normalized LCSS similarity between two trajectories |
| `detect_bus_bunching(data, angle_threshold, epsilon_t, epsilon_d_map, sigma, tau_minutes)` | Spark DataFrame and parameters | Spark DataFrame | Detects candidate two-vehicle co-movement pairs using grouped `applyInPandas` processing |
| `merge_bunching_events(events_df, win_size, tau_minutes)` | Two-vehicle co-movement results, window size, minimum duration | Spark DataFrame | Merges adjacent co-movement events across windows |
| `mutil_car_tandem_detection(pdf, tau_minutes)` | Merged results, minimum duration | Spark DataFrame | Extracts multi-vehicle co-movement components from two-vehicle edges using graph connected components |
| `save_multi_result(df, method_name)` | Result DataFrame, method name | — | Saves the output as a CSV file using `coalesce(1)` |
| `run_oa_mbr_and_save(data_path)` | GPS data path | — | Main entry function that connects the entire workflow |

**Execution:**

```bash
python "source code/OA-MBR.py"
```

---

### 2. `compare_summary_base_vs_oa_mbr.py` — Precision/Recall Analysis

This module compares OA-MBR prediction results with manually annotated ground-truth samples through a two-stage matching strategy. It calculates TP, FP, and FN, and exports the event-level classification results.

**Two-Stage Matching Strategy:**

| Stage | Component Requirement | Temporal IoU | Minimum Overlap | Description |
|------|------------------------|--------------|-----------------|-------------|
| Strict matching | Exactly the same license plate set | ≥ 0.1 | ≥ 1 s | Same route, same direction, same vehicle set, and basic temporal overlap |
| Relaxed matching | License plate Jaccard similarity ≥ 0.5 | ≥ 0.3 | ≥ 60 s | Allows partially different vehicle sets but requires stronger temporal consistency |

**Core Parameters:**

| Parameter | Default Value | Description |
|----------|---------------|-------------|
| `MERGE_GAP_SEC` | 180 s | Gap threshold for merging adjacent events with the same component |
| `STRICT_TIME_IOU` | 0.1 | Lower bound of temporal IoU for strict matching |
| `RELAXED_COMP_JACCARD` | 0.5 | Lower bound of license plate Jaccard similarity for relaxed matching |
| `RELAXED_TIME_IOU` | 0.3 | Lower bound of temporal IoU for relaxed matching |

**Function Interfaces:**

| Function | Input | Output | Description |
|---------|-------|--------|-------------|
| `normalize_component(comp)` | `str`, comma-separated license plate string | `str`, sorted and normalized license plate string | Avoids matching failures caused by different ordering of the same vehicle set |
| `component_to_set(comp)` | `str`, license plate string | `Set[str]` | Converts a license plate string into a set |
| `component_jaccard(comp_a, comp_b)` | `str`, `str` | `float` in [0, 1] | Computes the Jaccard similarity between two vehicle sets |
| `interval_overlap_seconds(s1, e1, s2, e2)` | Four `datetime` values | `float` | Computes the overlap duration between two time intervals in seconds |
| `interval_union_seconds(s1, e1, s2, e2)` | Four `datetime` values | `float` | Computes the union duration between two time intervals in seconds |
| `interval_iou(s1, e1, s2, e2)` | Four `datetime` values | `float` in [0, 1] | Computes temporal IoU as overlap divided by union |
| `to_target_tz_naive(x, target_tz)` | Timestamp, target time-zone name | `datetime`, tz-naive | Converts timestamps to the target time zone and removes time-zone information |
| `load_result(path, method_name)` | File path, method name | `DataFrame` | Loads prediction result CSV files and normalizes them into a unified format |
| `merge_adjacent_events(df, gap_sec)` | `DataFrame`, gap threshold in seconds | `DataFrame` | Merges adjacent events with the same route, direction, and vehicle component |
| `strict_match(base_df, ma_df, ...)` | Sample DataFrame, prediction DataFrame, thresholds | `(matches_df, base_unmatched, ma_unmatched)` | Performs strict matching and returns matched pairs and remaining unmatched records |
| `relaxed_match(base_df, ma_df, ...)` | Remaining data after strict matching, thresholds | `(matches_df, base_unmatched, ma_unmatched)` | Performs relaxed matching using Jaccard similarity and temporal IoU |
| `match_pair(df_a, df_b, name_a, name_b)` | Two DataFrames and their names | `(set_a_matched, set_b_matched, pairs_df)` | Combines strict and relaxed matching |
| `build_pr_output(df_pred, df_gt, pairs_df, pred_matched, gt_matched)` | Prediction data, ground-truth data, matched index pairs | `DataFrame` | Outputs results with the `result_type` column, i.e., TP, FP, or FN |
| `main()` | — | — | Main entry function: load data → merge events → match events → output results → print metrics |

**Execution:**

```bash
python "source code/compare_summary_base_vs_oa_mbr.py"
```

---

### 3. `plot_metrics.py` — Visualization of Metrics on Three Datasets

This module reads the `precision_recall_result.csv` files from the three datasets, computes Precision, Recall, and F1-Score, and draws a grouped bar chart.

**Function Interface:**

| Function | Input | Output | Description |
|---------|-------|--------|-------------|
| `compute_metrics(path)` | `str`, CSV path | `(precision, recall, f1)`, three `float` values in percentage | Counts TP, FP, and FN, and computes the three metrics |

**Metric Formulas:**

- Precision = TP / (TP + FP) × 100%
- Recall = TP / (TP + FN) × 100%
- F1 = 2 × P × R / (P + R)

**Execution:**

```bash
python "source code/plot_metrics.py"
```
