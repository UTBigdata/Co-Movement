# -*- coding: utf-8 -*-
"""
精准率/召回率分析脚本
加载样本数据 (sampled.csv, label=1正例/0负例) 与 OA-MBR 预测结果 (全部预测为正例)
合并 → 严格匹配 → 宽松匹配，输出 TP/FP/FN 分类及指标
"""
import glob
from typing import Set, Dict, List, Tuple

import numpy as np
import pandas as pd


# ============================================================
# 路径与参数配置
# ============================================================

GT_CSV = "../samples/2_1/sampled.csv"                    # 样本数据（ground truth）
PRED_CSV = "../results/accuracy2_1/oa_mbr/*.csv"  # OA-MBR 预测结果
OUT_CSV = "../results/accuracy2_1/precision_recall_result.csv"

TARGET_TIMEZONE = "Asia/Shanghai"
MERGE_GAP_SEC = 180           # 相邻同-component 事件合并间隔（秒）

# 严格匹配参数
STRICT_TIME_IOU = 0.1
STRICT_OVERLAP_SEC = 1.0

# 宽松匹配参数
RELAXED_COMP_JACCARD = 0.5    # 宽松匹配：车牌 Jaccard 下限
RELAXED_TIME_IOU = 0.3        # 宽松匹配：时间 IoU 下限
RELAXED_OVERLAP_SEC = 60    # 宽松匹配：最小重叠秒数


# ============================================================
# 基础工具函数
# ============================================================

def normalize_component(comp: str) -> str:
    if pd.isna(comp):
        return ""
    parts = [x.strip() for x in str(comp).split(",") if x.strip() != ""]
    def sort_key(x):
        return (0, int(x)) if x.isdigit() else (1, x)
    parts = sorted(parts, key=sort_key)
    return ",".join(parts)


def component_to_set(comp: str) -> Set[str]:
    if pd.isna(comp) or str(comp).strip() == "":
        return set()
    return set([x.strip() for x in str(comp).split(",") if x.strip() != ""])


def component_jaccard(comp_a: str, comp_b: str) -> float:
    a = component_to_set(comp_a)
    b = component_to_set(comp_b)
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def compute_vehicle_count_from_component(comp: str) -> int:
    return len(component_to_set(comp))


def to_target_tz_naive(x, target_tz=TARGET_TIMEZONE):
    if pd.isna(x):
        return pd.NaT
    ts = pd.to_datetime(x, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.tz_convert(target_tz).tz_localize(None)
    return ts


def interval_overlap_seconds(s1, e1, s2, e2) -> float:
    return max(0.0, (min(e1, e2) - max(s1, s2)).total_seconds())


def interval_union_seconds(s1, e1, s2, e2) -> float:
    return max(0.0, (max(e1, e2) - min(s1, s2)).total_seconds())


def interval_iou(s1, e1, s2, e2) -> float:
    inter = interval_overlap_seconds(s1, e1, s2, e2)
    union = interval_union_seconds(s1, e1, s2, e2)
    if union <= 0:
        return 0.0
    return inter / union


def abs_seconds_diff(t1, t2) -> float:
    return abs((t1 - t2).total_seconds())


# ============================================================
# 数据加载
# ============================================================

def load_result(path: str, method_name: str) -> pd.DataFrame:
    print(f"[LOAD] {method_name}: {path}")
    if "*" in path or "?" in path:
        files = sorted(glob.glob(path))
        if not files:
            raise FileNotFoundError(f"{method_name}: 未匹配到文件: {path}")
        print(f"  -> 匹配到 {len(files)} 个文件, 使用: {files[0]}")
        path = files[0]
    df = pd.read_csv(path)

    expected_cols = {
        "lineName", "direction", "component", "vehicle_count",
        "start_time", "end_time", "duration"
    }
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"{method_name} 缺少字段: {missing}")

    df = df.copy()
    df["method"] = method_name
    df["lineName"] = df["lineName"].astype(str)
    df["direction"] = pd.to_numeric(df["direction"], errors="coerce")
    df["component"] = df["component"].astype(str).map(normalize_component)
    df["vehicle_count"] = df["component"].map(compute_vehicle_count_from_component)

    df["start_time"] = df["start_time"].apply(lambda x: to_target_tz_naive(x))
    df["end_time"] = df["end_time"].apply(lambda x: to_target_tz_naive(x))

    if "duration" not in df.columns or df["duration"].isna().all():
        df["duration"] = (df["end_time"] - df["start_time"]).dt.total_seconds()
    else:
        df["duration"] = pd.to_numeric(df["duration"], errors="coerce")
        missing_dur = df["duration"].isna()
        df.loc[missing_dur, "duration"] = (
            df.loc[missing_dur, "end_time"] - df.loc[missing_dur, "start_time"]
        ).dt.total_seconds()

    df = df.dropna(subset=["start_time", "end_time"]).copy()
    df = df[df["end_time"] >= df["start_time"]].reset_index(drop=True)
    return df


# ============================================================
# 相邻同-component 事件合并
# ============================================================

def merge_adjacent_events(df: pd.DataFrame, gap_sec: float = MERGE_GAP_SEC) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    group_keys = ["lineName", "direction", "component"]
    merged_rows: List[Dict] = []

    for key, group in df.groupby(group_keys):
        group = group.sort_values(["start_time", "end_time"]).reset_index(drop=True)

        current_start = group.loc[0, "start_time"]
        current_end = group.loc[0, "end_time"]
        line_name = group.loc[0, "lineName"]
        direction = group.loc[0, "direction"]
        component = group.loc[0, "component"]

        for i in range(1, len(group)):
            next_start = group.loc[i, "start_time"]
            next_end = group.loc[i, "end_time"]
            gap = (next_start - current_end).total_seconds()

            if gap <= gap_sec:
                current_end = max(current_end, next_end)
            else:
                merged_rows.append({
                    "lineName": line_name, "direction": direction,
                    "component": component,
                    "vehicle_count": compute_vehicle_count_from_component(component),
                    "start_time": current_start, "end_time": current_end,
                    "duration": (current_end - current_start).total_seconds()
                })
                current_start = next_start
                current_end = next_end

        merged_rows.append({
            "lineName": line_name, "direction": direction,
            "component": component,
            "vehicle_count": compute_vehicle_count_from_component(component),
            "start_time": current_start, "end_time": current_end,
            "duration": (current_end - current_start).total_seconds()
        })

    merged_df = pd.DataFrame(merged_rows)
    merged_df = merged_df.sort_values(
        ["lineName", "direction", "start_time", "component"]
    ).reset_index(drop=True)
    return merged_df


# ============================================================
# 严格匹配（component 完全相同 + 时间重叠达阈值）
# ============================================================

def strict_match(
    base_df: pd.DataFrame,
    ma_df: pd.DataFrame,
    min_time_iou: float = STRICT_TIME_IOU,
    min_overlap_sec: float = STRICT_OVERLAP_SEC
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    matches = []
    base_used = set()
    ma_used = set()

    group_keys = ["lineName", "direction", "component"]
    base_groups = base_df.groupby(group_keys)
    ma_groups = ma_df.groupby(group_keys)
    common_keys = set(base_groups.groups.keys()) & set(ma_groups.groups.keys())
    print(f"[STRICT] common keys = {len(common_keys)}")

    for key in common_keys:
        base_sub = base_groups.get_group(key).sort_values("start_time").copy()
        ma_sub = ma_groups.get_group(key).sort_values("start_time").copy()
        base_idx_list = base_sub.index.tolist()
        ma_idx_list = ma_sub.index.tolist()
        local_used_ma = set()

        for base_idx in base_idx_list:
            base_row = base_df.loc[base_idx]
            best_ma_idx = None
            best_score = -1.0

            for ma_idx in ma_idx_list:
                if ma_idx in local_used_ma:
                    continue
                ma_row = ma_df.loc[ma_idx]
                overlap = interval_overlap_seconds(
                    base_row["start_time"], base_row["end_time"],
                    ma_row["start_time"], ma_row["end_time"])
                iou = interval_iou(
                    base_row["start_time"], base_row["end_time"],
                    ma_row["start_time"], ma_row["end_time"])
                if overlap < min_overlap_sec:
                    continue
                if iou < min_time_iou:
                    continue
                if iou > best_score:
                    best_score = iou
                    best_ma_idx = ma_idx

            if best_ma_idx is not None:
                matches.append({"oa_mbr": base_idx, "tkde_index": best_ma_idx})
                base_used.add(base_idx)
                ma_used.add(best_ma_idx)
                local_used_ma.add(best_ma_idx)

    base_unmatched = base_df.loc[~base_df.index.isin(base_used)].copy()
    ma_unmatched = ma_df.loc[~ma_df.index.isin(ma_used)].copy()
    print(f"[STRICT] matched = {len(matches)}, base_unmatched = {len(base_unmatched)}, ma_unmatched = {len(ma_unmatched)}")
    return pd.DataFrame(matches), base_unmatched, ma_unmatched


# ============================================================
# 宽松匹配（component Jaccard + 时间重叠）
# ============================================================

def relaxed_match(
    base_df: pd.DataFrame,
    ma_df: pd.DataFrame,
    min_comp_jaccard: float = RELAXED_COMP_JACCARD,
    min_time_iou: float = RELAXED_TIME_IOU,
    min_overlap_sec: float = RELAXED_OVERLAP_SEC
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    matches = []
    base_used = set()
    ma_used = set()

    group_keys = ["lineName", "direction"]
    base_groups = base_df.groupby(group_keys)
    ma_groups = ma_df.groupby(group_keys)
    common_keys = set(base_groups.groups.keys()) & set(ma_groups.groups.keys())
    print(f"[RELAXED] common line-direction groups = {len(common_keys)}")

    for key in common_keys:
        base_sub = base_groups.get_group(key).copy()
        ma_sub = ma_groups.get_group(key).copy()
        local_used_ma = set()

        for base_idx, base_row in base_sub.iterrows():
            best_ma_idx = None
            best_score = -1.0

            for ma_idx, ma_row in ma_sub.iterrows():
                if ma_idx in local_used_ma:
                    continue
                comp_j = component_jaccard(base_row["component"], ma_row["component"])
                if comp_j < min_comp_jaccard:
                    continue
                overlap = interval_overlap_seconds(
                    base_row["start_time"], base_row["end_time"],
                    ma_row["start_time"], ma_row["end_time"])
                if overlap < min_overlap_sec:
                    continue
                t_iou = interval_iou(
                    base_row["start_time"], base_row["end_time"],
                    ma_row["start_time"], ma_row["end_time"])
                if t_iou < min_time_iou:
                    continue
                score = 0.7 * t_iou + 0.3 * comp_j
                if score > best_score:
                    best_score = score
                    best_ma_idx = ma_idx

            if best_ma_idx is not None:
                matches.append({"oa_mbr": base_idx, "tkde_index": best_ma_idx})
                base_used.add(base_idx)
                ma_used.add(best_ma_idx)
                local_used_ma.add(best_ma_idx)

    base_unmatched = base_df.loc[~base_df.index.isin(base_used)].copy()
    ma_unmatched = ma_df.loc[~ma_df.index.isin(ma_used)].copy()
    print(f"[RELAXED] matched = {len(matches)}, base_unmatched = {len(base_unmatched)}, ma_unmatched = {len(ma_unmatched)}")
    return pd.DataFrame(matches), base_unmatched, ma_unmatched


# ============================================================
# 两模型匹配（严格 + 宽松），返回匹配的索引集合及配对
# ============================================================

def match_pair(df_a: pd.DataFrame, df_b: pd.DataFrame,
               name_a: str = "", name_b: str = "") -> Tuple[set, set, pd.DataFrame]:
    print(f"\n{'='*40}")
    print(f"[MATCH] {name_a}  vs  {name_b}")
    print(f"{'='*40}")

    strict_matched, a_left, b_left = strict_match(
        df_a, df_b,
        min_time_iou=STRICT_TIME_IOU,
        min_overlap_sec=STRICT_OVERLAP_SEC)

    relaxed_matched, a_only, b_only = relaxed_match(
        a_left, b_left,
        min_comp_jaccard=RELAXED_COMP_JACCARD,
        min_time_iou=RELAXED_TIME_IOU,
        min_overlap_sec=RELAXED_OVERLAP_SEC)

    all_matched = pd.concat([strict_matched, relaxed_matched], ignore_index=True)

    if len(all_matched) > 0:
        a_matched = set(all_matched["oa_mbr"].values)
        b_matched = set(all_matched["tkde_index"].values)
    else:
        a_matched = set()
        b_matched = set()

    print(f"[MATCH] {name_a}↔{name_b}: 总匹配 {len(all_matched)} 对")
    return a_matched, b_matched, all_matched


# ============================================================
# 构建输出 CSV
# ============================================================

OUTPUT_COLS = ["lineName", "direction", "component", "vehicle_count",
               "start_time", "end_time", "duration", "result_type"]


def build_pr_output(df_pred: pd.DataFrame, df_gt: pd.DataFrame,
                    pairs_df: pd.DataFrame, pred_matched: set,
                    gt_matched: set) -> pd.DataFrame:
    """
    根据匹配结果分类为 TP / FP / FN
    df_pred: OA—MBR 预测结果 (all predicted positive)
    df_gt:   样本数据 (label=1 正例, label=0 负例)
    pairs_df: 匹配对 (oa_mbr=pred_idx, tkde_index=gt_idx)
    """
    rows = []

    # --- TP: 预测匹配到正例 (gt label=1) ---
    # --- FP: 预测匹配到负例 (gt label=0) ---
    for _, pair in pairs_df.iterrows():
        pred_idx = pair["oa_mbr"]
        gt_idx = pair["tkde_index"]
        gt_label = df_gt.loc[gt_idx, "label"]
        pred_row = df_pred.loc[pred_idx]

        if gt_label == 1:
            result_type = "TP"
        else:
            result_type = "FP"

        rows.append({
            "lineName": pred_row["lineName"], "direction": pred_row["direction"],
            "component": pred_row["component"], "vehicle_count": pred_row["vehicle_count"],
            "start_time": pred_row["start_time"], "end_time": pred_row["end_time"],
            "duration": pred_row["duration"], "result_type": result_type
        })

    # --- FP: 预测未匹配到任何样本 ---
    for idx in df_pred.index:
        if idx not in pred_matched:
            row = df_pred.loc[idx]
            rows.append({
                "lineName": row["lineName"], "direction": row["direction"],
                "component": row["component"], "vehicle_count": row["vehicle_count"],
                "start_time": row["start_time"], "end_time": row["end_time"],
                "duration": row["duration"], "result_type": "FP"
            })

    # --- FN: 正例未被任何预测匹配到 ---
    for idx in df_gt.index:
        if df_gt.loc[idx, "label"] == 1 and idx not in gt_matched:
            row = df_gt.loc[idx]
            rows.append({
                "lineName": row["lineName"], "direction": row["direction"],
                "component": row["component"], "vehicle_count": row["vehicle_count"],
                "start_time": row["start_time"], "end_time": row["end_time"],
                "duration": row["duration"], "result_type": "FN"
            })

    result = pd.DataFrame(rows, columns=OUTPUT_COLS)
    result = result.sort_values(
        ["lineName", "direction", "start_time", "component"]
    ).reset_index(drop=True)
    return result


# ============================================================
# 主程序
# ============================================================

def main():
    name_pred = "oa_mbr"
    name_gt = "Sampled"

    # 1. 加载样本数据（ground truth）
    df_gt_raw = pd.read_csv(GT_CSV)
    df_gt_raw["start_time"] = pd.to_datetime(df_gt_raw["start_time"])
    df_gt_raw["end_time"] = pd.to_datetime(df_gt_raw["end_time"])
    df_gt_raw["lineName"] = df_gt_raw["lineName"].astype(str)
    df_gt_raw["direction"] = pd.to_numeric(df_gt_raw["direction"], errors="coerce")
    df_gt_raw["component"] = df_gt_raw["component"].astype(str).map(normalize_component)
    df_gt_raw["vehicle_count"] = df_gt_raw["component"].map(compute_vehicle_count_from_component)
    n_gt = len(df_gt_raw)
    n_pos = (df_gt_raw["label"] == 1).sum()
    n_neg = (df_gt_raw["label"] == 0).sum()
    print(f"[GT] {name_gt}: 共 {n_gt} 条 (正例={n_pos}, 负例={n_neg})")

    # 2. 加载预测结果
    df_pred_raw = load_result(PRED_CSV, name_pred)
    n_pred_raw = len(df_pred_raw)
    print(f"[PRED] {name_pred} 原始: {n_pred_raw} (全部预测为正例)")

    # 3. 合并相邻事件（预测结果需要合并，样本数据已规范化无需合并）
    df_gt = df_gt_raw.copy()
    df_pred = merge_adjacent_events(df_pred_raw)
    print(f"[MERGE] gap={MERGE_GAP_SEC}s:")
    print(f"  {name_gt}: {n_gt} 条 (无需合并)")
    print(f"  {name_pred}: {n_pred_raw}→{len(df_pred)} (-{n_pred_raw - len(df_pred)})")

    # 4. 匹配：预测 vs 样本
    pred_matched, gt_matched, pairs_df = match_pair(df_pred, df_gt, name_pred, name_gt)

    # 5. 构建输出
    out_df = build_pr_output(df_pred, df_gt, pairs_df, pred_matched, gt_matched)
    out_df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n[OUTPUT] {OUT_CSV} 已写入，共 {len(out_df)} 行")

    # 6. 统计指标
    n_tp = (out_df["result_type"] == "TP").sum()
    n_fp = (out_df["result_type"] == "FP").sum()
    n_fn = (out_df["result_type"] == "FN").sum()
    n_tn = n_neg - (out_df["result_type"] == "FP").sum()  # approximate

    precision = n_tp / (n_tp + n_fp) * 100 if (n_tp + n_fp) > 0 else 0.0
    recall = n_tp / (n_tp + n_fn) * 100 if (n_tp + n_fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    print()
    print("=" * 55)
    print(f"   精准率 / 召回率 分析")
    print("=" * 55)
    print()
    print(f"  TP (真正例):  {n_tp:>6d}")
    print(f"  FP (假正例):  {n_fp:>6d}")
    print(f"  FN (假负例):  {n_fn:>6d}")
    print(f"  TN (真负例):  {n_tn:>6d}")
    print()
    print(f"  精准率 (Precision): {n_tp}/{n_tp+n_fp} = {precision:.2f}%")
    print(f"  召回率 (Recall):    {n_tp}/{n_tp+n_fn} = {recall:.2f}%")
    print(f"  F1-Score:           {f1:.2f}%")
    print()
    print(f"[DONE] 结果已写入 {OUT_CSV}")


if __name__ == "__main__":
    main()
