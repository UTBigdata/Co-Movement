# -*- coding: utf-8 -*-
"""
OA-MBR / LCSS-based bus bunching detection
输出：多阶串车检测结果（两辆及以上）
"""

import os
import time
import shutil
import numpy as np
import pandas as pd
import pyspark.sql
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DoubleType, TimestampType
)

# =========================
# 本地输出目录
# Python 的 os 操作用 LOCAL_OUTPUT_DIR
# Spark 写文件时用 SPARK_OUTPUT_DIR（显式 file:///）
# =========================
LOCAL_OUTPUT_DIR = "../results/accuracy2_1"
SPARK_OUTPUT_DIR = "../results/accuracy2_1"
data_path = "../samples/gps2_1/*.csv"

def save_multi_result(df, method_name):
    """
    将结果保存到本地目录，而不是 HDFS。
    最终统一输出字段：
        method, lineName, direction, component, vehicle_count,
        start_time, end_time, duration
    """
    os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

    local_save_path = os.path.join(LOCAL_OUTPUT_DIR, method_name)
    spark_save_path = f"file://{local_save_path}"

    if os.path.exists(local_save_path):
        shutil.rmtree(local_save_path)

    result = (
        df.withColumn("method", F.lit(method_name))
        .withColumn("direction", F.col("direction").cast(DoubleType()))
        .withColumn("vehicle_count", F.col("vehicle_count").cast(IntegerType()))
        .withColumn("duration", F.col("duration").cast(DoubleType()))
        .withColumn("label", F.lit(1))
        .select(
            "method", "lineName", "direction", "component",
            "vehicle_count", "start_time", "end_time", "duration", "label"
        )
        .orderBy("lineName", "direction", "start_time", "component")
    )

    result.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(spark_save_path)

    print(f"[SAVE] {method_name} -> {local_save_path}")


def make_time_window_udf(win_size: int):
    @F.udf(returnType=IntegerType())
    def calculate_time_window(timestamp_obj):
        if timestamp_obj is None:
            return None
        midnight = datetime(timestamp_obj.year, timestamp_obj.month, timestamp_obj.day)
        minutes_since_midnight = (timestamp_obj - midnight).total_seconds() / 60.0
        window_start_minute = int(minutes_since_midnight // win_size * win_size)
        return window_start_minute

    return calculate_time_window


def calculate_angle_with_horizontal(end_dis, start_dis):
    vertical_distance = end_dis - start_dis
    return float(np.degrees(np.arctan2(vertical_distance, 1)) % 360)


def circular_angle_diff(angle1, angle2):
    diff = abs(angle1 - angle2) % 360.0
    return min(diff, 360.0 - diff)


def lcss_similarity(points_i, points_j, epsilon_t, epsilon_d):
    """
    基于 LCSS 的相似度计算
    points_i / points_j: [(timestamp, normalized_distance), ...]
    epsilon_t: 时间阈值（秒）
    epsilon_d: 相对距离阈值
    """
    m, n = len(points_i), len(points_j)
    if min(m, n) == 0:
        return 0.0

    dp = np.zeros((m + 1, n + 1), dtype=np.int32)

    for r in range(1, m + 1):
        t_i, dis_i = points_i[r - 1]
        for s in range(1, n + 1):
            t_j, dis_j = points_j[s - 1]

            if abs((t_i - t_j).total_seconds()) <= epsilon_t and abs(dis_i - dis_j) <= epsilon_d:
                dp[r, s] = dp[r - 1, s - 1] + 1
            else:
                dp[r, s] = max(dp[r - 1, s], dp[r, s - 1])

    return float(dp[m, n]) / float(min(m, n))


# 每条线路的 50m 归一化距离阈值
EPSILON_D_MAP = {
    "M2333": 0.00117242,
    "M2503": 0.00166688,
    "M3353": 0.00152815,
    "M5583": 0.00248862,
}


def detect_bus_bunching(data, angle_threshold, epsilon_t, epsilon_d_map, sigma, tau_minutes):
    """
    在同一 lineName、direction、win_start 内检测两辆车串车候选对
    epsilon_d_map: dict, key=lineName, value=epsilon_d
    """

    def mbrs_intersect(traj1, traj2):
        return not (
                traj1["mbr_max_time"] < traj2["mbr_min_time"] or
                traj1["mbr_min_time"] > traj2["mbr_max_time"] or
                traj1["mbr_max_dis"] < traj2["mbr_min_dis"] or
                traj1["mbr_min_dis"] > traj2["mbr_max_dis"]
        )

    def process_group_data(group_data: pd.DataFrame,
                           angle_threshold,
                           epsilon_t,
                           epsilon_d_map,
                           sigma,
                           tau_minutes):
        results = []

        if group_data.empty:
            return pd.DataFrame(results)

        line_name = str(group_data["lineName"].iloc[0])
        epsilon_d = epsilon_d_map.get(line_name, epsilon_d_map.get("M5583", 0.00248862))

        group_data = group_data.copy()
        group_data["time"] = pd.to_datetime(group_data["time"])
        group_data = group_data.sort_values(["idx", "time"]).reset_index(drop=True)

        traj_list = []
        for idx, traj in group_data.groupby("idx"):
            traj = traj.sort_values("time").reset_index(drop=True)

            if len(traj) < 2:
                continue

            points = list(zip(
                traj["time"].tolist(),
                traj["gps_normalized_distance_length"].astype(float).tolist()
            ))

            start_time = traj["time"].iloc[0]
            end_time = traj["time"].iloc[-1]
            start_dis = float(traj["gps_normalized_distance_length"].iloc[0])
            end_dis = float(traj["gps_normalized_distance_length"].iloc[-1])

            traj_list.append({
                "lineName": traj["lineName"].iloc[0],
                "direction": float(traj["direction"].iloc[0]),
                "idx": str(idx),
                "win_start": int(traj["win_start"].iloc[0]),
                "points": points,
                "start_time": start_time,
                "end_time": end_time,
                "start_dis": start_dis,
                "end_dis": end_dis,
                "mbr_min_time": traj["time"].min(),
                "mbr_max_time": traj["time"].max(),
                "mbr_min_dis": float(traj["gps_normalized_distance_length"].min()),
                "mbr_max_dis": float(traj["gps_normalized_distance_length"].max()),
                "angle": calculate_angle_with_horizontal(end_dis, start_dis)
            })

        if len(traj_list) < 2:
            return pd.DataFrame(results)

        for i in range(len(traj_list)):
            traj_i = traj_list[i]

            for j in range(i + 1, len(traj_list)):
                traj_j = traj_list[j]

                angle_diff = circular_angle_diff(traj_i["angle"], traj_j["angle"])
                if angle_diff > angle_threshold:
                    continue

                if not mbrs_intersect(traj_i, traj_j):
                    continue

                overlap_start = max(traj_i["start_time"], traj_j["start_time"])
                overlap_end = min(traj_i["end_time"], traj_j["end_time"])
                duration = (overlap_end - overlap_start).total_seconds()

                if duration < tau_minutes * 60:
                    continue

                points_i = [(t, d) for t, d in traj_i["points"] if overlap_start <= t <= overlap_end]
                points_j = [(t, d) for t, d in traj_j["points"] if overlap_start <= t <= overlap_end]

                if len(points_i) == 0 or len(points_j) == 0:
                    continue

                similarity = lcss_similarity(points_i, points_j, epsilon_t, epsilon_d)

                if similarity >= sigma:
                    idx1, idx2 = (
                        (traj_i["idx"], traj_j["idx"])
                        if traj_i["idx"] < traj_j["idx"]
                        else (traj_j["idx"], traj_i["idx"])
                    )

                    results.append({
                        "lineName": traj_i["lineName"],
                        "direction": traj_i["direction"],
                        "idx1": idx1,
                        "idx2": idx2,
                        "win_start": traj_i["win_start"],
                        "similarity": float(similarity),
                        "start_time": overlap_start,
                        "end_time": overlap_end,
                        "duration": float(duration)
                    })

        return pd.DataFrame(results)

    def process_group_data_wrapper(pdf):
        return process_group_data(
            pdf, angle_threshold, epsilon_t, epsilon_d_map, sigma, tau_minutes
        )

    output_schema = StructType([
        StructField("lineName", StringType(), True),
        StructField("direction", DoubleType(), True),
        StructField("idx1", StringType(), True),
        StructField("idx2", StringType(), True),
        StructField("win_start", IntegerType(), True),
        StructField("similarity", DoubleType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration", DoubleType(), True)
    ])

    return data.groupBy("lineName", "direction", "win_start").applyInPandas(
        process_group_data_wrapper,
        schema=output_schema
    )


def merge_bunching_events(events_df: pyspark.sql.DataFrame, win_size, tau_minutes):
    """
    将相邻窗口中的两车串车结果进行跨窗口合并
    """

    def process_group_data(group_data: pd.DataFrame):
        if group_data.empty:
            return pd.DataFrame([])

        group_data = group_data.copy()
        group_data["start_time"] = pd.to_datetime(group_data["start_time"])
        group_data["end_time"] = pd.to_datetime(group_data["end_time"])
        group_data = group_data.sort_values(by="win_start").reset_index(drop=True)

        group_data["last_end_time"] = group_data["end_time"].shift(1)
        group_data["time_diff"] = (
                group_data["start_time"] - group_data["last_end_time"]
        ).dt.total_seconds()

        temp = {0: []}
        now_key = 0

        for row in group_data.itertuples():
            row_dict = row._asdict()

            if row.Index == 0:
                temp[now_key].append(row_dict)
                continue

            if pd.notna(row.time_diff) and row.time_diff < win_size * 60:
                temp[now_key].append(row_dict)
            else:
                now_key += 1
                temp[now_key] = [row_dict]

        result = []
        for _, rows in temp.items():
            now_group = pd.DataFrame(rows)

            result.append({
                "lineName": rows[0]["lineName"],
                "direction": float(rows[0]["direction"]),
                "idx1": rows[0]["idx1"],
                "idx2": rows[0]["idx2"],
                "win_start": ",".join(now_group["win_start"].astype(str)),
                "similarity": float(now_group["similarity"].min()),
                "start_time": now_group["start_time"].min(),
                "end_time": now_group["end_time"].max(),
                "duration": float(now_group["duration"].sum()),
            })

        return pd.DataFrame(result)

    output_schema = StructType([
        StructField("lineName", StringType(), True),
        StructField("direction", DoubleType(), True),
        StructField("idx1", StringType(), True),
        StructField("idx2", StringType(), True),
        StructField("win_start", StringType(), True),
        StructField("similarity", DoubleType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration", DoubleType(), True),
    ])

    final_df = events_df.groupBy("lineName", "direction", "idx1", "idx2").applyInPandas(
        process_group_data,
        schema=output_schema
    )

    return final_df.filter((F.col("duration") / 60) >= tau_minutes)


def mutil_car_tandem_detection(pdf, tau_minutes):
    """
    基于两车串车边，构建图并提取多车串车分量（两辆及以上）
    """

    def find_serial_chains(pdf_group: pd.DataFrame):
        if pdf_group.empty:
            return pd.DataFrame([])

        pdf_group = pdf_group.copy()
        pdf_group["start_time"] = pd.to_datetime(pdf_group["start_time"])
        pdf_group["end_time"] = pd.to_datetime(pdf_group["end_time"])

        def find_component(start, visited_nodes):
            stack = [start]
            tandem_cars = []
            tandem_cars_info = {
                "start_time": graph[start]["start_time"],
                "end_time": graph[start]["end_time"],
                "similarity": graph[start]["similarity"]
            }

            tandem_cars.append(start)
            visited_nodes.add(start)

            while stack:
                car_node = stack.pop()
                for neighbor in graph[car_node]["neighbors"]:
                    if neighbor not in visited_nodes:
                        potential_start_time = max(
                            tandem_cars_info["start_time"],
                            graph[neighbor]["start_time"]
                        )
                        potential_end_time = min(
                            tandem_cars_info["end_time"],
                            graph[neighbor]["end_time"]
                        )
                        time_diff = potential_end_time - potential_start_time

                        if time_diff.total_seconds() >= tau_minutes * 60:
                            visited_nodes.add(neighbor)
                            tandem_cars.append(neighbor)
                            tandem_cars_info["start_time"] = potential_start_time
                            tandem_cars_info["end_time"] = potential_end_time
                            tandem_cars_info["similarity"] = min(
                                tandem_cars_info["similarity"],
                                graph[neighbor]["similarity"]
                            )
                            stack.append(neighbor)

            return tandem_cars, tandem_cars_info

        edges = pdf_group[
            ["idx1", "idx2", "start_time", "end_time", "similarity",
             "win_start_tandem_car_pairs", "lineName", "direction"]
        ].values.tolist()

        graph = {}
        for edge in edges:
            idx1, idx2, start_time, end_time, similarity, win_start_tandem_car_pairs, lineName, direction = edge

            if idx1 not in graph:
                graph[idx1] = {
                    "neighbors": [],
                    "start_time": start_time,
                    "end_time": end_time,
                    "similarity": similarity,
                    "win_start_tandem_car_pairs": int(win_start_tandem_car_pairs),
                    "lineName": lineName,
                    "direction": float(direction)
                }

            if idx2 not in graph:
                graph[idx2] = {
                    "neighbors": [],
                    "start_time": start_time,
                    "end_time": end_time,
                    "similarity": similarity,
                    "win_start_tandem_car_pairs": int(win_start_tandem_car_pairs),
                    "lineName": lineName,
                    "direction": float(direction)
                }

            graph[idx1]["neighbors"].append(idx2)
            graph[idx2]["neighbors"].append(idx1)

        visited = set()
        components = []

        for node in graph:
            if node not in visited:
                component, component_info = find_component(node, visited)
                components.append((component, component_info))

        result = []
        for component, component_info in components:
            if len(component) > 1:
                component_sorted = sorted([str(x) for x in component])
                component_str = ",".join(component_sorted)
                start_time = component_info["start_time"]
                end_time = component_info["end_time"]
                duration_time = (end_time - start_time).total_seconds()

                first_node = component[0]
                win_start_tandem_car_pairs = graph[first_node]["win_start_tandem_car_pairs"]
                lineName = graph[first_node]["lineName"]
                direction = graph[first_node]["direction"]

                result.append({
                    "lineName": lineName,
                    "direction": float(direction),
                    "component": component_str,
                    "vehicle_count": int(len(component)),
                    "win_start_tandem_car_pairs": int(win_start_tandem_car_pairs),
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": float(duration_time)
                })

        return pd.DataFrame(result)

    result_schema = StructType([
        StructField("lineName", StringType(), True),
        StructField("direction", DoubleType(), True),
        StructField("component", StringType(), True),
        StructField("vehicle_count", IntegerType(), True),
        StructField("win_start_tandem_car_pairs", IntegerType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration", DoubleType(), True)
    ])

    return pdf.groupBy("lineName", "direction", "win_start_tandem_car_pairs").applyInPandas(
        find_serial_chains,
        schema=result_schema
    )


def build_spark():
    """
    构建本地 Spark。
    关键点：显式指定默认文件系统为 file:///
    避免写出结果时被当成 HDFS 路径。
    """
    spark = SparkSession.builder \
        .master("local[32]") \
        .appName("BusBunchingDetection_OA_MBR_MultiCar") \
        .config("spark.executor.memory", "32g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "350") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def prepare_input_data(spark, data_path, win_size):
    """
    读取本地 csv 数据，并生成 win_start
    """
    schema = StructType([
        StructField("idx", StringType(), True),
        StructField("opath", StringType(), True),
        StructField("lineName", StringType(), True),
        StructField("direction", FloatType(), True),
        StructField("t_flag", IntegerType(), True),
        StructField("time", TimestampType(), True),
        StructField("lng", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("distance_of_gpspoint", DoubleType(), True),
        StructField("gps_normalized_distance_length", DoubleType(), True)
    ])

    time_window_udf = make_time_window_udf(win_size)

    sdf = spark.read.format("csv") \
        .schema(schema) \
        .option("header", True) \
        .option("delimiter", ",") \
        .load(data_path) \
        .drop("opath", "t_flag", "lng", "lat", "distance_of_gpspoint") \
        .repartition("lineName")

    sdf = sdf.withColumn("win_start", time_window_udf(F.col("time")))

    filtered_statistics = sdf.groupBy("lineName", "direction", "idx", "win_start").count()
    filtered_basis = filtered_statistics.filter(F.col("count") > 1).select(
        "lineName", "direction", "idx", "win_start"
    )

    sdf_filtered = sdf.join(
        filtered_basis,
        ["lineName", "direction", "idx", "win_start"],
        "inner"
    )

    return sdf_filtered, time_window_udf


def run_oa_mbr_and_save(data_path):
    """
    OA-MBR 主流程
    """
    ANGLE_THRESHOLD = 3
    EPSILON_T = 30
    EPSILON_D_MAP = {
        "M2333": 0.00117242,
        "M2503": 0.00166688,
        "M3353": 0.00152815,
        "M5583": 0.00248862,
    }
    SIGMA = 0.6
    WIN_SIZE = 4
    TAU_MINUTES = 3

    total_start_time = time.perf_counter()
    spark = None
    multi_result = None

    try:
        spark = build_spark()
        sdf_filtered, time_window_udf = prepare_input_data(spark, data_path, WIN_SIZE)

        print("[INFO] 开始检测两车串车候选对...")
        result_df = detect_bus_bunching(
            sdf_filtered,
            ANGLE_THRESHOLD,
            EPSILON_T,
            EPSILON_D_MAP,
            SIGMA,
            TAU_MINUTES
        )

        print("[INFO] 开始跨窗口合并两车串车事件...")
        merged_result_df = merge_bunching_events(
            result_df,
            WIN_SIZE,
            TAU_MINUTES
        )

        merged_result_df = merged_result_df.withColumn(
            "win_start_tandem_car_pairs",
            time_window_udf(F.col("start_time"))
        )

        print("[INFO] 开始多车串车检测...")
        multi_result = mutil_car_tandem_detection(
            merged_result_df,
            TAU_MINUTES
        ).persist()

        total_results = multi_result.count()
        save_multi_result(multi_result, "oa_mbr")

        print(f"[TOTAL_RESULT_NUM] : {total_results}")

    finally:
        try:
            if multi_result is not None:
                multi_result.unpersist()
        except Exception:
            pass

        try:
            if spark is not None:
                spark.stop()
        except Exception:
            pass

    total_end_time = time.perf_counter()
    elapsed_seconds = total_end_time - total_start_time
    print(f"[TIMER] total : {elapsed_seconds:.3f}s")


if __name__ == "__main__":
    # 本地输入路径

    run_oa_mbr_and_save(data_path)
