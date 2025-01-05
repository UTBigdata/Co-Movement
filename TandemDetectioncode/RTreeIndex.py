import os
import time
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, udf, unix_timestamp
from pyspark.sql.types import ArrayType
from datetime import datetime, timedelta
from rtree import index
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql import SparkSession


# 展示每个分区的数据量
def partition_info(df):
    def inner(iterator):
        yield len(list(iterator))
    return df.rdd.mapPartitions(inner).collect()


# 后续处理代码
def assign_to_windows(time):
    minutes_since_midnight = (time - datetime(time.year, time.month, time.day)).total_seconds() / 60.0
    window_start_minute = int(minutes_since_midnight // 4 * 4)
    window_end_minute = window_start_minute + 4
    return [window_start_minute, window_end_minute]


# 使用 applyInPandas 替换 pandas_udf
def calculate_mbrs(group):
    min_time = group['time'].min()
    max_time = group['time'].max()
    min_dis_location = group['gps_normalized_distance_length'].min()
    max_dis_location = group['gps_normalized_distance_length'].max()
    return pd.DataFrame(
        [[group['idx'].iloc[0], group['win_st'].iloc[0], min_time, max_time, min_dis_location, max_dis_location]])


def find_intersections(mbrs, Mbr_Tree):
    intersection_pairs = []
    for idx, mbr in mbrs.iterrows():
        intersecting_mbrs = list(
            Mbr_Tree.intersection((mbr.min_timestamp, mbr.min_dis_location, mbr.max_timestamp, mbr.max_dis_location),
                                  objects=True))
        for inter in intersecting_mbrs:
            if idx < inter.id and mbrs.loc[idx]['idx'] != mbrs.loc[inter.id]['idx']:  # 防止重复对比并排除自己
                intersection_pairs.append((idx, inter.id))
    return intersection_pairs


def mbrs_intersect(mbr1, mbr2):
    return not (mbr1['max_timestamp'] < mbr2['min_timestamp'] or mbr1['min_timestamp'] > mbr2['max_timestamp'] or
                mbr1['max_dis_location'] < mbr2['min_dis_location'] or mbr1['min_dis_location'] > mbr2[
                    'max_dis_location'])


def calculate_line_parameters(point1, point2):
    x_coords, y_coords = [point1[0], point2[0]], [point1[1], point2[1]]
    A = np.vstack([x_coords, np.ones(len(x_coords))]).T
    m, c = np.linalg.lstsq(A, y_coords, rcond=None)[0]
    return m, c


def point_to_line_distance(point, line_points):
    x0, y0 = point
    x1, y1 = line_points[0]
    x2, y2 = line_points[1]
    m, c = calculate_line_parameters((x1, y1), (x2, y2))
    A, B, C = -m, 1, -c
    distance = abs(A * x0 + B * y0 + C) / (np.sqrt(A ** 2 + B ** 2))
    return distance


def calculate_max_vertical_distance_between_lines(line1_points, line2_points):
    point1_line2, point2_line2, midpoint_line2 = line2_points
    distance_start = point_to_line_distance(point1_line2, line1_points)
    distance_mid = point_to_line_distance(midpoint_line2, line1_points)
    distance_end = point_to_line_distance(point2_line2, line1_points)
    return max(distance_start, distance_mid, distance_end)


def detect_bus_bunching(data, intersection_pairs, threshold):
    bunching_events = []

    for (idx1, idx2) in intersection_pairs:
        mbr1 = mbrs_pd.iloc[idx1]
        mbr2 = mbrs_pd.iloc[idx2]

        if mbrs_intersect(mbr1, mbr2):
            line1_points = [(mbr1['min_timestamp'], mbr1['min_dis_location']),
                            (mbr1['max_timestamp'], mbr1['max_dis_location'])]
            line2_points = [(mbr2['min_timestamp'], mbr2['min_dis_location']),
                            (mbr2['max_timestamp'], mbr2['max_dis_location'])]
            midpoint_line2 = (
                (line2_points[0][0] + line2_points[1][0]) / 2, (line2_points[0][1] + line2_points[1][1]) / 2)
            distance = calculate_max_vertical_distance_between_lines(line1_points,
                                                                     [line2_points[0], midpoint_line2, line2_points[1]])

            # 计算串车持续时间
            min_time = min(mbr1['min_time'], mbr2['min_time'])
            max_time = max(mbr1['max_time'], mbr2['max_time'])
            duration = max_time - min_time

            if distance < threshold:
                bunching_events.append({
                    'lineName': data.iloc[idx1]['lineName'],
                    'direction': data.iloc[idx1]['direction'],
                    'idx1': data.iloc[idx1]['idx'],
                    'idx2': data.iloc[idx2]['idx'],
                    'win_st': data.iloc[idx1]['win_st'],
                    'distance': distance,
                    'start_time': min_time,
                    'end_time': max_time,
                    'duration': duration
                })

    return pd.DataFrame(bunching_events)


def merge_bunching_events(events):
    # 过滤掉 idx1 与 idx2 相同的数据
    events = events[events['idx1'] != events['idx2']]

    # 按 win_st、idx1 和 idx2 分组，保留每组 distance 最大的记录
    events = events.loc[events.groupby(['win_st', 'idx1', 'idx2'])['distance'].idxmax()].reset_index(drop=True)

    events = events.sort_values(by='win_st').reset_index(drop=True)
    merged_events = []
    i = 0

    while i < len(events):
        current_event = events.iloc[i]
        j = i + 1

        while j < len(events):
            next_event = events.iloc[j]
            if (current_event['idx1'] == next_event['idx1'] and current_event['idx2'] == next_event['idx2'] and
                    (next_event['start_time'] - current_event['end_time']).total_seconds() < 5):
                current_event['end_time'] = next_event['end_time']
                current_event['duration'] = current_event['end_time'] - current_event['start_time']
                current_event['win_st'] = f"{current_event['win_st']},{next_event['win_st']}"
                j += 1
            else:
                break

        merged_events.append(current_event)
        i = j

    merged_df = pd.DataFrame(merged_events)
    # 过滤掉持续时间小于3分钟的事件
    filtered_merged_df = merged_df[merged_df['duration'] >= timedelta(minutes=3)]
    return filtered_merged_df


if __name__ == "__main__":
    os.environ['PYSPARK_PYTHON'] = '/home/lrr/softs/anaconda3/bin/python3.11'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/lrr/softs/anaconda3/bin/python3.11'
    # 记录开始时间
    start_time = time.time()
    # spark = SparkSession.builder \
    #     .master("local[32]") \
    #     .appName("BusBunchingDetection") \
    #     .config("spark.executor.cores", "32") \
    #     .config("spark.executor.memory", "32g") \
    #     .config("spark.driver.memory", "8g") \
    #     .config("spark.local.dir", "/home/lrr/jupyter/LRR_data/tmp") \
    #     .config("spark.sql.shuffle.partitions", "350") \
    #     .config("spark.driver.maxResultSize", "2g") \
    #     .getOrCreate()

    spark = SparkSession.builder \
        .master("local[32]") \
        .appName("BusBunchingDetection") \
        .config("spark.executor.cores", "28") \
        .config("spark.executor.memory", "32g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.local.dir", "/home/lrr/jupyter/LRR_data/tmp") \
        .config("spark.sql.shuffle.partitions", "350") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("idx", StringType(), True),
        StructField("opath", StringType(), True),
        StructField("lineName", StringType(), True),
        StructField("direction", DoubleType(), True),
        StructField("t_flag", IntegerType(), True),
        StructField("time", TimestampType(), True),
        StructField("lng", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("distance_of_gpspoint", DoubleType(), True),
        StructField("gps_normalized_distance_length", DoubleType(), True)
    ])

    # 设置数据路径，文件夹路径
    DataPath = "/home/lrr/jupyter/LRR_data/data_20_xin/*.csv"
    fdata = spark.read.format("csv").schema(schema).option("header", value=True).option("delimiter", ",").load(DataPath)
    # fdata.show()

    # 将数据按lineName字段分区
    fdata = fdata.repartition("lineName")

    assign_to_windows_udf = udf(assign_to_windows, ArrayType(IntegerType()))
    fdata = fdata.withColumn("win_st_end", assign_to_windows_udf(col("time")))

    fdata = fdata.withColumn("win_st", col("win_st_end")[0]) \
        .withColumn("win_end", col("win_st_end")[1])

    windowed_data = fdata.groupBy("lineName", "idx", "win_st").count()
    filtered_idx = windowed_data.filter(col("count") > 1).select("lineName", "idx", "win_st")
    fdata_filtered = fdata.join(filtered_idx, ["lineName", "idx", "win_st"], "inner")
    # fdata_filtered.show()
    # fdata_filtered.count()

    # 定义输出schema
    output_schema = StructType([
        StructField("idx", StringType(), True),
        StructField("win_st", IntegerType(), True),
        StructField("min_time", TimestampType(), True),
        StructField("max_time", TimestampType(), True),
        StructField("min_dis_location", DoubleType(), True),
        StructField("max_dis_location", DoubleType(), True)
    ])
    partitions_info = partition_info(fdata)

    assign_to_windows_udf = udf(assign_to_windows, ArrayType(IntegerType()))
    fdata = fdata.withColumn("win_st_end", assign_to_windows_udf(col("time")))

    fdata = fdata.withColumn("win_st", col("win_st_end")[0]) \
        .withColumn("win_end", col("win_st_end")[1])

    windowed_data = fdata.groupBy("lineName", "idx", "win_st").count()
    filtered_idx = windowed_data.filter(col("count") > 1).select("lineName", "idx", "win_st")
    fdata_filtered = fdata.join(filtered_idx, ["lineName", "idx", "win_st"], "inner")
    # fdata_filtered.show()
    # fdata_filtered.count()

    # 定义输出schema
    output_schema = StructType([
        StructField("idx", StringType(), True),
        StructField("win_st", IntegerType(), True),
        StructField("min_time", TimestampType(), True),
        StructField("max_time", TimestampType(), True),
        StructField("min_dis_location", DoubleType(), True),
        StructField("max_dis_location", DoubleType(), True)
    ])

    mbrs = fdata_filtered.groupBy("idx", "win_st").applyInPandas(calculate_mbrs, schema=output_schema)
    mbrs = mbrs.withColumn("min_timestamp", unix_timestamp(col("min_time"))) \
        .withColumn("max_timestamp", unix_timestamp(col("max_time")))

    # 创建 R-Tree 索引并查找相交的 MBR
    p = index.Property()
    Mbr_Tree = index.Index(properties=p)

    mbrs_pd = mbrs.toPandas()

    for idx, mbr in mbrs_pd.iterrows():
        Mbr_Tree.insert(idx, (mbr.min_timestamp, mbr.min_dis_location, mbr.max_timestamp, mbr.max_dis_location))

    intersection_pairs = find_intersections(mbrs_pd, Mbr_Tree)

    threshold = 0.01
    bunching_df = detect_bus_bunching(fdata_filtered.toPandas(), intersection_pairs, threshold)

    merged_bunching_df = merge_bunching_events(bunching_df)

    # 输出合并后的串车事件
    print(merged_bunching_df)

    # 记录结束时间
    end_time = time.time()

    # 计算并显示运行时间
    print(f"Total execution time: {end_time - start_time} seconds")

    # 停止SparkSession
    spark.stop()
