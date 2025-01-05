import os
import pyspark.sql.functions as F
import numpy as np
import pandas as pd
import time
from pyspark.sql.functions import lead, rank
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import col, concat_ws, lit, when
from pyspark.sql.window import Window


# 将数据进行分区，并展示每个分区的数据量
def partition_info(df):
    def inner(iterator):
        yield len(list(iterator))

    return df.rdd.mapPartitions(inner).collect()


# 将一个给定的时间 time 分配到一个特定的时间窗口中。这个时间窗口的长度是 4 分钟。
def assign_to_windows(time):
    # 从午夜到当前时间 time 经过的分钟数。
    minutes_since_midnight = (time - datetime(time.year, time.month, time.day)).total_seconds() / 60.0
    # 确定时间窗口的起始分钟数
    window_start_minute = int(minutes_since_midnight // 4 * 4)
    # 获得窗口的结束分钟数
    window_end_minute = window_start_minute + 4
    return [window_start_minute, window_end_minute]


def process_group(group, threshold):
    bunching_events = []

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

    for i in range(len(group) - 1):
        for j in range(i + 1, len(group)):
            if group.iloc[i]['idx'] == group.iloc[j]['idx']:
                continue  # 跳过相同车辆

            line1_points = [(group.iloc[i]['start_time'].timestamp(), group.iloc[i]['distance_of_gpspoint']),
                            (group.iloc[i]['end_time'].timestamp(), group.iloc[i]['distance_of_gpspoint'])]
            line2_points = [(group.iloc[j]['start_time'].timestamp(), group.iloc[j]['distance_of_gpspoint']),
                            (group.iloc[j]['end_time'].timestamp(), group.iloc[j]['distance_of_gpspoint'])]
            midpoint_line2 = ((line2_points[0][0] + line2_points[1][0]) / 2,
                              (line2_points[0][1] + line2_points[1][1]) / 2)

            distance = calculate_max_vertical_distance_between_lines(line1_points,
                                                                     [line2_points[0], midpoint_line2, line2_points[1]])

            min_time = min(group.iloc[i]['start_time'], group.iloc[j]['start_time'])
            max_time = max(group.iloc[i]['end_time'], group.iloc[j]['end_time'])
            duration = (max_time - min_time).total_seconds()

            if distance < threshold:
                bunching_events.append({
                    'lineName': group.iloc[i]['lineName'],
                    'direction': group.iloc[i]['direction'],
                    'idx1': group.iloc[i]['idx'],
                    'idx2': group.iloc[j]['idx'],
                    'win_st': group.iloc[i]['win_st'],
                    'distance': distance,
                    'start_time': min_time,
                    'end_time': max_time,
                    'duration': duration
                })

    return pd.DataFrame(bunching_events)


# 检测公交车串车
def detect_bus_bunching(data, threshold):
    def process_group_wrapper(pdf):
        return process_group(pdf, threshold)

    result = data.groupBy('lineName', 'win_st', 'direction').applyInPandas(process_group_wrapper, schema="""
        lineName string,
        direction double,
        idx1 string,
        idx2 string,
        win_st int,
        distance double,
        start_time timestamp,
        end_time timestamp,
        duration double
    """)

    # 过滤掉 idx1 与 idx2 相同的数据
    result = result.filter(result['idx1'] != result['idx2'])

    # 选择每组中 `distance` 值最大的记录
    window_spec = Window.partitionBy('lineName', 'win_st', 'direction', 'idx1', 'idx2').orderBy(col('distance').desc())
    result = result.withColumn('rank', rank().over(window_spec)).filter(col('rank') == 1).drop('rank')

    return result


def merge_bunching_events_spark(events_df):
    # 按 win_st 排序
    window_spec = Window.partitionBy('idx1', 'idx2').orderBy('win_st')

    # 创建下一个事件的列
    events_df = events_df.withColumn('next_start_time', F.lead('start_time').over(window_spec)) \
        .withColumn('next_end_time', F.lead('end_time').over(window_spec)) \
        .withColumn('next_win_st', F.lead('win_st').over(window_spec))

    # 计算两个事件之间的时间差
    events_df = events_df.withColumn('time_diff', (F.unix_timestamp('next_start_time') - F.unix_timestamp('end_time')))

    # 标记需要合并的事件
    events_df = events_df.withColumn('merge', when(
        (col('idx1') == F.lead('idx1', 1).over(window_spec)) &
        (col('idx2') == F.lead('idx2', 1).over(window_spec)) &
        (col('time_diff') < 5), lit(1)
    ).otherwise(lit(0)))

    # 合并事件
    merged_events = events_df.withColumn('merged_end_time', when(
        col('merge') == 1, F.lead('end_time', 1).over(window_spec)).otherwise(col('end_time'))) \
        .withColumn('merged_duration', when(
        col('merge') == 1,
        (F.unix_timestamp(F.lead('end_time', 1).over(window_spec)) - F.unix_timestamp('start_time'))).otherwise(
        F.unix_timestamp('end_time') - F.unix_timestamp('start_time'))) \
        .withColumn('merged_win_st', when(
        col('merge') == 1, concat_ws(',', col('win_st'), F.lead('win_st', 1).over(window_spec))).otherwise(
        col('win_st')))

    # 过滤掉合并后持续时间小于3分钟的事件
    filtered_merged_df = merged_events.filter((col('merged_duration') / 60) >= 3)

    # 选取需要的列
    final_df = filtered_merged_df.select(
        'lineName', 'direction', 'idx1', 'idx2', 'win_st', 'distance', 'start_time', 'merged_end_time',
        'merged_duration'
    ).withColumnRenamed('merged_end_time', 'end_time').withColumnRenamed('merged_duration', 'duration')

    return final_df


if __name__ == "__main__":
    # 记录开始时间
    start_time = time.time()

    os.environ['PYSPARK_PYTHON'] = '/home/lrr/softs/anaconda3/bin/python3.11'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/lrr/softs/anaconda3/bin/python3.11'
    # spark = SparkSession.builder \
    #     .master("local[*]") \
    #     .appName("BusBunchingDetection") \
    #     .config("spark.executor.memory", "8g") \
    #     .config("spark.driver.memory", "8g") \
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
    print("原始数据：")
    fdata.limit(2).show()

    # 分区数量设置
    fdata = fdata.repartition(32)

    partitions_info = partition_info(fdata)

    assign_to_windows_udf = udf(assign_to_windows, ArrayType(IntegerType()))
    fdata = fdata.withColumn("win_st_end", assign_to_windows_udf(col("time")))

    # 将时间窗口划分为起始时间win_st以及终止时间win_end
    fdata = fdata.withColumn("win_st", col("win_st_end")[0]) \
        .withColumn("win_end", col("win_st_end")[1])

    # 定义start_time和end_time列（此示例使用'time'列）
    fdata = fdata.withColumn("start_time", col("time"))

    # 将数据按照lineName和idx进行分组，再按照time排序，将每个分组中下一个gps点的起始时间作为前一个gps点的end_time
    fdata = fdata.withColumn("end_time", lead("time", 1).over(Window.partitionBy("lineName", "idx").orderBy("time")))

    # 过滤数据以进行处理
    # 对每辆车有多少时间窗口进行计数
    windowed_data = fdata.groupBy("lineName", "idx", "win_st").count()

    # 对时间窗口小于2的数据进行过滤
    filtered_idx = windowed_data.filter(col("count") > 1).select("lineName", "idx", "win_st")

    # 通过内连接保存fdta和filtered_idx都存在的行，以此来起到过滤数据的作用
    fdata_filtered = fdata.join(filtered_idx, ["lineName", "idx", "win_st"], "inner").dropna(
        subset=["end_time", "start_time"])

    # 运行检测
    threshold = 0.01
    bunching_df = detect_bus_bunching(fdata_filtered, threshold)

    merged_bunching_df = merge_bunching_events_spark(bunching_df)

    merged_bunching_df.limit(2).show()

    merged_bunching_df.show()
