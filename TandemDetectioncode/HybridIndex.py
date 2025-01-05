import os
import time
import pyspark.sql.functions as F
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import udf, first, last, lead, rank
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, ArrayType
from pyspark.sql.functions import col, concat_ws, lit, when
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

# 展示每个分区的数据量
def partition_info(df):
    def inner(iterator):
        yield len(list(iterator))

    return df.rdd.mapPartitions(inner).collect()
def assign_to_windows(time):
    minutes_since_midnight = (time - datetime(time.year, time.month, time.day)).total_seconds() / 60.0
    window_start_minute = int(minutes_since_midnight // 4 * 4)
    window_end_minute = window_start_minute + 4
    return [window_start_minute, window_end_minute]

# 定义UDF计算与水平线的夹角
def calculate_right_angle_with_horizontal(end_dis, start_dis):
    return float(np.degrees(np.arctan2(float(end_dis) - float(start_dis), 1)) % 360)
# 定义计算MBR的函数
def calculate_mbrs(group):
    min_time = group['time'].min()
    max_time = group['time'].max()
    min_dis_location = group['gps_normalized_distance_length'].min()
    max_dis_location = group['gps_normalized_distance_length'].max()
    return pd.Series([min_time, max_time, min_dis_location, max_dis_location],
                     index=['min_time', 'max_time', 'min_dis_location', 'max_dis_location'])
# 定义处理每个组的函数
def process_group(group, threshold):
    bunching_events = []

    # 计算由两点确定的直线参数（斜率和截距）
    def calculate_line_parameters(point1, point2):
        x_coords, y_coords = [point1[0], point2[0]], [point1[1], point2[1]]
        A = np.vstack([x_coords, np.ones(len(x_coords))]).T
        m, c = np.linalg.lstsq(A, y_coords, rcond=None)[0]
        return m, c

    # 计算点到直线的距离
    def point_to_line_distance(point, line_points):
        x0, y0 = point
        x1, y1 = line_points[0]
        x2, y2 = line_points[1]
        m, c = calculate_line_parameters((x1, y1), (x2, y2))
        A, B, C = -m, 1, -c
        distance = abs(A * x0 + B * y0 + C) / (np.sqrt(A ** 2 + B ** 2))
        return distance

    # 计算两条线段之间的最大垂直距离
    def calculate_max_vertical_distance_between_lines(line1_points, line2_points):
        point1_line2, point2_line2, midpoint_line2 = line2_points
        distance_start = point_to_line_distance(point1_line2, line1_points)
        distance_mid = point_to_line_distance(midpoint_line2, line1_points)
        distance_end = point_to_line_distance(point2_line2, line1_points)
        return max(distance_start, distance_mid, distance_end)

    # 检查MBR是否有交集
    def mbrs_intersect(mbr1, mbr2):
        return not (mbr1['max_time'] < mbr2['min_time'] or mbr1['min_time'] > mbr2['max_time'] or
                    mbr1['max_dis_location'] < mbr2['min_dis_location'] or mbr1['min_dis_location'] > mbr2[
                        'max_dis_location'])

    group = group.sort_values(by='angle')
    lines = group.groupby('idx').apply(calculate_mbrs).reset_index()
    lines = pd.merge(lines, group[
        ['idx', 'time', 'gps_normalized_distance_length', 'angle', 'lineName', 'direction', 'win_st']], on='idx')

    # 重命名列以避免冲突
    lines = lines.rename(columns={'time': 'start_time', 'gps_normalized_distance_length': 'start_dis'})
    lines['end_time'] = lines['start_time']
    lines['end_dis'] = lines['start_dis']

    for i in range(len(lines) - 1):
        if lines.iloc[i]['idx'] == lines.iloc[i + 1]['idx']:
            continue  # 跳过相同车辆的情况
        angle_diff = abs(lines.iloc[i + 1]['angle'] - lines.iloc[i]['angle'])
        if angle_diff <= 3:
            mbr1 = lines.iloc[i]
            mbr2 = lines.iloc[i + 1]

            if mbrs_intersect(mbr1, mbr2):
                line1_points = [(mbr1['start_time'].timestamp(), mbr1['start_dis']),
                                (mbr1['end_time'].timestamp(), mbr1['end_dis'])]
                line2_points = [(mbr2['start_time'].timestamp(), mbr2['start_dis']),
                                (mbr2['end_time'].timestamp(), mbr2['end_dis'])]
                midpoint_line2 = ((line2_points[0][0] + line2_points[1][0]) / 2,
                                  (line2_points[0][1] + line2_points[1][1]) / 2)
                distance = calculate_max_vertical_distance_between_lines(line1_points, [line2_points[0], midpoint_line2,
                                                                                        line2_points[1]])

                min_time = min(mbr1['min_time'], mbr2['min_time'])
                max_time = max(mbr1['max_time'], mbr2['max_time'])
                duration = (max_time - min_time).total_seconds()  # 转换timedelta为秒

                if distance < threshold:
                    bunching_events.append({
                        'lineName': mbr1['lineName'],
                        'direction': mbr1['direction'],
                        'idx1': mbr1['idx'],
                        'idx2': mbr2['idx'],
                        'win_st': mbr1['win_st'],
                        'distance': distance,
                        'start_time': min_time,
                        'end_time': max_time,
                        'duration': duration
                    })

    return pd.DataFrame(bunching_events)
# 定义检测公交车串车的函数
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
    # DataPath = "/home/lrr/jupyter/LRR_data/data_20_xin/*.csv"
    DataPath = "/home/lrr/LRR_data/code_data/renamed_bus_20/*.csv"
    fdata = spark.read.format("csv").schema(schema).option("header", value=True).option("delimiter", ",").load(DataPath)
    # fdata.show()
    # 将数据按lineName字段分区
    fdata = fdata.repartition("lineName")

    partitions_info = partition_info(fdata)

    # print(f"Number of rows per partition: {partitions_info}")

    assign_to_windows_udf = udf(assign_to_windows, ArrayType(IntegerType()))
    fdata = fdata.withColumn("win_st_end", assign_to_windows_udf(col("time")))

    fdata = fdata.withColumn("win_st", col("win_st_end")[0]) \
        .withColumn("win_end", col("win_st_end")[1])

    # 定义start_time和end_time列（此示例使用'time'列）
    fdata = fdata.withColumn("start_time", col("time"))
    fdata = fdata.withColumn("end_time", lead("time", 1).over(Window.partitionBy("lineName", "idx").orderBy("time")))

    # 过滤数据以进行处理
    windowed_data = fdata.groupBy("lineName", "idx", "win_st").count()
    filtered_idx = windowed_data.filter(col("count") > 1).select("lineName", "idx", "win_st")
    fdata_filtered = fdata.join(filtered_idx, ["lineName", "idx", "win_st"], "inner")

    # fdata_filtered.show()
    calculate_right_angle_with_horizontal_udf = udf(calculate_right_angle_with_horizontal, DoubleType())

    # 计算起始和结束距离
    start_dis = fdata_filtered.groupBy("idx", "win_st").agg(first("gps_normalized_distance_length").alias("start_dis"))
    end_dis = fdata_filtered.groupBy("idx", "win_st").agg(last("gps_normalized_distance_length").alias("end_dis"))

    # 连接距离并计算角度
    dis_positions = start_dis.join(end_dis, ["idx", "win_st"])
    dis_positions = dis_positions.withColumn("angle",
                                             calculate_right_angle_with_horizontal_udf(col("end_dis"),
                                                                                       col("start_dis")))
    fdata_filtered = fdata_filtered.join(dis_positions.select("idx", "win_st", "angle"), ["idx", "win_st"], "inner")
    # 检测公交车串车
    threshold = 0.01
    bunching_df = detect_bus_bunching(fdata_filtered, threshold)
    # bunching_df.show()

    # 合并串车事件
    merged_bunching_df = merge_bunching_events_spark(bunching_df)

    # 显示合并后的串车事件
    merged_bunching_df.show()

    # 记录结束时间
    end_time = time.time()

    # 计算并显示运行时间
    print(f"Total execution time: {end_time - start_time} seconds")

    # 停止SparkSession
    spark.stop()