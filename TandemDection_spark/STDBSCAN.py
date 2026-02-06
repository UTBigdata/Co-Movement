import sys
import os
import time
import warnings
import numpy as np
import pandas as pd
from time import perf_counter
from pyspark.storagelevel import StorageLevel

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DoubleType,
    TimestampType, TimestampNTZType
)
from pyspark.storagelevel import StorageLevel

from scipy.spatial.distance import pdist, squareform
from sklearn.cluster import DBSCAN
from sklearn.utils import check_array
from scipy.sparse import coo_matrix
from sklearn.neighbors import NearestNeighbors

from tools_wcw import *  # 你原来的装饰器等工具

R = 6371.0


def haversine(u, v):
    lat1, lon1 = np.radians(u)
    lat2, lon2 = np.radians(v)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))


class ST_DBSCAN():
    def __init__(self,
                 eps1=0.5,
                 eps2=10,
                 min_samples=5,
                 metric='euclidean',
                 n_jobs=-1):
        self.eps1 = eps1
        self.eps2 = eps2
        self.min_samples = min_samples
        self.metric = metric
        self.n_jobs = n_jobs

    def fit(self, X):
        X = check_array(X)

        if not self.eps1 > 0.0 or not self.eps2 > 0.0 or not self.min_samples > 0.0:
            raise ValueError('eps1, eps2, minPts must be positive')

        n, m = X.shape

        if n < 20000:
            time_dist = pdist(X[:, 0].reshape(n, 1), metric=self.metric)
            euc_dist = pdist(X[:, 1:], metric=haversine)
            dist = np.where(time_dist <= self.eps2, euc_dist, 2 * self.eps1)

            db = DBSCAN(eps=self.eps1,
                        min_samples=self.min_samples,
                        metric='precomputed')
            db.fit(squareform(dist))
            self.labels = db.labels_
        else:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")

                coords_rad = np.radians(X[:, 1:])

                nn_spatial = NearestNeighbors(metric='haversine', radius=self.eps1 / R)
                nn_spatial.fit(coords_rad)
                euc_sp = nn_spatial.radius_neighbors_graph(coords_rad, mode='distance')
                euc_sp = euc_sp * R

                nn_time = NearestNeighbors(metric=self.metric, radius=self.eps2)
                nn_time.fit(X[:, 0].reshape(n, 1))
                time_sp = nn_time.radius_neighbors_graph(X[:, 0].reshape(n, 1), mode='distance')

                row = time_sp.nonzero()[0]
                column = time_sp.nonzero()[1]
                v = np.array(euc_sp[row, column])[0]

                dist_sp = coo_matrix((v, (row, column)), shape=(n, n))
                dist_sp = dist_sp.tocsc()
                dist_sp.eliminate_zeros()

                db = DBSCAN(eps=self.eps1,
                            min_samples=self.min_samples,
                            metric='precomputed')
                db.fit(dist_sp)
                self.labels = db.labels_

        return self


@udf(returnType=IntegerType())
def calculate_time_window(timestamp, win_size=4):
    SECONDS_PER_DAY = 86400
    days_since_epoch = timestamp // SECONDS_PER_DAY
    midnight_seconds = days_since_epoch * SECONDS_PER_DAY
    seconds_since_midnight = timestamp - midnight_seconds
    minutes_since_midnight = seconds_since_midnight // 60
    return minutes_since_midnight // win_size * win_size


def calculate_bunching(group_data: pd.DataFrame):
    result_temp = {
        "lineName": [], "direction": [], "idx1": [], "idx2": [],
        "start_time": [], "end_time": [], "duration": []
    }

    idx_list: list = group_data['idx'].drop_duplicates().tolist()
    if len(idx_list) == 1:
        return pd.DataFrame(result_temp)

    group_data = group_data.reset_index(drop=True)

    idx_list_filter = []
    for idx in idx_list:
        temp = group_data[group_data['idx'] == idx]
        if len(temp) > 1:
            idx_list_filter.append(idx)

    if len(idx_list_filter) < 2:
        return pd.DataFrame(result_temp)

    for i in range(len(idx_list_filter)):
        idx_i = idx_list_filter[i]
        idx_i_data = group_data[group_data['idx'] == idx_i]
        for j in range(i + 1, len(idx_list_filter)):
            idx_j = idx_list_filter[j]
            idx_j_data = group_data[group_data['idx'] == idx_j]

            idx1, idx2 = (idx_i, idx_j) if idx_i < idx_j else (idx_j, idx_i)
            start_time = max(idx_i_data['time'].min(), idx_j_data['time'].min())
            end_time = min(idx_i_data['time'].max(), idx_j_data['time'].max())

            duration = end_time - start_time
            if duration > 0:
                result_temp["lineName"].append(group_data.loc[0, "lineName"])
                result_temp["direction"].append(group_data.loc[0, "direction"])
                result_temp["idx1"].append(idx1)
                result_temp["idx2"].append(idx2)
                result_temp["start_time"].append(start_time)
                result_temp["end_time"].append(end_time)
                result_temp["duration"].append(duration)
    return pd.DataFrame(result_temp)


def trajectory_cluster(sdf, distance_eps, time_eps, min_points):
    output_schema = StructType([
        StructField("lineName", StringType(), True),
        StructField("direction", IntegerType(), True),
        StructField("idx1", StringType(), True),
        StructField("idx2", StringType(), True),
        StructField("start_time", TimestampNTZType(), True),
        StructField("end_time", TimestampNTZType(), True),
        StructField("duration", DoubleType(), True)
    ])

    def process_group_data_wrapper(pdf: pd.DataFrame):
        st_dbscan = ST_DBSCAN(eps1=distance_eps, eps2=time_eps, min_samples=min_points)
        st_dbscan.fit(pdf[['time', 'lat', 'lng']])

        pdf["clusterID"] = st_dbscan.labels
        pdf = pdf[pdf["clusterID"] != -1]

        if pdf.empty:
            return pd.DataFrame(columns=[col.name for col in output_schema])

        bunching_events = pdf.groupby("clusterID").apply(calculate_bunching).reset_index(drop=True)
        bunching_events["start_time"] = pd.to_datetime(bunching_events["start_time"], unit='s')
        bunching_events["end_time"] = pd.to_datetime(bunching_events["end_time"], unit='s')
        return bunching_events

    result = sdf.groupBy("lineName", "direction", "win_start").applyInPandas(
        process_group_data_wrapper,
        schema=output_schema
    )
    return result


def merge_bunching_events(events_df, win_size):
    output_schema = StructType([
        StructField("lineName", StringType(), True),
        StructField("direction", IntegerType(), True),
        StructField("idx1", StringType(), True),
        StructField("idx2", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration", IntegerType(), True),
    ])

    def process_group_data(group_data: pd.DataFrame):
        group_data = group_data.sort_values(by="start_time").reset_index(drop=True)
        group_data['last_end_time'] = group_data["end_time"].shift(1)
        group_data['time_diff'] = (group_data['start_time'] - group_data['last_end_time']).dt.total_seconds()

        group_data["merge_flag"] = 0
        for row in group_data.itertuples():
            if row.Index == 0:
                continue
            if row.time_diff >= win_size * 60:
                group_data.loc[row.Index, 'merge_flag'] = 1

        for row in group_data.itertuples():
            if row.Index == 0:
                continue
            group_data.loc[row.Index, 'merge_flag'] = group_data.loc[row.Index - 1, 'merge_flag'] + row.merge_flag

        result = []
        line_name = group_data.loc[0, "lineName"]
        direction = group_data.loc[0, "direction"]
        idx1 = group_data.loc[0, "idx1"]
        idx2 = group_data.loc[0, "idx2"]

        for flag in range(group_data.loc[len(group_data) - 1, "merge_flag"] + 1):
            filter_group_data = group_data[group_data["merge_flag"] == flag]
            start_time = filter_group_data['start_time'].min()
            end_time = filter_group_data['end_time'].max()
            duration = (end_time - start_time).total_seconds()
            if duration >= 180:
                result.append(
                    {
                        "lineName": line_name,
                        "direction": direction,
                        "idx1": idx1,
                        "idx2": idx2,
                        "start_time": start_time,
                        "end_time": end_time,
                        "duration": duration,
                    })
        return pd.DataFrame(result)

    final_df = events_df.groupBy("lineName", "direction", "idx1", "idx2").applyInPandas(
        process_group_data,
        schema=output_schema
    )
    return final_df


def _materialize(df, name: str, do_cache: bool = True):
    """
    Spark 惰性执行：必须用 action（如 count）触发，才能真实计时。
    这里用 persist+count 把某阶段的计算“物化”，并返回耗时。
    """
    start = time.time()
    if do_cache:
        df.persist(StorageLevel.MEMORY_AND_DISK)
    n = df.count()
    cost = time.time() - start
    print(f"[TIMER] {name:<8} | count={n:<12d} | time={cost:.3f}s")
    return df, cost, n


@time_consume
def calculater_serial_vehicle(data_path):
    # ========= 计时起点（三个阶段将覆盖从这里到函数结束的全部时间） =========
    t0 = perf_counter()

    # =========================
    # Phase 1: project
    # =========================
    spark = SparkSession.builder \
        .master("local[64]") \
        .appName("BusBunchingDetection_Optimized") \
        .config("spark.executor.cores", "64") \
        .config("spark.executor.memory", "128g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "350") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    RADIUS = 0.01
    MIN_PTS = 4
    MAX_TIME_GAP = 4
    WIN_SIZE = 4
    TIME_THRESHOLD = 60

    schema = StructType([
        StructField("idx", StringType(), True),
        StructField("opath", StringType(), True),
        StructField("lineName", StringType(), True),
        StructField("direction", FloatType(), True),
        StructField("t_flag", IntegerType(), True),
        StructField("time", TimestampNTZType(), True),
        StructField("lng", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("distance_of_gpspoint", DoubleType(), True),
        StructField("gps_normalized_distance_length", DoubleType(), True)
    ])

    sdf_project = spark.read.format("csv") \
        .schema(schema) \
        .option("header", value=True) \
        .option("delimiter", ",") \
        .load(data_path) \
        .drop("opath", "t_flag", "gps_normalized_distance_length", "distance_of_gpspoint") \
        .withColumn("time", F.unix_timestamp("time")) \
        .repartition("lineName") \
        .withColumn("win_start", calculate_time_window(F.col("time"), F.lit(WIN_SIZE)))

    sdf_project.persist(StorageLevel.MEMORY_AND_DISK)
    n_project = sdf_project.count()  # 触发 project 的真实计算与缓存填充

    t1 = perf_counter()
    t_project = t1 - t0
    print(f"[TIMER] project  | count={n_project:<12d} | time={t_project:.3f}s")

    # =========================
    # Phase 2: filter（从 t1 到 t2）
    # =========================
    filtered_statistics = sdf_project.groupBy("lineName", "idx", "direction", "win_start").count()
    filtered_basis = filtered_statistics.filter(F.col("count") > 1) \
        .select("lineName", "idx", "direction", "win_start")

    sdf_filtered = sdf_project.join(
        filtered_basis, ["lineName", "idx", "direction", "win_start"], "inner"
    )

    sdf_filtered.persist(StorageLevel.MEMORY_AND_DISK)
    n_filter = sdf_filtered.count()  # 触发 filter 的真实计算与缓存填充

    t2 = perf_counter()
    t_filter = t2 - t1
    print(f"[TIMER] filter   | count={n_filter:<12d} | time={t_filter:.3f}s")

    # project 缓存不用了可以释放（这一步也算在 refine 里或 filter 里都行；我放这里无所谓）
    sdf_project.unpersist(blocking=False)

    # =========================
    # Phase 3: refine（从 t2 到函数结束，把 merge + 最终 action + spark.stop 都算进去）
    # =========================
    result = trajectory_cluster(sdf_filtered, RADIUS, TIME_THRESHOLD, MIN_PTS)
    merged_result = merge_bunching_events(result, MAX_TIME_GAP)

    total_results = merged_result.count()  # 触发 refine 的真实计算
    sdf_filtered.unpersist(blocking=False)

    spark.stop()

    t3 = perf_counter()
    total_time = t3 - t0


    t_refine = total_time - t_project - t_filter

    print(f"[TIMER] refine   | count={total_results:<12d} | time={t_refine:.3f}s")

    print("\n========== Stage Timing Summary ==========")
    print(f"project : {t_project:.3f}s (rows={n_project})")
    print(f"filter  : {t_filter:.3f}s (rows={n_filter})")
    print(f"refine  : {t_refine:.3f}s (results={total_results})")
    print("-----------------------------------------")
    print(f"total   : {total_time:.3f}s (== sum of three stages)")
    print("=========================================\n")

    spark.stop()


if __name__ == "__main__":
    PYTHON_PATH = sys.executable
    os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
    os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH

    #     # data_path = "/lc/LRR_data/data_20_xin/*.csv"
    #     # data_path = "file:///home/lrr/lc_datas/gps_data/4/*.csv"
    #     # data_path = "file:///home/lrr/lc_datas/gps_data/8/*.csv"
    #     # data_path = "file:///home/lrr/lc_datas/gps_data/12/*.csv"
    #     # data_path = "file:///home/lrr/lc_datas/gps_data/16/*.csv"
    #     # data_path = "file:///home/lrr/lc_datas/gps_data/20/*.csv"
    # data_path = 'file:///home/lrr/jupyter/LRR_data/data_20_xin/M2333_gpsdistance_normalized.csv'
    data_path = "file:///home/lrr/lc_datas/gps_data/160/*.csv"
    calculater_serial_vehicle(data_path)
