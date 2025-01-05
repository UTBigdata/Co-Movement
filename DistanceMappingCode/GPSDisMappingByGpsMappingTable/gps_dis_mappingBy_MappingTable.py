import multiprocessing
import os
import time
from multiprocessing import Pool, cpu_count

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point


# 计算归一化 GPS 距离的函数
def gpsdistance_normalization(args):
    car_gps_df, opath_distance, gdf, longest_path_distance = args
    gps_distance_list = []  # 存储归一化 GPS 距离的列表
    for gps_point in car_gps_df.itertuples():  # 遍历每个 GPS 点
        # 获取当前 GPS 点对应的路径信息
        target_gps_opath = opath_distance[opath_distance["opath"] == gps_point[2]]
        if not target_gps_opath.empty:
            # 获取当前 GPS 点所在路径的几何线
            opath_linestring = gdf[gdf["fid"] == gps_point[2]]["geometry"].iloc[0]
            linestring_coords = list(opath_linestring.coords)
            point_gps = Point(gps_point[4], gps_point[5])  # 当前 GPS 点的坐标

            min_distance_sum = float('inf')  # 最小距离和初始化为无穷大
            closest_pair = None  # 最近点对初始化为空
            start_distance = 0  # 起始距离初始化为0

            # 遍历路径线段的每一段
            for i in range(len(linestring_coords) - 1):
                start = Point(linestring_coords[i])  # 当前段起始点
                end = Point(linestring_coords[i + 1])  # 当前段终点
                distance1 = point_gps.distance(start)  # GPS 点到起始点的距离
                distance2 = point_gps.distance(end)  # GPS 点到终点的距离
                distance_sum = distance1 + distance2  # 距离之和

                # 更新最小距离和及最近点对
                if distance_sum < min_distance_sum:
                    min_distance_sum = distance_sum
                    closest_pair = (start.coords[0], end.coords[0])
                    start_distance = distance1  # 更新起始距离

            if closest_pair:
                # 获取前一个数据点到当前最近点的路径距离
                pre_datum_point_distance = target_gps_opath.loc[
                    (target_gps_opath["lng"] == closest_pair[0][0]) &
                    (target_gps_opath["lat"] == closest_pair[0][1]) &
                    (target_gps_opath["opath"] == gps_point[2]), "distance_opath"].iloc[0]

                # 计算当前 GPS 点到路径上的距离
                distance_of_gpspoint = pre_datum_point_distance + start_distance
                # 计算归一化的 GPS 距离长度
                gps_normalized_distance_length = distance_of_gpspoint / longest_path_distance
                # 将结果添加到列表中
                gps_distance_list.append((
                    gps_point[1], gps_point[2], gps_point[3], gps_point[9],
                    gps_point[8], gps_point[6], gps_point[4], gps_point[5],
                    distance_of_gpspoint, gps_normalized_distance_length))

    return gps_distance_list


# 处理每辆车 GPS 数据的函数
def process_car(car_name, gps_path):
    # 读取每辆车的 GPS 数据
    car_gps_data = gpd.read_file(os.path.join(gps_path, car_name, f"{car_name}.shp"))
    car_gps_list_1 = []
    car_gps_list_2 = []

    # 根据数据格式整理车辆的 GPS 数据
    for row in car_gps_data.itertuples():
        if row[9] == 1:
            car_gps_list_1.append(row[1:])
        else:
            car_gps_list_2.append(row[1:])

    # 构建 DataFrame 存储车辆的 GPS 数据
    car_gps_df_1 = pd.DataFrame(car_gps_list_1, columns=["idx", "opath", "lineName", "lng", "lat",
                                                         "time", "pgeom", "journey", "direction", "geometry"])
    car_gps_df_2 = pd.DataFrame(car_gps_list_2, columns=["idx", "opath", "lineName", "lng", "lat",
                                                         "time", "pgeom", "journey", "direction", "geometry"])

    return car_gps_df_1, car_gps_df_2


# 并行处理 GPS 数据的函数
def process_in_parallel(car_gps_df, opath_distance, gdf, longest_path_distance):
    num_cores = cpu_count()  # 获取 CPU 核心数
    df_split = np.array_split(car_gps_df, num_cores)  # 将数据分割成多个子集

    with Pool(num_cores) as pool:
        # 使用多进程并行处理 GPS 距离归一化计算
        results = pool.map(gpsdistance_normalization,
                           [(df, opath_distance, gdf, longest_path_distance) for df in df_split])

    normalized_result = [item for sublist in results for item in sublist]  # 合并结果列表
    return normalized_result


# 获取每辆车 GPS 数据的函数
def multi_get_gps_directionDF(gps_path):
    gps_files = os.listdir(gps_path)  # 获取 GPS 数据文件列表
    pool = multiprocessing.Pool()  # 创建进程池
    results = [pool.apply_async(process_car, args=(car_name, gps_path)) for car_name in gps_files]
    pool.close()
    pool.join()

    # 整理所有车辆的 GPS 数据
    all_car_gps_df_1 = pd.concat([result.get()[0] for result in results])
    all_car_gps_df_2 = pd.concat([result.get()[1] for result in results])

    return all_car_gps_df_1, all_car_gps_df_2


# 主执行部分
if __name__ == "__main__":
    # result_file_path = "/home/wcw/data/selected_line_information/selected_line_information.csv"
    # result_file_path = "/home/wcw/data/selected_line_information/selected_line_information.csv"
    # infor_DF2 = pd.read_csv(result_file_path, dtype={"bus_line_name（线路名称）": str})  # 读取结果文件

    # root_file_path = "/home/wcw/data/selected_data"
    root_file_path = "/home/lrr/LRR_data/code_data/unMappingData"
    file_name_list = os.listdir(root_file_path)  # 获取线路文件列表

    for name_of_line in file_name_list:
        t1 = time.time()  # 记录处理开始时间
        print(f"处理线路: {name_of_line}")

        # 设置保存结果的路径和读取路径
        result_save_path = os.path.join(root_file_path, name_of_line, "result")
        line_network_path = os.path.join(root_file_path, name_of_line, "networks")
        gps_file_path = os.path.join(root_file_path, name_of_line, "cars")

        # 获取车辆的 GPS 数据
        car_gps_df_1, car_gps_df_2 = multi_get_gps_directionDF(gps_file_path)
        num_gps_points = len(car_gps_df_1) + len(car_gps_df_2)  # 计算 GPS 点数量

        car_file_path = os.path.join(root_file_path, name_of_line, "cars")
        car_name_list = os.listdir(car_file_path)
        num_cars = len(car_name_list)  # 计算车辆数量

        # 读取路径网络和路径距离信息
        opath_distance_1 = pd.read_csv(os.path.join(line_network_path, "direction1", "network_distance.csv"))
        opath_distance_2 = pd.read_csv(os.path.join(line_network_path, "direction2", "network_distance.csv"))

        net_file1 = os.path.join(line_network_path, "direction1", f"{name_of_line}.shp")
        net_file2 = os.path.join(line_network_path, "direction2", f"{name_of_line}.shp")

        gdf1 = gpd.read_file(net_file1)
        gdf2 = gpd.read_file(net_file2)

        # 获取最长路径距离
        longest_path_distance1 = opath_distance_1.iloc[-1]["distance_opath"]
        longest_path_distance2 = opath_distance_2.iloc[-1]["distance_opath"]

        # 并行处理 GPS 数据并计算归一化 GPS 距离
        data1 = process_in_parallel(car_gps_df_1, opath_distance_1, gdf1, longest_path_distance1)
        data2 = process_in_parallel(car_gps_df_2, opath_distance_2, gdf2, longest_path_distance2)

        all_gps_normalized_distance_length = data1 + data2

        # 构建 DataFrame 存储所有车辆的归一化 GPS 距离
        df_all_gps_normalized_distance_length = pd.DataFrame(all_gps_normalized_distance_length, columns=[
            "idx", "opath", "lineName", "direction", "t_flag", "time", "lng", "lat",
            "distance_of_gpspoint", "gps_normalized_distance_length"])

        if not os.path.exists(result_save_path):
            os.makedirs(result_save_path)

        save_path = os.path.join(result_save_path, f"{name_of_line}_gpsdistance_normalized.csv")

        # 保存处理后的结果至 CSV 文件
        df_all_gps_normalized_distance_length.to_csv(save_path, index=False)
        print(f"保存处理后的数据至: {save_path}")

        t2 = time.time()
        t = t2 - t1
        time_compute = t  # 计算处理时间
        print(f"线路{name_of_line}耗时：",time_compute)
    #     # 更新结果文件中的相关信息
    #     mask = infor_DF2['bus_line_name（线路名称）'] == name_of_line
    #     infor_DF2.loc[mask, 'bus_car_num（辆）'] = num_cars
    #     infor_DF2.loc[mask, 'bus_gps_num（个）'] = num_gps_points
    #     infor_DF2.loc[mask, 'time_compute_gps_distance（秒）'] = time_compute
    #
    # # 保存更新后的结果文件
    # if os.path.exists(result_file_path):
    #     os.remove(result_file_path)
    # infor_DF2.to_csv(result_file_path, index=False,encoding='utf-8')
