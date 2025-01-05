import os
import time
from multiprocessing import Pool, cpu_count

import geopandas as gpd
import pandas as pd
from geopy import distance


def compute_distance_network(args):
    # 定义计算每条线路路网长度的函数
    file_name, root_file_path = args
    direction_list = os.listdir(os.path.join(root_file_path, file_name, "networks"))

    # 初始化计时器
    t1 = time.time()

    # 遍历上行和下行两个方向的路网
    for direction in direction_list:
        # 构建文件路径
        net_file = os.path.join(root_file_path, file_name, f"networks/{direction}/{file_name}.shp")
        net_opath = os.path.join(root_file_path, file_name, f"networks/{direction}/{file_name}.csv")

        # 存储计算结果的列表
        target_result = []

        # 读取路网和路段序列
        net_gdf = gpd.read_file(net_file)
        net_direction = net_gdf.iloc[0]["direction"]
        net_opath_seq = pd.read_csv(net_opath)
        opath_list = net_opath_seq["opath"].tolist()

        # 计算每个路段的长度
        correct_miles = 0
        distance_opath = 0
        start_demo = 0

        # todo 在此处判断direction是哪个方向的，如果direction为2方向，把opath反过来算，以此来让回程距离从1到0，而去程则是从0到1
        for opath in opath_list:
            target_opath_gdf = net_gdf[net_gdf["fid"] == opath]
            target_gps_geometry = target_opath_gdf["geometry"]

            for line in target_gps_geometry:
                for lng, lat in list(line.coords):
                    if start_demo == 0:
                        pre_gps = (lat, lng)
                    distance_opath += distance.distance(pre_gps, (lat, lng)).km
                    pre_gps = (lat, lng)
                    start_demo = 1

                    target_result.append((net_direction, opath, lng, lat, distance_opath))

            correct_miles += sum(target_opath_gdf["length"])

        # 构建结果DataFrame并保存为CSV文件
        target_result_df = pd.DataFrame(target_result,
                                        columns=["net_direction", "opath", "lng", "lat", "distance_opath"])
        out_file = os.path.join(root_file_path, file_name, f"networks/{direction}/network_distance.csv")

        if os.path.exists(out_file):
            os.remove(out_file)
        target_result_df.to_csv(out_file, sep=",", header=True, index=False)

        # 计算方向1和方向2的总路网长度
        if direction == "direction1":
            distance_dir1 = target_result_df.iloc[-1]["distance_opath"]
        else:
            distance_dir2 = target_result_df.iloc[-1]["distance_opath"]

    # 计算总时间并返回结果
    t2 = time.time()
    compute_time = t2 - t1
    return file_name, "", "", compute_time, "", distance_dir1, distance_dir2


def multilprocess_compute_distance_network(linename_list, root_file_path):
    # 使用多进程计算所有线路的路网长度信息
    infor_list = []
    num_cores = cpu_count()

    with Pool(num_cores) as pool:
        infor_data = pool.map(compute_distance_network, [(linename, root_file_path) for linename in linename_list])

    for data in infor_data:
        infor_list.append(data)

    return infor_list


if __name__ == "__main__":
    start_time = time.time()

    # 定义文件路径和相关信息的存储位置
    # file_path_infor_DF = "/home/wcw/data/selected_line_information/selected_line_information.csv"
    # file_path_infor_DF = "/home/lrr/jupyter/LRR_data/selected_line_information/selected_line_information.csv"
    # root_file_path = "/home/wcw/data/selected_data"
    root_file_path = "/home/lrr/LRR_data/code_data/unMappingData"
    linename_list = os.listdir(root_file_path)

    # 多进程计算每条线路的路网长度信息
    infor_list = multilprocess_compute_distance_network(linename_list, root_file_path)
    infor_DF = pd.DataFrame(infor_list, columns=["bus_line_name（线路名称）", "bus_car_num（辆）", "bus_gps_num（个）",
                                                 "time_generate_distance_index（秒）", "time_compute_gps_distance（秒）",
                                                 "distance_direction1（公里）", "distance_direction2（公里）"])

    print(infor_list)
    # # 保存计算结果到CSV文件
    # if os.path.exists(file_path_infor_DF):
    #     os.remove(file_path_infor_DF)
    # infor_DF.to_csv(file_path_infor_DF, index=False)

    # 计算总耗时并打印
    end_time = time.time()
    print("总计算耗时：", end_time - start_time, "秒。")
