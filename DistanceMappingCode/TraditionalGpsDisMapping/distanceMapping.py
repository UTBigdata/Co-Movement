# 根据之前生成的路段距离文件，对gps数据的距离进行计算
import os
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point
from geopy import distance
import time

start_time = time.time()

line_name = "M2503"
# 读取Gps数据
gps_file_path = f"/home/lrr/LRR_data/code_data/unMappingData/{line_name}/cars"

# 定义存储所有归一化之后GPS点的列表
all_gps_normalized_distance_length = []

gps_files = os.listdir(gps_file_path)
for car_name in gps_files:
    # print(car_name)
    car_gps_data = gpd.read_file(gps_file_path + "/" + car_name + "/" + car_name + ".shp")
    # print(car_gps_data.iloc[0]["direction"])
    # print(car_gps_data)

    # 判断每个GPS点的direction
    # for index,gpsline in car_gps_data.iterrows():
    #     #print(gpsline)
    #     if gpsline["direction"] == 1:
    #         #print("direction:1")
    #         #路网所有的路段以及其出现顺序的文件路径
    #         opath_seq_file = f"/home/CM/LC/lc/M3353/networks/direction1/M3353.csv"
    #         #具体的路网文件路径
    #         opath_distance_file = f"/home/CM/LC/lc/M3353/networks/direction1/network_distance.csv"
    #     else:
    #         opath_seq_file = f"/home/CM/LC/lc/M3353/networks/direction2/M3353.csv"
    #         opath_distance_file = f"/home/CM/LC/lc/M3353/networks/direction2/network_distance.csv"
    # print(car_gps_data.iloc[0]["direction"])
    # 读取对应directrion的路网的路段序列文件：
    # opath_seq = pd.read_csv(opath_seq_file,sep=",")
    # 读取direction为1或者2的路段距离文件
    # opath_distance = pd.read_csv(opath_distance_file,sep=",")
    # print("direction",opath_distance.iloc[0]["net_direction"])

    opath_seq_1 = pd.read_csv(f"/home/lrr/LRR_data/code_data/unMappingData/{line_name}/networks/direction1/{line_name}.csv", sep=",")
    opath_distance_1 = pd.read_csv(f"/home/lrr/LRR_data/code_data/unMappingData/{line_name}/networks/direction1/network_distance.csv", sep=",")
    opath_seq_2 = pd.read_csv(f"/home/lrr/LRR_data/code_data/unMappingData/{line_name}/networks/direction2/{line_name}.csv", sep=",")
    opath_distance_2 = pd.read_csv(f"/home/lrr/LRR_data/code_data/unMappingData/{line_name}/networks/direction2/network_distance.csv", sep=",")

    for index, gps_point in car_gps_data.iterrows():
        # print(gps_point)
        if gps_point["direction"] == 1:
            # print("direction:1")
            # 路网所有的路段以及其出现顺序的文件路径
            opath_seq = opath_seq_1
            # 具体的路网文件路径
            opath_distance = opath_distance_1
        else:
            opath_seq = opath_seq_2
            opath_distance = opath_distance_2

        # 此处需要判断gps所属路段编号是否存在于对应路网的路段编号之中。
        # 根据gps的路段编号，获得对应的路网距离
        if gps_point["opath"] in opath_seq["opath"].values:
            gps_datum_point = []
            for index, opath_information in opath_distance.iterrows():
                if opath_information["opath"] == gps_point["opath"]:
                    # print(opath_information["opath"])
                    # print(gps_point["opath"])
                    gps_datum_point.append(opath_information)
            # print(gps_point)
            # print(gps_datum_point)
            # print("*******************************")

            # 实现判断gps点位于哪两个基准点之间，计算gps点到原点的距离。
            # 定义存储不同opath的LingString的基准点的集合
            opath_datum_points = []
            for datum_point in gps_datum_point:
                # print(datum_point["lng"])
                point = (datum_point["lng"], datum_point["lat"])
                opath_datum_points.append(point)
                # print(point)
                # print("******************")
                # 根据不断输入的point创建LINESTRING的geometry图形
            # 将opath_datum_points集合转换为LINGSTRING对象
            opath_linestring = LineString(opath_datum_points)
            # print(opath_linestring)

            # 获取LineString对象的坐标列表
            linestring_cooders = list(opath_linestring.coords)
            # 定义需要检查的gps点的point对象
            point_gps = Point(gps_point["lng"], gps_point["lat"])
            xp = gps_point["lng"]
            yp = gps_point["lat"]
            # 定义point_gps和基准点的最小距离和坐标对
            min_distance_sum = float('inf')
            closest_pair = None
            # 遍历LINESTRING的所有线段
            for i in range(len(linestring_cooders) - 1):
                start = Point(linestring_cooders[i])
                end = Point(linestring_cooders[i + 1])
                # 计算gps点到两个线段端点的距离之和
                distance_sum = point_gps.distance(start) + point_gps.distance(end)
                # 如果这个距离之和小于当前最小距离，则更新最小距离和对应的坐标对
                if distance_sum < min_distance_sum:
                    min_distance_sum = distance_sum
                    closest_pair = (start.coords[0], end.coords[0])

            # 输出结果  
            # if closest_pair:  
            #     print(f"GPS点({xp}, {yp})最接近的坐标对是({closest_pair[0][0]}, {closest_pair[0][1]})和({closest_pair[1][0]}, {closest_pair[1][1]})。")  
            # else:  
            #     print(f"没有找到合适的坐标对。")
            # print("*******************************************")
            # 获取最近基准点对的前一个点距离原点的距离加上这个点离gps的距离即可计算出gps点离原点的距离
            # print(opath_distance)
            # print("******************************")
            # pre_datum_point_distance = opath_distance[(opath_distance["lng"]=closest_pair[0][0]&opath_distance["lat"] = closest_pair[0][1])]["distance_opath"]
            pre_datum_point_distance = opath_distance.loc[(opath_distance["lng"] == closest_pair[0][0]) & (
                        opath_distance["lat"] == closest_pair[0][1]) & (opath_distance["opath"] == gps_point["opath"]),
                                       :]
            # print(pre_datum_point_distance.iloc[0]["distance_opath"])
            # print("*****************************************")
            # 车辆的gps点为
            latlng_car = (gps_point["lat"], gps_point["lng"])
            # 基准点的GPS为
            latlng_datumn = (closest_pair[0][1], closest_pair[0][0])
            # 计算目标gps和原点之间的距离
            distance_of_gpspoint = pre_datum_point_distance["distance_opath"] + distance.distance(latlng_datumn,
                                                                                                  latlng_car).km
            # print("当前gps点距离原点的距离为:",distance_of_gpspoint.iloc[0])
            # print(type(distance_of_gpspoint))
            # print("******************************")
            # 接下来就是使用线路的最长距离对gps点的距离进行归一化处理。
            # 获取线路的最大长度
            longest_path_distance = opath_distance.iloc[-1]["distance_opath"]
            # print(opath_distance.iloc[-1]["distance_opath"])
            # 归一化之后gps点的值为
            gps_normalized_distance_length = distance_of_gpspoint.iloc[0] / longest_path_distance
            # print(gps_point)
            # print("gps点距离归一化之后的值为:",gps_normalized_distance_length)
            all_gps_normalized_distance_length.append((gps_point["idx"], gps_point["opath"], gps_point["linenumber"],
                                                       gps_point["direction"], gps_point["time"],
                                                       gps_point["lng"], gps_point["lat"], distance_of_gpspoint.iloc[0],
                                                       gps_normalized_distance_length))

df_all_gps_normalized_distance_length = pd.DataFrame(all_gps_normalized_distance_length,
                                                     columns=["idx", "opath", "lineName", "direction", "time",
                                                              "lng", "lat", "distance_of_gpspoint",
                                                              "gps_normalized_distance_length"])
df_all_gps_normalized_distance_length.to_csv(f"/home/lrr/LRR_data/code_data/unMappingData/{line_name}/result/{line_name}_gpsdistance_normalized.csv",
                                             index=False)
end_time = time.time()
total_time = end_time - start_time
print("程序运行总用时:", total_time, "s")