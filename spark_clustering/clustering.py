import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext

spark = SparkSession.builder.appName("plot trajectory").getOrCreate()
df = spark.read.csv('data/part-00000-5311efdb-cda9-4ac2-8249-2fbf5536d3aa-c000.csv', header=True)

trajectory_list = df.select(collect_list('trajectory')).first()[0]
cluster_list = df.select(collect_list('cluster')).first()[0]

for i in range(0, len(trajectory_list)):
    # print(type(uuid_list[i]))
    lan_array = []
    lng_array = []
    pins = trajectory_list[i].split(';')
    for pin in pins:
        pin_list = pin.split(',')
        lan_array.append(float(pin_list[0]))
        lng_array.append(float(pin_list[1]))
    color_label = 'b'
    if int(cluster_list[i]) == 0:
        color_label = 'g'
    elif int(cluster_list[i]) == 1:
        color_label = 'r'
    elif int(cluster_list[i]) == 2:
        color_label = 'c'
    elif int(cluster_list[i]) == 3:
        color_label = 'm'
    elif int(cluster_list[i]) == 4:
        color_label = 'y'
    elif int(cluster_list[i]) == 5:
        color_label = 'k'
    elif int(cluster_list[i]) == 6:
        color_label = 'tab:pink'
    plt.plot(lan_array, lng_array,color=color_label)

print('drawing')

plt.show()
         
              