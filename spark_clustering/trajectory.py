import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext

spark = SparkSession.builder.appName("plot trajectory").getOrCreate()
df = spark.read.csv('data/part-00000-18150c91-b97a-4fb6-ad34-4e01ea85f281-c000.csv', header=True)

df = df.limit(10)

trajectory_list = df.select(collect_list('trajectory')).first()[0]
uuid_list = df.select(collect_list('uuid')).first()[0]

for i in range(0, len(trajectory_list)):
    # print(type(uuid_list[i]))
    lan_array = []
    lng_array = []
    pins = trajectory_list[i].split(';')
    for pin in pins:
        pin_list = pin.split(',')
        lan_array.append(float(pin_list[0]))
        lng_array.append(float(pin_list[1]))
    plt.plot(lan_array, lng_array,label=uuid_list[i])

print('drawing')
plt.legend()
plt.show()
         
              