import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext

spark = SparkSession.builder.appName("plot trajectory with speed").getOrCreate()
df = spark.read.csv('data/ada_sort.csv', header=True)

color_label = 'red'

trajectory_list = df.select(collect_list('trajectory')).first()[0]
# ada_list = df.select(collect_list('ada')).first()[0]
speed_list = df.select(collect_list('ada')).first()[0]

for i in range(0, len(trajectory_list)):
    # print(type(uuid_list[i]))
    if i % 3 == 0:
        lan_array = []
        lng_array = []
        pins = trajectory_list[i].split(';')
        for pin in pins:
            pin_list = pin.split(',')
            lan_array.append(float(pin_list[0]))
            lng_array.append(float(pin_list[1]))
    # color_label = 'b'
    # if float(speed_list[i]) > 3 and float(speed_list[i]) < 5:
    #     color_label = 'g'
    # elif float(speed_list[i]) > 5 and float(speed_list[i]) < 6:
    #     color_label = 'r'
    # if float(ada_list[i]) > 14000 and float(ada_list[i]) < 15000:
    #     color_label = 'blue'
    # elif float(ada_list[i]) > 15000 and float(ada_list[i]) < 16000:
    #     color_label = 'red'
    # elif float(ada_list[i]) > 16000 and float(ada_list[i]) < 17000:
    #     color_label = 'green'
    # if float(speed_list[i]) < 5000:
    #      color_label = 'blue'
    # elif float(speed_list[i]) > 5000 and float(speed_list[i]) < 6000:
    #      color_label = 'blue'
    # elif float(speed_list[i]) > 6000 and float(speed_list[i]) < 7000:
    #     color_label = 'yellow'
    # elif float(speed_list[i]) > 7000 and float(speed_list[i]) < 8000:
    #     color_label = 'green'
    # elif float(speed_list[i]) > 8000 and float(speed_list[i]) < 9000:
    #     color_label = 'red'
    # elif float(speed_list[i]) > 9000:
    #     color_label = 'black'
    plt.plot(lan_array, lng_array,color=color_label)

print('drawing')

plt.show()
         
              