from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt

sc = SparkContext()
#4 data points (0.0, 0.0), (1.0, 1.0), (9.0, 8.0) (8.0, 9.0)
dataset = [19.09988,72.87479,19.09988,72.87479,19.10932,72.85358,19.12868,72.8552,19.14572,72.85237,
        19.09988,72.87479,19.09988,72.87479,19.10465,72.88658,19.11222,72.88842,19.11458,72.89984,
        19.21628,72.87064,19.18983,72.85841,19.16373,72.85825,19.1341,72.85519,19.09973,72.87561,
        19.09988,72.87479,19.09001,72.84375,19.06089,72.84928,19.06033,72.85749,19.06668,72.86757]
data = array(dataset).reshape(4,10)
# for item in data:
#     print(type(item))
# #Generate K means
model = KMeans.train(sc.parallelize(data), 2, maxIterations = 1000, initializationMode="random")
#Print out the cluster of each data point
print (model.predict(array([19.09988,72.87479,19.09988,72.87479,19.10932,72.85358,19.12868,72.8552,19.14572,72.85237])))
print (model.predict(array([19.09988,72.87479,19.09988,72.87479,19.10465,72.88658,19.11222,72.88842,19.11458,72.89984])))
print (model.predict(array([19.21628,72.87064,19.18983,72.85841,19.16373,72.85825,19.1341,72.85519,19.09973,72.87561])))
print (model.predict(array([19.09988,72.87479,19.09001,72.84375,19.06089,72.84928,19.06033,72.85749,19.06668,72.86757])))
