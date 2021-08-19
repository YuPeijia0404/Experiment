from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("KMeansExample")\
        .getOrCreate()
    dataset = spark.read.format("libsvm").load("data/kmeans_data.txt")

    # Trains a k-means model.
    kmeans = KMeans().setK(2).setSeed(42)
    model = kmeans.fit(dataset)

    # Make predictions
    predictions = model.transform(dataset)

    # Evaluate clustering by computing Silhouette score
    evaluator = ClusteringEvaluator()

    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))

    # Shows the result.
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)
