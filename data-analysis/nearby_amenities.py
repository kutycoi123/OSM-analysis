import sys
import numpy as np
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lower
from pyspark.ml import Pipeline
import matplotlib.pyplot as plt
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import Vectors
# add more functions as necessary
schema = types.StructType([
	types.StructField('lat', types.DoubleType(), nullable=False),
	types.StructField('lon', types.DoubleType(), nullable=False),
	types.StructField('timestamp', types.TimestampType(), nullable=False),
	types.StructField('amenity', types.StringType(), nullable=False),
	types.StructField('name', types.StringType(), nullable=True),
	types.StructField('tags', types.MapType(types.StringType(), types.StringType()), nullable=False),
])

def main():
    # main logic starts here
	data = spark.read.json("../entertainments-vancouver", schema=schema)
	assembler = VectorAssembler(
		inputCols = ['lat', 'lon'],
		outputCol='features')
	kmeans_estimator = KMeans().setK(20)\
		.setFeaturesCol("features").setPredictionCol('prediction')
	pipeline = Pipeline(stages=[assembler, kmeans_estimator])
	model = pipeline.fit(data)
	predictions = model.transform(data)

	centers = np.array(model.stages[-1].clusterCenters())
	for center in centers:
		print(center)

    #plt.show();return
    #plt.figure(figsize=(10,10))
	plt.scatter(predictions.select('lat').collect(),
				predictions.select('lon').collect(),
				c=predictions.select('prediction').collect(),
				cmap='Set1', edgecolor='k', s=20)    
	plt.title("Amenities scatter")
	plt.scatter(centers[:,0], centers[:,1], s=50)
	#plt.show()
	plt.savefig("../images/nearby_amenities.png")	


if __name__ == '__main__':
	spark = SparkSession.builder.appName('example code').getOrCreate()
	assert spark.version >= '2.4' # make sure we have Spark 2.4+
	spark.sparkContext.setLogLevel('WARN')
    #sc = spark.sparkContext

	main()