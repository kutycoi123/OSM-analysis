import sys
import numpy as np
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession, functions, types
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt
# add more functions as necessary

inputs = "../amenities-vancouver.json.gz"

schema = types.StructType([
	types.StructField('lat', types.DoubleType(), nullable=False),
	types.StructField('lon', types.DoubleType(), nullable=False),
	types.StructField('timestamp', types.TimestampType(), nullable=False),
	types.StructField('amenity', types.StringType(), nullable=False),
	types.StructField('name', types.StringType(), nullable=True),
	types.StructField('tags', types.MapType(types.StringType(), types.StringType()), nullable=False),
])

def data_cluster(pipeline, data, title, path):
	model = pipeline.fit(data)
	predictions = model.transform(data)
	plt.xlabel("Latitude")
	plt.ylabel("Longitude")
	plt.title(title)
	plt.scatter(predictions.select('lat').collect(),
				predictions.select('lon').collect(),
				c=predictions.select('prediction').collect(),
				cmap='Set1', edgecolor='k', s=20)
	plt.savefig(path)	

def main():
    # main logic starts here
	data = spark.read.json(inputs, schema=schema)

	fastfood = data.filter(data['amenity'] == 'fast_food')
	#fast_food.show()
	subways = fastfood.filter(functions.lower(fastfood['name']) == 'subway')
	#mcdonald = fastfood.filter(functions.lower(fastfood['name']) == "mcdonald's")
	timhortons = fastfood.filter(functions.lower(fastfood['name']) == 'tim hortons')
	assembler = VectorAssembler(
		inputCols = ['lat', 'lon'],
		outputCol='features')
	kmeans_estimator = KMeans().setK(12)\
		.setFeaturesCol("features").setPredictionCol('prediction')
	pipeline = Pipeline(stages=[assembler, kmeans_estimator])

	#data_cluster(pipeline, fastfood, "Fastfood scatter", "../images/fastfood_cluster.png")
	#data_cluster(pipeline, subways, "Subway cluster", "../images/subway_scatter.png")
	#data_cluster(pipeline, timhortons, "Tim Hortons cluster", "../images/timhorton_scatter.png")


	# restaurant = data.filter(data['amenity'] == 'restaurant')
	# plt.figure(figsize=(10, 5))	
	# plt.subplot(1,2,1)
	# plt.scatter(fastfood.select('lat').collect(),
	# 			fastfood.select('lon').collect(),
	# 			s=2)
	# plt.xlabel("Latitude")
	# plt.ylabel("Longitude")
	# plt.title("Fastfood scatter")

	# plt.subplot(1,2,2)
	# plt.scatter(restaurant.select('lat').collect(),
	# 			restaurant.select('lon').collect(),
	# 			s=2)
	# plt.xlabel("Latitude")
	# plt.ylabel("Longitude")
	# plt.title("Independent-owned restaurant scatter")
	# plt.savefig("../images/fastfood_vs_restaurant_scatter.png")


if __name__ == '__main__':
	spark = SparkSession.builder.appName('example code').getOrCreate()
	assert spark.version >= '2.4' # make sure we have Spark 2.4+
	spark.sparkContext.setLogLevel('WARN')
    #sc = spark.sparkContext

	main()