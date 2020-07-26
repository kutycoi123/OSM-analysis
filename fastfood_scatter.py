import sys
import numpy as np
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession, functions, types
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt
# add more functions as necessary
schema = types.StructType([
	types.StructField('lat', types.DoubleType(), nullable=False),
	types.StructField('lon', types.DoubleType(), nullable=False),
	types.StructField('timestamp', types.TimestampType(), nullable=False),
	types.StructField('amenity', types.StringType(), nullable=False),
	types.StructField('name', types.StringType(), nullable=True),
	types.StructField('tags', types.MapType(types.StringType(), types.StringType()), nullable=False),
])


def main(inputs, output=None):
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
	kmeans_estimator = KMeans().setK(10)\
		.setFeaturesCol("features").setPredictionCol('prediction')
	pipeline = Pipeline(stages=[assembler, kmeans_estimator])
	model_fastfood = pipeline.fit(fastfood)
	model_subways = pipeline.fit(subways)
	model_timhortons = pipeline.fit(timhortons)

	predictions_fastfood = model_fastfood.transform(fastfood)
	# predictions_subways = model_subways.transform(subways)
	# predictions_timhortons = model_timhortons.transform(timhortons)

	# plt.scatter(predictions_fastfood.select('lat').collect(),
	# 			predictions_fastfood.select('lon').collect(),
	# 			c=predictions_fastfood.select('prediction').collect(),
	# 			cmap='Set1', edgecolor='k', s=20)
	# plt.savefig("fastfood_scatter.png")

	# plt.scatter(predictions_subways.select('lat').collect(),
	# 			predictions_subways.select('lon').collect(),
	# 			c=predictions_subways.select('prediction').collect(),
	# 			cmap='Set1', edgecolor='k', s=20)
	# plt.savefig("subway_scatter.png")
	
	# plt.scatter(predictions_timhortons.select('lat').collect(),
	# 			predictions_timhortons.select('lon').collect(),
	# 			c=predictions_timhortons.select('prediction').collect(),
	# 			cmap='Set1', edgecolor='k', s=20)
	# plt.savefig("timhorton_scatter.png")
	#timhortons.show(100)
	#mcdonald.show(100)


if __name__ == '__main__':
	inputs = sys.argv[1]
	#output = sys.argv[2]
	spark = SparkSession.builder.appName('example code').getOrCreate()
	assert spark.version >= '2.4' # make sure we have Spark 2.4+
	spark.sparkContext.setLogLevel('WARN')
    #sc = spark.sparkContext

	main(inputs)