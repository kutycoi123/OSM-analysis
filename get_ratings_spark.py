import sys
import numpy as np
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lower
from pyspark.ml import Pipeline
import json
import urllib.request
import urllib.parse
radius = 500
api_key = 'AIzaSyAoFo2n3NfRB7n3a8ltVufvwPAj65J4zZU'
schema = types.StructType([
	types.StructField('lat', types.DoubleType(), nullable=False),
	types.StructField('lon', types.DoubleType(), nullable=False),
	types.StructField('timestamp', types.TimestampType(), nullable=False),
	types.StructField('amenity', types.StringType(), nullable=False),
	types.StructField('name', types.StringType(), nullable=True),
	types.StructField('tags', types.MapType(types.StringType(), types.StringType()), nullable=False),
])

def ratings(data):
	lat, lon, name = data[0], data[1], data[2]
	if name:
		name = urllib.parse.quote(name)
		url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={},{}&radius={}&name={}&key={}"\
				.format(lat, lon, radius, name, api_key)
		data = json.load(urllib.request.urlopen(url))
		if len(data['results']) > 0:
			return data['results'][0]['rating']
	return 0
	
def main(inputs, output):
	# main logic starts here
	ratings_udf = functions.udf(ratings)
	data = spark.read.json(inputs, schema=schema)
	df = data.withColumn('locations', ratings_udf(functions.array('lat', 'lon', 'name')))
	df.show(10)

if __name__ == '__main__':
	inputs = sys.argv[1]
	output = sys.argv[2]
	spark = SparkSession.builder.appName('example code').getOrCreate()
	assert spark.version >= '2.4' # make sure we have Spark 2.4+
	spark.sparkContext.setLogLevel('WARN')
    #sc = spark.sparkContext

	main(inputs, output)