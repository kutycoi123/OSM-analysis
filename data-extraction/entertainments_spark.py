import sys
import numpy as np
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lower
from pyspark.ml import Pipeline

# add more functions as necessary
schema = types.StructType([
	types.StructField('lat', types.DoubleType(), nullable=False),
	types.StructField('lon', types.DoubleType(), nullable=False),
	types.StructField('timestamp', types.TimestampType(), nullable=False),
	types.StructField('amenity', types.StringType(), nullable=False),
	types.StructField('name', types.StringType(), nullable=True),
	types.StructField('tags', types.MapType(types.StringType(), types.StringType()), nullable=False),
])
entertainments = ['arts_centre', 'bistro', 'nightclub', 'bbq', 'car_rental',
                      'leisure', 'park', 'restaurant', 'bar', 'casino', 'gambling',
                      'cafe', 'theatre', 'stripclub', 'pub']
def main():
    # main logic starts here
	data = spark.read.json("../amenities-vancouver.json.gzip", schema=schema)
	entertainments_data = data.filter(data.amenity.isin(entertainments))
	#entertainments_data.show()
	entertainments_data.write.json("../entertainments-vancouver", compression='gzip', mode="overwrite")

if __name__ == '__main__':
	spark = SparkSession.builder.appName('example code').getOrCreate()
	assert spark.version >= '2.4' # make sure we have Spark 2.4+
	spark.sparkContext.setLogLevel('WARN')
    #sc = spark.sparkContext

	main()