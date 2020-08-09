import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Row
import matplotlib.pyplot as plt
import re
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LinearSVC
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors


spark = SparkSession.builder.appName('bike_parking').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
#sc = spark.sparkContext


amenity_schema = types.StructType([
    types.StructField('lat', types.DoubleType(), nullable=False),
    types.StructField('lon', types.DoubleType(), nullable=False),
    types.StructField('timestamp', types.TimestampType(), nullable=False),
    types.StructField('amenity', types.StringType(), nullable=False),
    types.StructField('name', types.StringType(), nullable=True),
    types.StructField('tags', types.MapType(types.StringType(), types.StringType()), nullable=False),
])

def year_month(s):
	return re.search(r'(\d+\-\d+).*', s).group(1)

def main(inputs, output):
    poi = spark.read.json(inputs, schema=amenity_schema)
    poi = poi.filter((poi['lon'] > -123.5) & (poi['lon'] < -122))
    poi = poi.filter((poi['lat'] > 49) & (poi['lat'] < 49.5))
    # poi.show(100)
    poi = poi.filter(poi['amenity'] == 'bicycle_parking')
    split_date = functions.split(poi['timestamp'], ' ')
    poi = poi.withColumn('date', split_date.getItem(0))

    remove_day = functions.udf(year_month, returnType=types.StringType())

    bike_parking = poi.select(
    	poi['lon'],
    	poi['lat'],
    	remove_day(poi['date']).alias('date'),
    	poi['amenity'],
    	poi['name']
    )
    print(bike_parking.count())
    grouped_month = bike_parking.groupby('date').agg(functions.count(bike_parking['amenity']).alias('count'))
    grouped_month = grouped_month.filter(grouped_month['count'] > 10)
    grouped_month = grouped_month.sort('date')
    df = grouped_month.toPandas()
    plt.figure(figsize=(10, 5))
    plt.xticks(rotation=45)
    plt.locator_params(axis='x', nbins=10)
    plt.plot(df['date'], df['count'], 'b')
    plt.savefig('bike_parking_by_month')
    # plt.show()


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)