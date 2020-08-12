# Limit OSM data to just greater Vancouver
# Typical invocation:
# spark-submit just-vancouver.py amenities amenities-vancouver
# hdfs dfs -cat amenities-vancouver/* | gzip -d - | gzip -c > amenities-vancouver.json.gz

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.cluster import KMeans, AgglomerativeClustering, AffinityPropagation
from pyspark.sql import SparkSession, functions, types, Row
spark = SparkSession.builder.appName('OSM point of interest extracter').getOrCreate()
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

transportations = ['bicycle_parking', 'bus_station', 'parking', 'fuel',
                    'car_sharing', 'ferry_terminal', 'parking_entrance', 'seaplane terminal']
schools = ['school', 'university', 'college']

def main(inputs):
    poi = spark.read.json(inputs, schema=amenity_schema)
    poi = poi.filter((poi['lon'] > -123.5) & (poi['lon'] < -122))
    poi = poi.filter((poi['lat'] > 49) & (poi['lat'] < 49.5))
    #poi = poi.coalesce(1) # ~1MB after the filtering 

    transportations_data = poi.filter(poi.amenity.isin(transportations))
    schools_data = poi.filter(poi.amenity.isin(schools))
    bike_parking_data = transportations_data.filter((transportations_data['amenity'] == 'bicycle_parking') & (functions.size('tags') > 0))
    fuel_data = transportations_data.filter(transportations_data['amenity'] == 'fuel')

    transportations_data.write.json('../transportations-Vancouver', mode='overwrite', compression='gzip')
    schools_data.write.json('../schools-Vancouver', mode='overwrite', compression='gzip')
    bike_parking_data.write.json('../bikes-Vancouver', mode='overwrite', compression='gzip')
    fuel_data.write.json('../fuel-Vancouver', mode='overwrite', compression='gzip')

if __name__ == '__main__':
    inputs = '../amenities-vancouver.json.gz'
    main(inputs)