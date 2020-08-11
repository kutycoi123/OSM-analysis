# Limit OSM data to just greater Vancouver
# Typical invocation:
# spark-submit just-vancouver.py amenities amenities-vancouver
# hdfs dfs -cat amenities-vancouver/* | gzip -d - | gzip -c > amenities-vancouver.json.gz

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import matplotlib.pyplot as plt
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
    plt.figure(figsize=(10, 6))
    plt.subplot(1, 2, 1)
    plt.xticks(rotation=25)
    plt.scatter(transportations_data.select('lon').collect(), transportations_data.select('lat').collect(), s = 20, marker='.')
    # plt.show()
    plt.xlim(-123.5, -122)
    plt.ylim(49, 49.5)
    plt.title('Transportations Distribution')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')

    plt.subplot(1, 2, 2)
    plt.xticks(rotation=25)
    plt.scatter(schools_data.select('lon').collect(), schools_data.select('lat').collect(), s = 20, marker='o', color='b')
    # plt.show()
    plt.xlim(-123.5, -122)
    plt.ylim(49, 49.5)
    plt.title('Schools Distribution')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.savefig('School_Distribution')

    plt.savefig('Transportation_School_Distribution')

    # transportations_data.write.json(output, mode='overwrite', compression='gzip')

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)