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

transportations_schools = ['bicycle_parking', 'bus_station', 'parking', 'fuel',
                    'car_sharing', 'ferry_terminal', 'parking_entrance', 'seaplane terminal']
# transportations = ['bus_station']

# transportations =['bicycle_parking']
def main(inputs, output):
    poi = spark.read.json(inputs, schema=amenity_schema)
    poi = poi.filter((poi['lon'] > -123.5) & (poi['lon'] < -122))
    poi = poi.filter((poi['lat'] > 49) & (poi['lat'] < 49.5))
    #poi = poi.coalesce(1) # ~1MB after the filtering 

    transportations_data = poi.filter(poi.amenity.isin(transportations_schools))
    # transportations_data = transportations_data.filter(functions.size('tags') > 0)
    plt.scatter(transportations_data.select('lon').collect(), transportations_data.select('lat').collect(), s = 20, marker='.')
    # plt.show()
    plt.savefig('transportations_schools_map')
    transportations_data.write.json(output, mode='overwrite', compression='gzip')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
