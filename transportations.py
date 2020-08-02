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
empty = []

# transportations =['bicycle_parking']
def main(inputs, output):
    poi = spark.read.json(inputs, schema=amenity_schema)
    poi = poi.filter((poi['lon'] > -123.5) & (poi['lon'] < -122))
    poi = poi.filter((poi['lat'] > 49) & (poi['lat'] < 49.5))
    #poi = poi.coalesce(1) # ~1MB after the filtering 
    transportations_data = poi.filter(poi.amenity.isin(transportations))
    # print(transportations_data.count())
    
    # transportations_data.show(100)
    # poi = poi.filter((poi['amenity'] == 'cafe') & (poi['name'] != 'null'))
    # poi = poi.filter((poi['amenity'] == 'bank'))
    # poi.show()
    # bankCount = poi.groupBy(poi['name']).agg(functions.count(poi['name']))
    plt.scatter(poi.select('lat').collect(), poi.select('lon').collect(), s = 20, marker='.')
    plt.gca().invert_yaxis()
    plt.gca().invert_xaxis()
    # plt.show()
    plt.savefig('Greater_Vancouver_thru_transportation_map')
    # bankCount.show()    
    # print(poi.count())
    # poi.sort(functions.asc('timestamp')).show(100)
    # poi.write.json(output, mode='overwrite', compression='gzip')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
