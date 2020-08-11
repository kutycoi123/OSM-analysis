# Limit OSM data to just greater Vancouver
# Typical invocation:
# spark-submit just-vancouver.py amenities amenities-vancouver
# hdfs dfs -cat amenities-vancouver/* | gzip -d - | gzip -c > amenities-vancouver.json.gz

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Row
import matplotlib.pyplot as plt
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LinearSVC
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors


spark = SparkSession.builder.appName('commuter').getOrCreate()
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


def main(inputs):
    poi = spark.read.json(inputs, schema=amenity_schema)
    poi = poi.filter((poi['lon'] > -123.5) & (poi['lon'] < -122))
    poi = poi.filter((poi['lat'] > 49) & (poi['lat'] < 49.5))

    stage1 = VectorAssembler(inputCols =['lon', 'lat'], outputCol='features')
    stage2 = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
    stage3 = KMeans().setK(7).setFeaturesCol("scaledFeatures").setPredictionCol('prediction')
    pipeline = Pipeline(stages=[stage1, stage2, stage3])
    model = pipeline.fit(poi)
    predictions = model.transform(poi)
    plt.scatter(predictions.select('lon').collect(),
                predictions.select('lat').collect(),
                c=predictions.select('prediction').collect(),
                cmap='Set1', edgecolor='k', s=20)
    plt.title('Potential Bus Zone')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.savefig('Potential_Bus_Zone')
    #poi = poi.coalesce(1) # ~1MB after the filtering 
    # poi.write.json(output, mode='overwrite', compression='gzip')


if __name__ == '__main__':
    inputs = sys.argv[1]
    # output = sys.argv[2]
    main(inputs)