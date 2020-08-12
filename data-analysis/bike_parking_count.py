import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Row
import matplotlib.pyplot as plt
import re
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
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

def to_timestamp(x):
	return x.timestamp()

def main(inputs):
    poi = spark.read.json(inputs, schema=amenity_schema)
    poi = poi.filter((poi['lon'] > -123.5) & (poi['lon'] < -122))
    poi = poi.filter((poi['lat'] > 49) & (poi['lat'] < 49.5))
    # poi.show(100)

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
    # print(bike_parking.count())
    grouped_month = bike_parking.groupby('date').agg(functions.count(bike_parking['amenity']).alias('count'))
    grouped_month = grouped_month.filter(grouped_month['count'] > 10)
    grouped_month = grouped_month.sort('date')
    df = grouped_month.toPandas() 
    # df['date'] = pd.to_datetime(df['date'])
    # df= df[df['date'] > '2017-01']
    # df['timestamp'] = df['date'].apply(to_timestamp)

    sns.set(color_codes=True)
    plt.figure(figsize=(10, 5))
    plt.xticks(rotation=25)
    plt.locator_params(axis='x', nbins=10)
    plt.plot(df['date'], df['count'], 'b')
    plt.title('Bike Parking Checkin')
    plt.xlabel('Date by Month/Year')
    plt.ylabel('Numbers of Checkin')
    plt.savefig('../images/Bike_Parking_Checkin')
    # plt.show()

if __name__ == '__main__':
    inputs = '../bikes-Vancouver.json.gz'
    main(inputs)