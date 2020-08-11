import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Row
import matplotlib.pyplot as plt
import re
import numpy as np
import pandas as pd
import seaborn as sns
from datetime import date
from scipy import stats
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
	return re.search(r'(\d+\-\d+\-\d+).*', s).group(1)

def to_timestamp(x):
	return x.timestamp()

def day_num(x):
    return x.weekday()

def main(inputs):
    poi = spark.read.json(inputs, schema=amenity_schema)
    poi = poi.filter((poi['lon'] > -123.5) & (poi['lon'] < -122))
    poi = poi.filter((poi['lat'] > 49) & (poi['lat'] < 49.5))
    # poi.show(100)
    poi = poi.filter((poi['amenity'] == 'bicycle_parking') & (functions.size('tags') > 0))
    split_date = functions.split(poi['timestamp'], ' ')
    poi = poi.withColumn('date', split_date.getItem(0))

    remove_hour = functions.udf(year_month, returnType=types.StringType())

    bike_parking = poi.select(
    	poi['lon'],
    	poi['lat'],
    	remove_hour(poi['date']).alias('date'),
    	poi['amenity']
    )

    grouped = bike_parking.groupby('date').agg(functions.count(bike_parking['amenity']).alias('count'))
    # grouped_month = grouped_month.filter(grouped_month['count'] > 10)
    grouped = grouped.sort('date')

    df = grouped.toPandas() 
    df['date'] = pd.to_datetime(df['date'])
    df['day_num'] = df['date'].apply(day_num)
    weekdays = df[df['day_num'] < 5]
    weekends = df[df['day_num'] >= 5]
    # print(weekdays)
    sqrt_weekdays = np.sqrt(weekdays['count'])
    sqrt_weekends = np.sqrt(weekends['count'])
    print(stats.normaltest(weekends['count']).pvalue)
    print(stats.ttest_ind(weekdays['count'],weekends['count']).pvalue)
    plt.hist([sqrt_weekdays,sqrt_weekends])
    plt.title('Weekdays vs Weekends Count')
    plt.xlabel('Count')
    plt.ylabel('Intensity')
    plt.legend(['Weekdays', 'Weekends'])
    plt.savefig('Histogram_Bike_Parking_Count')
    plt.show()
    # df['timestamp'] = df['date'].apply(to_timestamp)
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)