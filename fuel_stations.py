import sys
import pandas as pd
import numpy as np
from math import pi
import matplotlib.pyplot as plt
from tabulate import tabulate
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Row
spark = SparkSession.builder.appName('fuel_and_amenity').getOrCreate()
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

closeBy_amenity = ['cafe', 'toilets', 'vending_machine', 'car_wash', 'drinking_water',
                    'atm', 'telephone', 'ice_cream', 'fastfood', 'bank']

# Reference: https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/21623206
def distance(fuel, amenities):
    lat1 = fuel['lat']
    lon1 = fuel['lon']
    lat2 = amenities['lat']
    lon2 = amenities['lon']
    p = np.pi/180
    a = 0.5 - np.cos((lat2-lat1)*p)/2 + np.cos(lat1*p) * np.cos(lat2*p) * (1-np.cos((lon2-lon1)*p))/2
    result = 12742 * np.arcsin(np.sqrt(np.absolute(a))) #2*R*asin...
    return result[result < .2]

def count_nearBy(fuel, amenities):
    temp = distance(fuel, amenities)
    # print(temp)
    return temp.count()

def main(inputs, output):
    poi = spark.read.json(inputs, schema=amenity_schema)
    poi = poi.filter((poi['lon'] > -123.5) & (poi['lon'] < -122))
    poi = poi.filter((poi['lat'] > 49) & (poi['lat'] < 49.5))
    #poi = poi.coalesce(1) # ~1MB after the filtering 
    fuel_stations = poi.filter(poi['amenity'] == 'fuel')
    fuel_stations = fuel_stations.filter(fuel_stations['name'] != 'null')
    fuel_stations = fuel_stations.drop('tags')
    # close_amenities.show(100)
    close_amenities = poi.filter(poi.amenity.isin(closeBy_amenity))

    plt.scatter(close_amenities.select('lon').collect(),
                close_amenities.select('lat').collect(), marker='.')
    plt.title('Accessible Amenities at Gas Station')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    # plt.savefig('Accessible_Amenities')

    plt.scatter(fuel_stations.select('lon').collect(),
                fuel_stations.select('lat').collect())
    plt.title('Gas Station')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.legend(['Close By Amenities', 'Gas Stations'])
    plt.savefig('Gas_Staion_with_CloseBy_Amenities')

    fuel_stations = fuel_stations.toPandas()
    close_amenities = close_amenities.toPandas()
    fuel_stations['score'] = fuel_stations.apply(lambda fuel: count_nearBy(fuel, close_amenities), axis = 1) 
    # fuel_stations = fuel_stations.sort_values(by=['score'], ascending = False)
    df = spark.createDataFrame(fuel_stations)
    # df = df.filter(df['score'] > 0).sort(functions.desc('score'))
    charReplace = functions.udf(lambda x: x.replace(u'-',' '))
    df = df.select(
        df['lat'],
        df['lon'],
        charReplace(df['name']).alias('name'),
        df['score']
    )
    df = df.groupby('name').agg(
        functions.mean('score').alias('mean'),
        functions.count('name').alias('count')
    )
    # df = df.filter(((df['mean'] <= 1) == False) | ((df['count'] <= 1) == False))
    df.sort(functions.desc('score')).write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)