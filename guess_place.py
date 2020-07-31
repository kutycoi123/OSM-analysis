import sys
import pandas as pd
import numpy as np
from math import pi
def dist(lat1, lon1, lat2, lon2):
    # Reference: https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/21623206
    p = pi/180
    a = 0.5 - np.cos((lat2-lat1)*p)/2 + np.cos(lat1*p) * np.cos(lat2*p) * (1-np.cos((lon2-lon1)*p))/2
    return 12742 * np.arcsin(np.sqrt(a))

def main(inputs, output=None):
	lat,lon = 49.268737,-123.045729
	df = pd.read_json(inputs, orient='records', compression='gzip', lines=True)
	#print(df.head(50))
	df['dist'] = df.apply(lambda x: dist(x.lat, x.lon, lat, lon), axis=1)
	places = df[df['dist'] < 0.2]
	print(places.columns)
	print(places.head(2))

if __name__ == '__main__':
	inputs = sys.argv[1]
	main(inputs)