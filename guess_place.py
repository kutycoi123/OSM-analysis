import sys
import pandas as pd
import numpy as np
from math import pi
def dist(lat1, lon1, lat2, lon2):
    # Reference: https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/21623206
    p = pi/180
    a = 0.5 - np.cos((lat2-lat1)*p)/2 + np.cos(lat1*p) * np.cos(lat2*p) * (1-np.cos((lon2-lon1)*p))/2
    return 12742 * np.arcsin(np.sqrt(a))

def main(inputs, locations, output=None):
	df = pd.read_json(inputs, orient='records', compression='gzip', lines=True)
	loc = pd.read_csv(locations)
	lat = loc['latitude'].to_numpy()
	lon = loc['longitude'].to_numpy()
	places = None
	for i in range(len(lat)):
		lat_value = lat[i]
		lon_value = lon[i]
		df['dist'] = df.apply(lambda x: dist(x.lat, x.lon, lat_value, lon_value), axis=1)
		if places is not None:
			places = pd.concat([places, df[df['dist'] < 0.05]])
		else:
			places = df[df['dist'] < 0.05]
	places = places.drop_duplicates(subset=['lat','lon'], keep='first')
	print(places.head(10))
	places.to_json(output, orient='records', lines=True)

if __name__ == '__main__':
	inputs = sys.argv[1]
	locations = sys.argv[2]
	output = sys.argv[3]
	main(inputs, locations, output)