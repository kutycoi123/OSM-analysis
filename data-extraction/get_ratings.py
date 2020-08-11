import sys
import json
import urllib.request
import urllib.parse
import pandas as pd
radius = 500
api_key = 'AIzaSyAoFo2n3NfRB7n3a8ltVufvwPAj65J4zZU'
def get_ratings(x):
	lat, lon, name = x['lat'], x['lon'], x['name']
	if name:
		original_name = name
		name = urllib.parse.quote(name)
		url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={},{}&radius={}&name={}&key={}"\
				.format(lat, lon, radius, name, api_key)
		data = json.load(urllib.request.urlopen(url))
		if len(data['results']) > 0:
			print("{}: {}".format(original_name, data['results'][0]['rating']))
			return data['results'][0]['rating']
	return None

def main(inputs, output):
	df = pd.read_json(inputs, orient='records', compression='gzip', lines=True)
	df['rating'] = df.apply(get_ratings, axis=1)
	print(df.head(10))
	df.to_json(output, orient='records', compression='gzip', lines=True)
	
if __name__ == '__main__':
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs, output)