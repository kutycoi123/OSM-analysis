import sys
import pandas as pd

entertainments = ['arts_centre', 'bistro', 'nightclub', 'bbq', 'car_rental',
                      'leisure', 'park', 'restaurant', 'bar', 'casino', 'gambling',
                      'cafe', 'theatre', 'stripclub', 'pub']

inputs = "amenities-vancouver.json.gz"
output = "entertainments-vancouver.json.gzip"
def main():
	df = pd.read_json(inputs, lines=True)
	df = df[df['amenity'].isin(entertainments)]
	#print(df['amenity'])
	df.to_json(output, orient='records', compression='gzip', lines=True)


if __name__ == '__main__':
	main()