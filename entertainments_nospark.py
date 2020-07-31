import sys
import pandas as pd

entertainments = ['arts_centre', 'bistro', 'nightclub', 'bbq', 'car_rental',
                      'leisure', 'park', 'restaurant', 'bar', 'casino', 'gambling',
                      'cafe', 'theatre', 'stripclub', 'pub']

def main(inputs, output):
	df = pd.read_json(inputs, lines=True)
	df = df[df['amenity'].isin(entertainments)]
	#print(df['amenity'])
	df.to_json(output, orient='records', compression='gzip', lines=True)


if __name__ == '__main__':
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs, output)