import sys
import pandas as pd

def main(inputs, output):
	df = pd.read_json(inputs, lines=True)
	df = df[df['amenity'] == 'fast_food']
	#print(df['amenity'])
	df.to_json(output, orient='records', compression='gzip', lines=True)
	print(df.head(50))


if __name__ == '__main__':
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs, output)