import sys
import pandas as pd

inputs = "amenities-vancouver.json.gz"
output = "fastfood.json.gz"

def main():
	df = pd.read_json(inputs, lines=True)
	df = df[df['amenity'] == 'fast_food']
	df.to_json(output, orient='records', compression='gzip', lines=True)


if __name__ == '__main__':
	main()