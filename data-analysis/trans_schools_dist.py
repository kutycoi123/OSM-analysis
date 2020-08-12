import sys
import matplotlib.pyplot as plt
import pandas as pd

def main(input1, input2):
	transportations_data = pd.read_json(input1, orient='records', lines= True, compression='gzip')
	schools_data = pd.read_json(input2, orient='records', lines = True, compression='gzip')
	
	plt.figure(figsize=(10, 6))
	plt.subplot(1, 2, 1)
	plt.xticks(rotation=25)
	plt.scatter(transportations_data['lon'], transportations_data['lat'], s = 20, marker='.')
	plt.xlim(-123.5, -122)
	plt.ylim(49,49.5)
	plt.title('Transportations Distribution')
	plt.xlabel('Longitude')
	plt.ylabel('Latitude')
	
	plt.subplot(1, 2, 2)
	plt.xticks(rotation=25)
	plt.scatter(schools_data['lon'], schools_data['lat'], s = 20, marker='o', color='b')
	plt.title('Schools Distribution')
	plt.xlabel('Longitude')
	plt.ylabel('Latitude')
	plt.savefig('../images/Transportation_School_Distribution')

if __name__ == '__main__':
    transportations = '../transportations-Vancouver.json.gzip'
    schools = '../schools-Vancouver.json.gzip'
    main(transportations, schools)