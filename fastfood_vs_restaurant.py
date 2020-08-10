import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
import sys


def main():
	entertainments = pd.read_json("entertainments-with-ratings.json.gzip", orient='records', compression='gzip', lines=True)
	fastfoods = pd.read_json("fastfood-with-ratings.json.gzip", orient='records', compression='gzip', lines=True)
	restaurants = entertainments[entertainments['amenity'] == 'restaurant']
	restaurants = restaurants[restaurants['rating'].notna()]
	fastfoods = fastfoods[fastfoods['rating'].notna()]
	restaurants_rating = restaurants['rating']
	fastfoods_rating = fastfoods['rating']
	print(restaurants_rating)
	print(fastfoods_rating)
	ttest = stats.ttest_ind(restaurants_rating, fastfoods_rating)
	print("ttest: ", ttest)
	print("Restaurant ratings:", np.mean(restaurants_rating))
	print("Fastfood ratings: ", np.mean(fastfoods_rating))
	x = np.array(list(zip(restaurants_rating, fastfoods_rating)))
	plt.hist(x, histtype='bar', label=['Restaurant', 'Fastfood'])
	plt.title("Restaurant and fastfood ratings")
	plt.legend(['Independent-owned restaurant', 'Fast food restaurant'])
	plt.savefig("fastfood_vs_restaurant_historgram.png")

if __name__ == '__main__':
	main()