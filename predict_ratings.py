import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits import mplot3d
from sklearn.neural_network import MLPRegressor, MLPClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score

def is_good_restaurant(x):
	rating = x['rating']
	if rating >= 3.0:
		return True
	return False
amenities = ['restaurant', 'fast_food']
def main():
	entertainments = pd.read_json("entertainments-with-ratings.json.gzip", orient="records", compression="gzip", lines=True)
	fastfoods = pd.read_json("fastfood-with-ratings.json.gzip", orient='records', compression='gzip', lines=True)
	fastfoods = fastfoods[fastfoods['rating'].notna()]	
	restaurants = entertainments[entertainments['amenity'].isin(amenities)]
	restaurants = restaurants[restaurants['rating'].notna()]
	restaurants = pd.concat([restaurants, fastfoods])
	#restaurants = fastfoods
	restaurants['is_good'] = restaurants.apply(is_good_restaurant, axis=1)
	bad = restaurants[restaurants['is_good'] == False]
	good = restaurants[restaurants['is_good'] == True]
	restaurants['lat'] = restaurants.apply(lambda x: round(x['lat'],3), axis=1)
	restaurants['lon'] = restaurants.apply(lambda x: round(x['lon'],3), axis=1)
	print(bad)
	print(restaurants)
	location = np.array(restaurants[['lat', 'lon']])
	is_good = np.array(restaurants['is_good'])
	good_res = np.array(good)
	bad_res = np.array(bad)
	X_train, X_test, y_train, y_test = train_test_split(location, is_good)
	X = location
	y = is_good
	model = MLPClassifier(hidden_layer_sizes=(100,50))
	model.fit(X_train, y_train)
	predict = model.predict(X)
	print(model.score(X_train, y_train))
	print(model.score(X_test, y_test))
	print(predict)
	#fig, (ax1, ax2) = plt.subplots(1,2,figsize=(12,10), constrained_layout=True)
	#ax1.scatter(X[:, 0], X[:, 1], c=y, cmap='Set1')
	#ax1.set_title("Original ratings")
	#ax2.scatter(X[:, 0], X[:, 1], c=predict, cmap='Set1')
	#ax2.set_title("Predicted ratings")
	#plt.savefig("restaurant_fastfood_ratings.png")
	plt.scatter(bad_res[:, 0], bad_res[:, 1])
	plt.show()


if __name__ == "__main__":
	main();