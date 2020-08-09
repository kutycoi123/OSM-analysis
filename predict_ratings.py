import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits import mplot3d
from sklearn.neural_network import MLPRegressor, MLPClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score

def is_good_restaurant(x):
	rating = x['rating']
	if rating >= 3.7:
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
	restaurants['is_good'] = restaurants.apply(is_good_restaurant, axis=1)
	bad = restaurants[restaurants['is_good'] == False]
	print(bad)
	print(restaurants)
	location = np.array(restaurants[['lat', 'lon']])
	is_good = np.array(restaurants['is_good'])
	X_train, X_test, y_train, y_test = train_test_split(location, is_good)
	model = MLPClassifier(solver='lbfgs', hidden_layer_sizes=(5,5), activation='logistic')
	model.fit(X_train, y_train)
	predict_train = model.predict(X_train)
	print(model.score(X_train, y_train))
	print(model.score(X_test, y_test))
	plt.figure(figsize=(12,10))
	plt.scatter(X_train[:, 0], X_train[:, 1], c=y_train)
	plt.savefig("predict_ratings.png")

if __name__ == "__main__":
	main();