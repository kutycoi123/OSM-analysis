|Problem |Filename |Command |Result|
|---|---|---|---|
|Extract entertaining places|entertainments_spark.py or entertainments_nospark.py|python3 entertainments_nospark.py or spark-submit entertainments_spark.py|entertainments-vancouver.json.gzip or entertainments-vancouver folder (produced by spark)|
|Extract fast food|fastfood.py|python3 fastfood.py|fastfood.json.gzip|
|Get ratings for entertaining places|get_ratings.py|python3 get_ratings.py ../entertainments-vancouver.json.gzip ../entertainments-with-ratings.json.gzip|entertainments-with-ratings.json.gzip|
|Get ratings for fastfood restaurants|get_ratings.py|python3 get_ratings.py ../fastfood.json.gzip ../fastfood-with-ratings.json.gzip|fastfood-with-ratings.json.gzip|
|Extract 4 categories of data|extract_data_spark.py|spark-submit extract_data_spark.py|bikes-Vancouver folder **and** bikes-Vancouver.json.gz, fuel-Vancouver folder **and** fuel-Vancouver.json.gz, transportations-Vancouver folder **and** transportations-Vancouver.json.gz, school-Vancouver folder|
- Note: All the data outputs will be located in the entry main folder, not this folder