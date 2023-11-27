# deprecated code workaround
import collections
from collections import abc
collections.MutableMapping = abc.MutableMapping
collections.Mapping = abc.Mapping
collections.Sequence = abc.Sequence
collections.Callable = abc.Callable


# imports
from coinbase.wallet.client import Client
import time
import re
import pandas as pd


# api
handshake = open('api.dat', 'r').read().splitlines()
client = Client(handshake[0], handshake[1])


# output
wire = open("wire.dat", "w")
pd.set_option('display.max_rows', 500)    
pd.set_option('display.max_columns', 50)


# get all coinbase cryptos
def getCB():

	# snapshot of latest coinbase data
	coins = client._get("v2", "assets", "prices", 
		params = {
		"base": "USD",
		"filter": "holdable",
		"resolution": "latest"})
	
	# parse text and prepare for pandas
	# each line is one coin
	lines = coins.content.decode("utf8")
	lines = lines.replace("\"", "")
	lines = lines.replace("data:[", "")
	lines = lines.replace("}]}", "")
	lines = lines.replace("{{base:", "")
	lines = re.split("},{base:", lines)
	
	# parse further, dictionary, list and then pandas
	lineList = []
	for i in lines:
		i = i.replace("}}}", "")
		i = i.replace("currency:USD,prices:{", "")
		i = i.replace("latest_price:{amount:{", "")
		i = i.replace("},timestamp", ",timestamp")
		i = i.replace("percent_change:{", "")
		i = "name:" + i
		
		res = []
		for sub in i.split(','):
			if ':' in sub:
				res.append(map(str.strip, sub.split(':', 1)))
		res = dict(res)	
		lineList.append(res)
	
	df = pd.DataFrame(lineList)
	
	# output name, hourly % change and base_id to file
	wire.write(str(df[['name', 'hour', 'base_id']]))
	wire.write("\n\n")
	
	return df


##############
###  MAIN  ###
##############

start_time = time.time()

cryptos = getCB()
print(str(cryptos[['name', 'hour', 'base_id']]))

end_time = time.time()
print("--- %s seconds ---" % (end_time - start_time))
print("--- %s minutes ---" % ((end_time - start_time) / 60))

# close file output
wire.close()
