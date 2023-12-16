# deprecated code workaround
import collections
from collections import abc
collections.MutableMapping = abc.MutableMapping
collections.Mapping = abc.Mapping
collections.Sequence = abc.Sequence
collections.Callable = abc.Callable


# imports
from coinbase.wallet.client import Client
import cbpro
import time
import pandas as pd
import numpy as np


# api
handshake = open('api.dat', 'r').read().splitlines()
client = Client(handshake[0], handshake[1])
cbp = cbpro.PublicClient()


# output
wire = open("wire.dat", "w")
pd.set_option('display.max_rows', 500)    
pd.set_option('display.max_columns', 50)


# get all available coinbase crypto names tied to account
def getNames():
	
	wire.write("Available Cryptos:\n")
	names = []
	
	try:
		account = client.get_accounts(limit = 300)	# 264 coins as of 12/13/23
		for wallet in account.data:
			wire.write(str(wallet['balance']['currency']) + "\n")
			names.append(wallet['balance']['currency'])
	
	# cb client response error
	except:
		return 0		
	
	return names


# get public candle data from coinbase
# and calculate volatility
def getVolatility(nameInput):
	
	wire.write("\nCandle Data:\n\n")
	volDict = {}
	
	for name in nameInput:
		try:
			# 60 second granularity * 60 rows -> appx 5 hours
			raw = cbp.get_product_historic_rates(product_id = name + "-USD", granularity = 60)
			
			# chronological order, then send to pandas
			raw.reverse()
			df = pd.DataFrame(raw, columns = ["Date", "Open", "High", "Low", "Close", "Volume"]) 			
			
			# convert date from unix timestamp to readable format
			df['Date'] = pd.to_datetime(df['Date'], unit='s')
			
			# insert log return
			df.insert(6, "Log Return", np.log(df['Close']/df['Close'].shift()))
			
			# calculate the time difference between consecutive data points
			time_diff = df['Date'].diff().mean()
			
			# calculate volatility with a dynamic annualization factor 
			# based on the average time difference
			annualization_factor = 24 / time_diff.total_seconds()  # 24 hours in a day
			vol = (df['Log Return'].std() * (annualization_factor) ** 0.5) * 100
			vol = round(vol, 2)
			volDict[name] = vol
			
			wire.write("\n" + name + " " + str(vol) + "\n")
			wire.write(str(df) + "\n")
			
		except:
			wire.write("\n" + name + "Invalid\n")	
	
	# sort by volatility descending
	volDict = sorted(volDict.items(), key = lambda x:x[1], reverse = True)
	
	wire.write("\n\n" + str(volDict) + "\n")
	print(str(volDict))




##############
###  MAIN  ###
##############


start_time = time.time()	# track runtime


cryptoNames = getNames()	# all cb wallets

# loop until no errors
while cryptoNames == 0:
	cryptoNames = getNames()
	
print(str(cryptoNames))		# console output

getVolatility(cryptoNames)	# calculate volatility based on public cb data


end_time = time.time()
print("--- %s seconds ---" % (end_time - start_time))
print("--- %s minutes ---" % ((end_time - start_time) / 60))

# close file output
wire.close()
