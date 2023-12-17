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
import csv


# api
handshake = open('api.dat', 'r').read().splitlines()
client = Client(handshake[0], handshake[1])
cbp = cbpro.PublicClient()


# output
wire = open("wire.dat", "w")
pd.set_option('display.max_rows', 500)    
pd.set_option('display.max_columns', 50)


# read from file list of valid cryptos
# 229 as of 12/17/23
def getInclude():
	
	include = []	# to return
	
	with open("include.dat", mode = "r") as infile:
		reader = csv.reader(infile)
		for row in reader:	
			include.append(row[0])
			
	wire.write(str(include) + "\n\n")
	return include
	
	
# read from file list of invalid cryptos
# stable, dex, delisted, etc
# 35 as of 12/17/23
def getExclude():

	exclude = []	# to return
	
	with open("exclude.dat", mode = "r") as infile:
		for line in infile:
			exclude.append(line.strip())
			
	wire.write(str(exclude) + "\n\n")
	return exclude
	

# get all available coinbase crypto names tied to account
# check for new cryptos
def getNames(include, exclude):
	
	names = []	# to return
	
	try:
		account = client.get_accounts(limit = 300)	# 264 as of 12/13/23
		for wallet in account.data:
			# wire.write(str(wallet['name']) + "\n")
			# wire.write(str(wallet) + "\n\n")
			names.append(wallet['name'])
			
			if wallet['name'] not in include and wallet['name'] not in exclude:
				wire.write("\n\n")
				wire.write("################\n")
				wire.write("#  NEW CRYPTO  #\n")
				wire.write("################\n")
				wire.write("\n" + str(wallet['name']) + "\n")
				
				
	# cb client response error
	except:
		return 0		
	
	wire.write(str(names) + "\n\n")
	return names


# get public candle data from coinbase and calculate volatility
def getVolatility(include):
	
	volDict = {}
	
	for name in include:
		try:
			# 60 second granularity * 60 rows -> appx 5 hours
			raw = cbp.get_product_historic_rates(product_id = name.replace(" Wallet", "-USD"), granularity = 60)
			
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
			wire.write("\n" + name + " Invalid\n")	
	
	# sort by volatility descending
	volDict = sorted(volDict.items(), key = lambda x:x[1], reverse = True)
		
	wire.write("\n\n" + str(volDict) + "\n")




##############
###  MAIN  ###
##############


start_time = time.time()	# track runtime


include = getInclude()		# valid wallets

exclude = getExclude()		# invalid wallets

cryptoNames = getNames(include, exclude)

while cryptoNames == 0:		# loop until no errors
	cryptoNames = getNames(include, exclude)

getVolatility(include)		# calculate volatility


end_time = time.time()
print("--- %s seconds ---" % (end_time - start_time))
print("--- %s minutes ---" % ((end_time - start_time) / 60))

wire.close()				# close file output
