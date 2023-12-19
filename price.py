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
import coinmarketcapapi
import time
import pandas as pd
import numpy as np
import csv


# api
handshake = open('api.dat', 'r').read().splitlines()
client = Client(handshake[0], handshake[1])
cbp = cbpro.PublicClient()
cmc = coinmarketcapapi.CoinMarketCapAPI(handshake[2])


# output
wire = open("wire.dat", "w")
pd.set_option('display.max_rows', 500)    
pd.set_option('display.max_columns', 50)


# read from file list of valid cryptos -> 228 as of 12/19/23
def getInclude():
	
	include = []	# list of names
	cmcID = {}		# name : cmcID
	idCMC = {}		# cmcID : name
	build = []		# int list cmcID
	strBuild = ""	# for cmc latest quote -> 1hr % change
	
	with open("include.dat", mode = "r") as infile:
		reader = csv.reader(infile)
		for row in reader:	
			include.append(row[0])
			cmcID[row[0]] = row[1]
			idCMC[row[1]] = row[0]
			build.append(int(row[1]))
			
	build.sort()	# ascending order
	
	for i in build:	# convert to one string
		strBuild += str(i) + ","
	strBuild = strBuild[:-1]
	
	wire.write(str(include) + "\n\n")
	wire.write(str(cmcID) + "\n\n")
	wire.write(str(idCMC) + "\n\n")
	wire.write(strBuild + "\n\n")
	
	yield include	# multiple return values
	yield cmcID
	yield idCMC
	yield strBuild
	
	
# read from file list of invalid cryptos
# stable, dex, delisted, etc
# 36 as of 12/19/23
def getExclude():

	exclude = []	# to return
	
	with open("exclude.dat", mode = "r") as infile:
		for line in infile:
			exclude.append(line.strip())
			
	wire.write(str(exclude) + "\n\n")
	return exclude
	

# get all available coinbase crypto names tied to account
# get assetID and check for new crypto
def getNames(include, exclude):
	
	names = []	# to return
	astID = {}
	
	try:
		account = client.get_accounts(limit = 300)	# 264 as of 12/13/23
		for wallet in account.data:
			# wire.write(str(wallet['name']) + "\n")
			# wire.write(str(wallet) + "\n\n")
			names.append(wallet['name'])
			
			if wallet['name'] in include:
				astID[wallet['name']] = wallet['currency']['asset_id']
				build 
			
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
	wire.write(str(astID) + "\n\n")
	
	yield names
	yield astID
	

# get public candle data from coinbase and calculate volatility
def getVolatility(include):
	
	volDict = {}
	
	for name in include:
		try:
			# 60 second granularity * 300 rows -> at least 5 hours
			# reporting based on volume so could be a couple days
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
			
			wire.write(name + " " + str(vol) + "\n")
			# wire.write(str(df) + "\n")
			
		except:
			wire.write("\n" + name + " Invalid\n")	
	
	# sort by volatility descending
	# only the top 22 most volatile cryptos
	volDict = sorted(volDict.items(), key = lambda x:x[1], reverse = True)[:22]
		
	wire.write("\n\n" + str(volDict) + "\n")




##############
###  MAIN  ###
##############


start_time = time.time()		# track runtime


includeResult = getInclude()	# all valid wallets
include = next(includeResult)	# list of valid crypto names
cmcID = next(includeResult)		# name : cmcID
idCMC = next(includeResult)		# cmcID : name
build = next(includeResult)		# list 

exclude = getExclude()			# invalid wallets

cryptoNames = getNames(include, exclude)

while cryptoNames == 0:			# loop until no errors
	cryptoNames = getNames(include, exclude)
	
names = next(cryptoNames)		# list of all crypto names
astID = next(cryptoNames)		# name : assetID

getVolatility(include)			# calculate volatility


end_time = time.time()
print("--- %s seconds ---" % (end_time - start_time))
print("--- %s minutes ---" % ((end_time - start_time) / 60))

wire.close()				# close file output
