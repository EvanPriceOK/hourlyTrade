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

count = 0	# track number of trades


class trade:

	# api connections
	handshake = open('api.dat', 'r').read().splitlines()
	client = Client(handshake[0], handshake[1])
	cbp = cbpro.PublicClient()
	cmc = coinmarketcapapi.CoinMarketCapAPI(handshake[2])
	
	include = []	# for crypto names
	exclude = []
	
	cmcID = {}		# name : cmcID and reverse
	idCMC = {}
	assetID = {}	# name : cbAssetID
	volDict = {}	# name : volatility
	oneHour = {}	# name : 1 hr % change
	
	idString = ""	# cmcID, cmcID, ...
	
	
	def __init__(self):
						
		global count
		count += 1
		
		# file output and pandas output config
		wireName = "wire" + str(count) + ".dat"
		wire = open(wireName, "w")
		pd.set_option('display.max_rows', 500)    
		pd.set_option('display.max_columns', 50)
		
		self.readFromFile(wire)
		
		allCryptos = self.checkForNew(wire)

		# loop until no errors
		while allCryptos == 0:
			allCryptos = self.checkForNew(wire)

		self.getVolatility(wire)
		
		self.getLatestQuotes(wire)
		
		wire.close()	# close file output

	
	def readFromFile(self, wire):

		# available cryptos
		with open("include.dat", mode = "r") as infile:
			reader = csv.reader(infile)
			for row in reader:	
				self.include.append(row[0])
				self.cmcID[row[0]] = row[1]
				self.idCMC[row[1]] = row[0]
				self.idString += (str(row[1]) + ",")
			
			self.idString = self.idString[:-1]	# remove last ','
		
		# unavailable cryptos
		with open("exclude.dat", mode = "r") as infile:
			for line in infile:
				self.exclude.append(line.strip())

		# output
		wire.write("Available Cryptos ->\n")
		wire.write(str(self.include) + "\n\n")
		wire.write("Name : Coinmarketcap ID ->\n")
		wire.write(str(self.cmcID) + "\n\n")
		wire.write("Coinmarketcap ID : Name ->\n")
		wire.write(str(self.idCMC) + "\n\n")
		wire.write("Coinmarketcap IDs ->\n")
		wire.write(str(self.idString) + "\n\n")
		wire.write("Unavailable Cryptos ->\n")
		wire.write(str(self.exclude) + "\n\n")
	
	
	def checkForNew(self, wire):
		
		# sometimes get_accounts will error out
		try:
			account = self.client.get_accounts(limit = 300)		# 264 as of 12/13/23
			for wallet in account.data:

				if wallet['name'] in self.include:
					self.assetID[wallet['name']] = wallet['currency']['asset_id']
					
				if wallet['name'] not in self.include and wallet['name'] not in self.exclude:
					wire.write("################\n")
					wire.write("#  NEW CRYPTO  #\n")
					wire.write("################\n")
					wire.write("\n" + str(wallet['name']) + "\n")

		# cb client response error
		except:
			return 0
		
		# output
		wire.write("Name : Coinbase Asset ID ->\n")
		wire.write(str(self.assetID) + "\n\n")
		
		return 1	


	def getVolatility(self, wire):

		volOut = []
		
		for name in self.include:
			try:
				# 60 second granularity * 300 rows -> at least 5 hours
				# reporting based on volume so could be a couple days
				raw = self.cbp.get_product_historic_rates(product_id = name.replace(" Wallet", "-USD"), granularity = 60)

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
				annualization_factor = 24 / time_diff.total_seconds()	# 24 hours in a day
				vol = (df['Log Return'].std() * (annualization_factor) ** 0.5) * 100
				vol = round(vol, 2)
				self.volDict[name] = vol

				volOut.append(name + " " + str(vol))
				
				wire.write(name + "\n")
				wire.write(str(df) + "\n\n")

			except:
				wire.write(name + " Invalid\n\n")	

		# sort by volatility descending
		# only the top 22 most volatile cryptos
		self.volDict = sorted(self.volDict.items(), key = lambda x:x[1], reverse = True)[:22]

		# output
		wire.write("Volatility ->\n")
		wire.write(str(volOut) + "\n\n")	
		wire.write("Top 22 Most Volatile Cryptos ->\n")
		wire.write(str(self.volDict) + "\n\n")


	def getLatestQuotes(self, wire):

		quote = self.cmc.cryptocurrency_quotes_latest(id=self.idString, convert='USD')
		df = pd.DataFrame.from_records(quote.data)

		df = df.iloc[19]	# price data

		for name, value in df.items():
			value['USD']['name'] = self.idCMC[name]
			self.oneHour[self.idCMC[name]] = value['USD']['percent_change_1h']

		self.oneHour = sorted(self.oneHour.items(), key = lambda x:x[1], reverse = True)

		# output
		wire.write("One Hour Percentage Change ->\n")
		wire.write(str(self.oneHour) + "\n\n")


##############
###  MAIN  ###
##############

start_time = time.time()	# track runtime

_new = trade()

end_time = time.time()
print("--- %s seconds ---" % (end_time - start_time))
print("--- %s minutes ---" % ((end_time - start_time) / 60))
