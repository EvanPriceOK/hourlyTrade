# deprecated code workaround
import collections
from collections import abc
collections.MutableMapping = abc.MutableMapping
collections.Mapping = abc.Mapping
collections.Sequence = abc.Sequence
collections.Callable = abc.Callable

# imports
from coinbase.wallet.client import Client as cbc
from binance import Client as bnc
import cbpro
import coinmarketcapapi
import pandas as pd
import polars as pl
import numpy as np
import talib
from ta.volatility import average_true_range
from prophet import Prophet
from pmdarima import auto_arima
import time
from datetime import datetime, timedelta
import csv
import re

# pandas output config
pd.set_option('display.max_rows', 25)
pd.set_option('display.max_columns', 25)

# global variable
# 	prevents making the same trade
#	twice in a row
lastTrade = "First Run"


class trade:

	# api connections
	handshake = open('/home/evan/Documents/hourlyTrade/api.dat', 'r').read().splitlines()
	
	# for coinbase account trades
	client = cbc(handshake[0], handshake[1])
	
	# for binance public data
	# bn_client = bnc(handshake[2], handshake[3], tld = "us")
	
	# for coinmarketcap data
	# requires hobbyist tier
	cmc = coinmarketcapapi.CoinMarketCapAPI(handshake[4])
	
	# for coinbase public data
	cbp = cbpro.PublicClient()
	
	# set up output
	now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	wireName = "/home/evan/Documents/hourlyTrade/dat/" + str(now) + ".dat"
	wire = open(wireName, "w")
	wire.write("####################\n")
	wire.write("##  BEGIN SCRIPT  ##\n")
	wire.write("####################\n\n")
	wire.close()
		
	# class variables
	include = []
	exclude = []
	
	cmcID   = {}	# name : cmcID and reverse
	idCMC   = {}
	assetID = {}	# name : cbAssetID
	volDict = {}	# name : volatility
	pred1hr = {}	# name : predicted 1hr % change
	
	
	# everything is run from the initializer
	# schedule fifteen minute trade intervals with cron
	def __init__(self):
						
		start_time = time.time()	# track runtime
		
		global lastTrade
				
		self.readFromFile()
		
		allCryptos = self.checkForNew()

		# loop until no errors
		while allCryptos == 0:
			allCryptos = self.checkForNew()
		
		vList = self.getVolatility()
		
		for name in vList:
		
			df = self.getHistorical(self.cmcID[name])
		
			self.predict(df, name)
		
		self.pred1hr = sorted(self.pred1hr.items(), key = lambda x:x[1], reverse = True)[:2]
		
		self.output("Top 2 -> " + str(self.pred1hr) + "\n\n")
		
		# don't do the same trade twice
		self.output("lastTrade -> " + lastTrade + "\n\n")
		self.output(str(next(iter(self.pred1hr[0]))) + "\n\n")
		
		if lastTrade == str(next(iter(self.pred1hr[0]))):
			self.output("MATCH\n\n")
			self.pred1hr.pop(0)
		else:
			self.output("NOT MATCH\n\n")
			self.pred1hr.pop(1)
		
		# check price of last trade before new pick output
		# track price movement after sell
		if lastTrade != "First Run":

			name = lastTrade.replace(" Wallet", "-USD")
			self.output("Last Trade Price ->\n")
			quote = self.cbp.get_product_ticker(product_id = name)
			
			# in case of delisted choice
			self.output(str(quote) + "\n\n")
		
		self.output("\nCombined Final Prediction -> " + str(self.pred1hr[0]) + "\n\n")
		
		end_time = time.time()
		
		self.output("\nRuntime ->\n")
		self.output(str(end_time - start_time) + " seconds\n")
		self.output(str((end_time - start_time) / 60) + " minutes\n\n")
		
		lastTrade = str(next(iter(self.pred1hr[0])))
		
		self.trackMovement(lastTrade)
		
	
	# streamline output file
	def output(self, message):
		self.wire = open(self.wireName, "a")
		self.wire.write(message)
		self.wire.close()
		
	
	# input files
	def readFromFile(self):

		# available cryptos
		with open("/home/evan/Documents/hourlyTrade/cbInclude.dat", mode = "r") as infile:
			reader = csv.reader(infile)
			for row in reader:	
				self.include.append(row[0])
				self.cmcID[row[0]] = row[1]
				self.idCMC[row[1]] = row[0]
		
		# unavailable cryptos
		with open("/home/evan/Documents/hourlyTrade/cbExclude.dat", mode = "r") as infile:
			for line in infile:
				self.exclude.append(line.strip())

		
	# need to automate this every time script is run
	# manual process right now
	def checkForDelisted(self):
		
		account = self.client.get_accounts(limit = 300)
		for wallet in account.data:
			
			try:
				
				# fix name for coinbase product ticker
				name = str(wallet['name'])
				name = name.replace(" Wallet", "-USD")
				
				# the output will specify if the wallet is delisted
				quote = self.cbp.get_product_ticker(product_id = name)
				self.output(str(wallet['name']) + "\n" + str(quote) + "\n\n")
		
			except Exception as e:
				
				self.output(str(e) + "\n\n")
	
	
	# loop all wallets and look for ones not 
	# listed on include or exclude file
	def checkForNew(self):
		
		# sometimes get_accounts will error out
		try:
			account = self.client.get_accounts(limit = 300)
			for wallet in account.data:

				if wallet['name'] in self.include:
					self.assetID[wallet['name']] = wallet['currency']['asset_id']
					
				if wallet['name'] not in self.include and wallet['name'] not in self.exclude:
					self.output("##################\n")
					self.output("##  NEW CRYPTO  ##\n")
					self.output("##################\n\n")
					self.output(str(wallet['name']) + "\n\n")

		# cb client response error
		except:
			return 0
		
		# success
		return 1	

	
	# choose which cryptos go to modeling
	# looking for volatile ones on an upswing
	def getVolatility(self):
		
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
				
				# average true range volatility
				df['ATR'] = average_true_range(df['High'], df['Low'], df['Close'])

				# calculate percentage change in closing prices
				df['Close_pct_change'] = df['Close'].pct_change() * 100
				
				# calculate normalized ATR by dividing by the close price and time difference
				# only last three values will populate with data
				df['Normalized_ATR'] = df['ATR'] / (df['Close'] * df['Date'].diff().dt.total_seconds())
				
				# calculate logarithmic returns
				df['Log_Ret'] = np.log(df['High'] / df['Low'])
				
				# remove NA rows
				df = df.dropna(axis = 'rows')
				
				# all df rows for volatility
				# calculate historical volatility without a rolling window
				vol = df['Log_Ret'].std() * np.sqrt(len(df))
				
				# 80 "minutes" for atr
				df = df.tail(80)
				
				# average normalized atr
				atr = df['Normalized_ATR'].mean()
				
				# tiny positive momentum
				df = df.tail(4)
				
				# average percentage change in closing prices
				momentum = df['Close_pct_change'].mean()
				
				# combine metrics
				combined_metric = (0.33 * vol) + (0.33 * atr) + (0.33 * momentum)
				
				self.output(str(name) + "\n" + str(df) + "\n\n")
				
				# add to dict
				self.volDict[name] = combined_metric
								
			except Exception as e:
				self.output(str(e) + "\n\n")	

		self.output("Vol ->\n")
		self.output(str(self.volDict) + "\n\n")
		
		# sort top thirty by volatility value descending
		self.volDict = sorted(self.volDict.items(), key = lambda x:x[1], reverse = True)[:30]
		
		self.output(str(self.volDict) + "\n\n")
		
		# return list of keys only
		vList = []
		for key, value in self.volDict:
			vList.append(key)
		
		return vList
		
	
	# more reliable data via coinmarketcap
	# requires >= hobbyist tier
	def getHistorical(self, num):
		
		response = self.cmc.cryptocurrency_quotes_historical(id = num, count = 81, convert='USD')

		# parse text
		response = str(response)
		response = response.replace(response[:31], "")
		response = response[:-1]
		response = response.replace("[","")
		response = response.replace("]","")

		while response[-1] != "}":
			response = response[:-1]

		lines = re.split("\}\}\}", response)    # make list
		lines.pop()

		dfList = []
		dictList = []

		for i in lines:
			i = i.replace(", {", "").replace("{", "")
			i = i.replace(i[:57], "")
			dfList.append(i)

			res = []
			for sub in i.split(', '):
				if ':' in sub:
					res.append(map(str.strip, sub.split(':', 1)))
			res = dict(res)
			dictList.append(res)

		# send to dataframe
		df = pd.DataFrame(dictList)
		
		# remove columns with static values
		df.drop(['\'total_supply\'', '\'circulating_supply\''], axis = 1, inplace = True)
		
		# send to polars for multithreading
		polarsDF = pl.DataFrame(df, schema=['percent_change_1h', 'percent_change_24h', 'percent_change_7d', 'percent_change_30d', 'price', 'volume_24h', 'market_cap', 'timestamp'])
		
		# technical analysis indicators
		rsi = pl.Series(talib.RSI(polarsDF["price"]))
		polarsDF = polarsDF.with_columns(pl.Series(name="rsi", values=rsi))
		
		upperband, middleband, lowerband = pl.Series(talib.BBANDS(polarsDF["price"]))
		polarsDF = polarsDF.with_columns(pl.Series(name = "upperband", values = upperband))
		polarsDF = polarsDF.with_columns(pl.Series(name = "middleband", values = middleband))
		polarsDF = polarsDF.with_columns(pl.Series(name = "lowerband", values = lowerband))
		
		fastk, fastd = pl.Series(talib.STOCHRSI(polarsDF["price"]))
		polarsDF = polarsDF.with_columns(pl.Series(name="fastk", values = fastk))
		polarsDF = polarsDF.with_columns(pl.Series(name="fastd", values = fastd))
				
		ema = pl.Series(talib.EMA(polarsDF["price"]))
		polarsDF = polarsDF.with_columns(pl.Series(name="ema", values = ema))
		
		sma = pl.Series(talib.SMA(polarsDF["price"]))
		polarsDF = polarsDF.with_columns(pl.Series(name="sma", values = sma))
		
		obv = pl.Series(talib.OBV(polarsDF["price"], polarsDF["volume_24h"]))
		polarsDF = polarsDF.with_columns(pl.Series(name="obv", values = obv))
		
		std = pl.Series(talib.STDDEV(polarsDF["price"]))
		polarsDF = polarsDF.with_columns(pl.Series(name="std", values = std))
		
		macd, macdsignal, macdhist = pl.Series(talib.MACD(polarsDF["price"]))
		polarsDF = polarsDF.with_columns(pl.Series(name="macd", values = macd))
		polarsDF = polarsDF.with_columns(pl.Series(name="macdsignal", values = macdsignal))
		polarsDF = polarsDF.with_columns(pl.Series(name="macdhist", values = macdhist))

		# rename for prophet
		polarsDF = polarsDF.rename({"timestamp" : "ds"})
		polarsDF = polarsDF.rename({"percent_change_1h" : "y"})

		# back to pandas and remove 33 rows of nan values
		df = polarsDF.to_pandas().dropna()
		
		# change to datetime
		df["ds"] = pd.to_datetime(df["ds"])
		
		self.output(str(self.idCMC[num]) + "\n")
		self.output(str(df) + "\n\n")

		return df
		
		
	# five minute future price prediction
	# arima algo
	def predict(self, df, name):
		
		try:
			
			features = ["rsi", "fastk", "fastd", "ema", "obv", "std", "macd", "macdsignal", "macdhist", "upperband", "middleband", "lowerband", "price", "volume_24h", 'percent_change_24h', 'percent_change_7d', 'percent_change_30d']

			# arima forecast
			model = auto_arima(df["y"], exogenous = df[features], trace = True, error_action = "ignore", suppress_warnings = True)
			aforecast = model.predict(n_periods = 1,  exogenous = df[features])
			ar = float(aforecast.iloc[-1])

			self.output(name + "\nprice   -> " + str(df['price'].iloc[-1]) + "\narima   -> " + str(ar) + "\n\n")

			self.pred1hr[name] = ar
		
		except Exception as e:
			self.output(str(e) + "\n\n")
			
	
	# update price every fifteen seconds
	def trackMovement(self, name):
		
		# trade performance file
		# reset after code modification
		wire = open("/home/evan/Documents/hourlyTrade/result.dat", "a")
		
		try:
			
			# most recent coinbase price
			# temporary value for the trade price
			name = name.replace(" Wallet", "-USD")
			quote = self.cbp.get_product_ticker(product_id = name)
			
			# in case of delisted choice
			self.output(str(quote) + "\n\n")
			
			lastPrice = float(quote['price'])
			
			# counter
			i = 0
			
			# check price movement
			while True:

				# pause for fifteen seconds
				time.sleep(15)
				
				# one quarter of a minute increment
				i += 0.25
				
				# calculate % change
				quote1 = self.cbp.get_product_ticker(product_id = name)
				newPrice = float(quote1['price'])
				percentChange = ((newPrice - lastPrice) / lastPrice) * 100
								
				self.output(str(i) + "\n")
				self.output(str(newPrice) + " -> " + str(percentChange) + "\n")
														
				# gain				
				if percentChange > 1.5:
					self.output("SELL\n\n")
					wire.write(str(percentChange) + ",")
					wire.close()
					
					# break circular reference by setting reference to None
					self._new_trade_instance = None
					self._new_trade_instance = trade()
		
				# still need to figure out decrease logic
				# or maybe just not...
					
		except Exception as e:
			self.output(str(e) + "\n\n")

		
def run():
	_new = trade()


##############
###  MAIN  ###
##############

run()


# this prints out a ton of info, including the base_id of
# each crypto to make the trades work
# base_id goes into source_asset and target_asset as shown below
# in next chunk of code
#
# prices = client._get("v2", "assets", "prices", params={
#    "base": "USD",
#	 "filter": "holdable",
#	 "resolution": "latest"
# })
# print(f"Status Code: {prices.status_code}")
# print(f'Response Body: {prices.content.decode("utf8")}')


# this converts $5 of one crypto to another
# requires base_id for each
# works with no output for now, updates online quick, ~30 seconds
#
# r = client._post('v2', "trades", data={
#    "amount":"5.00",
#    "amount_asset":"USD",
#    "amount_from":"input",
#    "source_asset":"",
#    "target_asset":""
#    }
# )
# result = r.json()
# trade_id = result['data']['id']
# client._post("v2", "trades", trade_id, "commit")



	#######################
	## binance functions ##
	#######################
	
	# potentially could use
	
	# input files
	# def binInput(self):

		# available cryptos
		# with open("/home/evan/Documents/hourlyTrade/cbInclude.dat", mode = "r") as infile:
			# reader = csv.reader(infile)
			# for row in reader:	
				# self.include.append(row[0])
				# self.cmcID[row[0]] = row[1]
				# self.idCMC[row[1]] = row[0]
		
		# unavailable cryptos
		# with open("/home/evan/Documents/hourlyTrade/cbExclude.dat", mode = "r") as infile:
			# for line in infile:
				# self.exclude.append(line.strip())
	
	
	# available binance cryptos
	# def binCryptos(self):
		
		# try:
			# get account information
			# account_info = self.client.get_account()

			# self.output("Available ->\n")	

			#for crypto in account_info['balances']:
				# self.output(str(crypto['asset']) + "\n")


			# Get all exchange info
			# exchange_info = self.client.get_exchange_info()

			# Extract available cryptocurrencies (assets)
			# available_cryptos = [symbol['baseAsset'] for symbol in exchange_info['symbols']]

			# Remove duplicates and sort
			# available_cryptos = sorted(set(available_cryptos))

			# Print the list of available cryptocurrencies
			# self.output("Available cryptocurrencies:\n")

			# for crypto in available_cryptos:
				
				# try:
					
					# self.output(str(crypto) + "\n")

					# Make the API request using the python-binance library
					# klines = self.client.get_klines(symbol = str(crypto) + "USDT", interval = "1m", limit = 120)

					# Convert the data to a Pandas DataFrame
					# df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])

					# Convert timestamp to datetime format
					# df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

					# Display the DataFrame
					# self.output(str(df) + "\n\n")
				
				# except Exception as e:
					# self.output(str(e) + "\n\n")		
		
		# except Exception as e:
			# self.output(str(e) + "\n\n")	
