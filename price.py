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
import logging

# no prophet console output
logging.getLogger("prophet").disabled = True
logging.getLogger("cmdstanpy").disabled = True

# pandas output config
pd.set_option('display.max_rows', 25)
pd.set_option('display.max_columns', 25)


class trade:

	# api connections
	handshake = open('/home/evan/Documents/hourlyTrade/api.dat', 'r').read().splitlines()
	client = Client(handshake[0], handshake[1])
	cbp = cbpro.PublicClient()
	cmc = coinmarketcapapi.CoinMarketCapAPI(handshake[2])
	
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
		
		self.output("Initializer\n\n")
		
		self.readFromFile()
		
		allCryptos = self.checkForNew()

		# loop until no errors
		while allCryptos == 0:
			allCryptos = self.checkForNew()
		
		vList = self.getVolatility()
		
		for name in vList:
		
			df = self.getHistorical(self.cmcID[name])
		
			self.predict(df, name)
		
		self.pred1hr = sorted(self.pred1hr.items(), key = lambda x:x[1], reverse = True)[:1]
		self.output("\nCombined Final Prediction -> " + str(self.pred1hr) + "\n\n")
		
		end_time = time.time()
		
		self.output("Runtime ->\n")
		self.output(str(end_time - start_time) + " seconds\n")
		self.output(str((end_time - start_time) / 60) + " minutes\n\n")
		
		keyList = []
		for key, value in self.pred1hr:
			keyList.append(key)
			self.output(str(key) + "\n")
		
		self.output(str(keyList) + "\n\n")
		
		self.trackMovement(keyList[0])
		
	
	# streamline output file
	def output(self, message):
		self.wire = open(self.wireName, "a")
		self.wire.write(message)
		self.wire.close()
		
	
	# input files
	def readFromFile(self):

		# available cryptos
		with open("/home/evan/Documents/hourlyTrade/include.dat", mode = "r") as infile:
			reader = csv.reader(infile)
			for row in reader:	
				self.include.append(row[0])
				self.cmcID[row[0]] = row[1]
				self.idCMC[row[1]] = row[0]
		
		# unavailable cryptos
		with open("/home/evan/Documents/hourlyTrade/exclude.dat", mode = "r") as infile:
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
			account = self.client.get_accounts(limit = 300)		# 264 as of 12/13/23
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

				# focus on very recent data
				df = df.tail(16)
				
				# average true range volatility
				df['ATR'] = average_true_range(df['High'], df['Low'], df['Close'])

				# calculate percentage change in closing prices
				df['Close_pct_change'] = df['Close'].pct_change() * 100
				
				# calculate normalized ATR by dividing by the close price and time difference
				# only last three values will populate with data
				df['Normalized_ATR'] = df['ATR'] / (df['Close'] * df['Date'].diff().dt.total_seconds())
				
				# remove first row
				df = df.dropna(axis = 'rows')
				
				# average normalized atr
				atr = df['Normalized_ATR'].mean()
				
				# average percentage change in closing prices
				momentum = df['Close_pct_change'].mean()
				
				# combine metrics
				combined_metric = (0.5 * atr) + (0.5 * momentum)
				
				self.output(str(name) + "\n" + str(df) + "\n\n")
				
				# add to dict
				self.volDict[name] = combined_metric
								
			except Exception as e:
				self.output(str(e) + "\n\n")	

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
		
		response = self.cmc.cryptocurrency_quotes_historical(id = num, count = 69, convert='USD')

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

		return df
		
		
	# five minute future price prediction
	# arima and prophet algos
	def predict(self, df, name):
		
		try:
			# last timestamp
			last_observed_time = df['ds'].max()

			# 5 minutes the last timestamp
			future_time = last_observed_time + timedelta(hours = 0.0833)

			# create dataframe with the future timestamp
			future_date = pd.DataFrame({'ds': [future_time]})

			# same for both algos
			features = ["rsi", "fastk", "fastd", "ema", "obv", "std", "macd", "macdsignal", "macdhist", "upperband", "middleband", "lowerband", "price", "volume_24h", 'percent_change_24h', 'percent_change_7d', 'percent_change_30d']

			# fill in the regressor values for the future timestamp
			for f in features:
				future_date[f] = df[f].iloc[-1]		

			# arima forecast
			model = auto_arima(df["y"], exogenous = df[features], trace = True, error_action = "ignore", suppress_warnings = True)
			aforecast = model.predict(n_periods = 1,  exogenous = df[features])
			ar = float(aforecast.iloc[-1])

			# configure prophet
			pmodel = Prophet(yearly_seasonality = False, weekly_seasonality = False, daily_seasonality = False)
			pmodel.add_seasonality(name = "hourly", period = (1/24), fourier_order = 7)
			
			# add features to prophet model
			for f in features:
				pmodel.add_regressor(f)
			
			# prophet forecast
			pmodel.fit(df[["ds", "y"] + features])
			pforecast = pmodel.predict(future_date)
			pr = float(pforecast['yhat'].iloc[0])

			# average two models
			ar = ar * 0.5
			pr = pr * 0.5
			combine = ar + pr

			self.output(name + "\nprice   -> " + str(df['price'].iloc[-1]) + "\nprophet -> " + str(pforecast['yhat'].iloc[0]) + "\narima   -> " + str(aforecast.iloc[-1]) + "\n\n")

			# rule out negative predictions
			# and cases where algos wildly disagree, ie arima -5.2 and prophet 11.5
			if ar > 0 and pr > 0:

				self.pred1hr[name] = combine
		
		except Exception as e:
			self.output(str(e) + "\n\n")
			
	
	# usually one to two minute runtime
	# twelve minute trade window
	def trackMovement(self, name):
		
		# trade performance file
		# reset after code modification
		wire = open("./result.dat", "a")
		
		try:
			
			# most recent coinbase price
			# temporary value for the trade price
			name = name.replace(" Wallet", "-USD")
			quote = self.cbp.get_product_ticker(product_id = name)
			
			# in case of delisted choice
			self.output(str(quote) + "\n\n")
			
			lastPrice = float(quote['price'])
			
			breakOut = False
			
			# check price movement for twelve minutes
			for i in range(12):

				# pause for one minute
				time.sleep(60)
				
				# calculate % change
				quote1 = self.cbp.get_product_ticker(product_id = name)
				newPrice = float(quote1['price'])
				percentChange = ((newPrice - lastPrice) / lastPrice) * 100
								
				self.output(str(i + 1) + "\n")
				self.output(str(newPrice) + " -> " + str(percentChange) + "\n")
														
				# gain				
				if percentChange > 0.49 and i < 11 and breakOut == False:
					self.output("SELL\n\n")
					wire.write(str(percentChange) + ",")
					breakOut = True
					
				# still need to figure out decrease logic
				
				# end of count
				elif i == 11 and breakOut == False:
					self.output("SELL\n\n")
					wire.write(str(percentChange) + ",")
						
		except Exception as e:
			self.output(str(e) + "\n\n")

		wire.close()

		
def run():
	_new = trade()


##############
###  MAIN  ###
##############

run()
