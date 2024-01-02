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
from sklearn.preprocessing import MinMaxScaler
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
pd.set_option('display.max_rows', 80)
pd.set_option('display.max_columns', 25)


class trade:

	# api connections
	handshake = open('/home/evan/Documents/hourlyTrade/api.dat', 'r').read().splitlines()
	client = Client(handshake[0], handshake[1])
	cbp = cbpro.PublicClient()
	cmc = coinmarketcapapi.CoinMarketCapAPI(handshake[2])
	
	# class variables
	# filter cryptos
	include = []
	exclude = []
	
	cmcID   = {}	# name : cmcID and reverse
	idCMC   = {}
	assetID = {}	# name : cbAssetID
	cmPrice = {}	# name : cmcPrice
	cbPrice = {}	# name : cbPrice
	volDict = {}	# name : volatility
	oneHour = {}	# name : 1 hr % change (previous hour)
	pred1hr = {}	# name : predicted 1hr % change
	
	idString = ""	# cmcID, cmcID, ...
	
	
	# everything is run from the initializer
	# schedule with cron instead
	def __init__(self):
						
		start_time = time.time()	# track runtime
		
		# file output
		now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		wireName = str(now) + ".dat"
		wire = open("/home/evan/Documents/hourlyTrade/dat/" + wireName, "w")
		
		self.readFromFile(wire)
		
		allCryptos = self.checkForNew(wire)

		# loop until no errors
		while allCryptos == 0:
			allCryptos = self.checkForNew(wire)

		self.getLatestQuotes(wire)
		
		vList = self.getVolatility(wire)
		
		for name in vList:
		
			df = self.getHistorical(wire, self.cmcID[name])
		
			self.predict(wire, df, name)
		
		self.pred1hr = sorted(self.pred1hr.items(), key = lambda x:x[1], reverse = True)[:1]
		wire.write("\nCombined Final Prediction -> " + str(self.pred1hr) + "\n")
		
		wire.close()	# close file output
		
		end_time = time.time()
		#print("--- %s seconds ---" % (end_time - start_time))
		#print("--- %s minutes ---" % ((end_time - start_time) / 60))

	
	def readFromFile(self, wire):

		# available cryptos
		with open("/home/evan/Documents/hourlyTrade/include.dat", mode = "r") as infile:
			reader = csv.reader(infile)
			for row in reader:	
				self.include.append(row[0])
				self.cmcID[row[0]] = row[1]
				self.idCMC[row[1]] = row[0]
				self.idString += (str(row[1]) + ",")
			
			self.idString = self.idString[:-1]	# remove last ','
		
		# unavailable cryptos
		with open("/home/evan/Documents/hourlyTrade/exclude.dat", mode = "r") as infile:
			for line in infile:
				self.exclude.append(line.strip())

		# wire.write("Available Cryptos ->\n")
		# wire.write(str(self.include) + "\n\n")
		# wire.write("Name : Coinmarketcap ID ->\n")
		# wire.write(str(self.cmcID) + "\n\n")
		# wire.write("Coinmarketcap ID : Name ->\n")
		# wire.write(str(self.idCMC) + "\n\n")
		# wire.write("Coinmarketcap IDs ->\n")
		# wire.write(str(self.idString) + "\n\n")
		# wire.write("Unavailable Cryptos ->\n")
		# wire.write(str(self.exclude) + "\n\n")
	
	
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
					wire.write("################\n\n")
					wire.write(str(wallet['name']) + "\n\n")

		# cb client response error
		except:
			return 0
		
		# wire.write("Name : Coinbase Asset ID ->\n")
		# wire.write(str(self.assetID) + "\n\n")
		
		return 1	


	def getLatestQuotes(self, wire):

		quote = self.cmc.cryptocurrency_quotes_latest(id=self.idString, convert='USD')
		df = pd.DataFrame.from_records(quote.data)

		df = df.iloc[19]	# price data
		
		for name, value in df.items():
			value['USD']['name'] = self.idCMC[name]
			self.oneHour[self.idCMC[name]] = value['USD']['percent_change_1h']
			self.cmPrice[self.idCMC[name]] = value['USD']['price']

		self.oneHour = sorted(self.oneHour.items(), key = lambda x:x[1], reverse = True)

		wire.write("One Hour Percentage Change ->\n")
		wire.write(str(self.oneHour) + "\n\n")
		# wire.write("Coinmarketcap Latest Price ->\n")
		# wire.write(str(self.cmPrice) + "\n\n")


	def getVolatility(self, wire):
		
		for name in self.include:
			try:
				# 60 second granularity * 300 rows -> at least 5 hours
				# reporting based on volume so could be a couple days
				raw = self.cbp.get_product_historic_rates(product_id = name.replace(" Wallet", "-USD"), granularity = 60)

				# chronological order, then send to pandas
				raw.reverse()
				df = pd.DataFrame(raw, columns = ["Date", "Open", "High", "Low", "Close", "Volume"]) 			
				
				# assuming four figure (and plus) cryptos won't move much in an hour
				if df['Close'].iloc[-1] < 1000.00:
					# convert date from unix timestamp to readable format
					df['Date'] = pd.to_datetime(df['Date'], unit='s')

					# average true range volatility
					time_diff = df['Date'].diff().dt.total_seconds()
					high_low = df['High'] - df['Low']
					high_close_prev = abs(df['High'] - df['Close'].shift(1))
					low_close_prev = abs(df['Low'] - df['Close'].shift(1))
					true_range = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1)
					atr = true_range / time_diff
					atr = true_range.mean() * 100	# multiply by 100 for percentage
					atr = round(atr, 2)

					# add to dict
					self.volDict[name] = atr				
					# wire.write(str(df) + "\n\n")
				
			except Exception as e:
				wire.write(str(e) + "\n\n")	

		# sort by volatility descending
		# only the top 29 most volatile cryptos
		self.volDict = sorted(self.volDict.items(), key = lambda x:x[1], reverse = True)[:29]
		wire.write(str(self.volDict) + "\n\n")
		
		# return list of keys
		vList = []
		for key, value in self.volDict:
			vList.append(key)
		
		return vList
		
	
	# more reliable data via coinmarketcap
	# requires >= hobbyist tier
	def getHistorical(self, wire, num):
		
		response = self.cmc.cryptocurrency_quotes_historical(id = num, count = 129, convert='USD')

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
		
		# wire.write(str(df) + "\n\n")

		return df
		
		
	def predict(self, wire, df, name):

		# last timestamp
		last_observed_time = df['ds'].max()

		# one hour from the last timestamp
		future_time = last_observed_time + timedelta(hours = 1)

		# create dataframe with the future timestamp
		future_date = pd.DataFrame({'ds': [future_time]})

		features = ["rsi", "fastk", "fastd", "ema", "obv", "std", "macd", "macdsignal", "macdhist", "upperband", "middleband", "lowerband", "price", "volume_24h", 'percent_change_24h', 'percent_change_7d', 'percent_change_30d']
		
		# fill in the regressor values for the future timestamp
		for f in features:
			future_date[f] = df[f].iloc[-1]		
		
		# arima
		model = auto_arima(df["y"], exogenous = df[features], trace = True, error_action = "ignore", suppress_warnings = True)
		aforecast = model.predict(n_periods = 12,  exogenous = df[features])
		ar = float(aforecast.iloc[-1])
		
		# prophet
		pmodel = Prophet(yearly_seasonality = False, weekly_seasonality = False, daily_seasonality = False)
		pmodel.add_seasonality(name = "hourly", period = (1/24), fourier_order = 7)
		
		for f in features:
			pmodel.add_regressor(f)

		pmodel.fit(df[["ds", "y"] + features])
		pforecast = pmodel.predict(future_date)
		pr = float(pforecast['yhat'].iloc[0])
		
		# average two models
		combine = (ar + pr) / 2
				
		wire.write(name + "\nprice -> " + str(df['price'].iloc[-1]) + "\nprophet -> " + str(pforecast['yhat'].iloc[0]) + "\narima -> " + str(aforecast.iloc[-1]) + "\n\n")
		self.pred1hr[name] = combine
		

def run():
	_new = trade()


##############
###  MAIN  ###
##############

run()
