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
from prophet import Prophet
import time
from datetime import datetime, timedelta
import csv
import re
import logging
import schedule

logging.getLogger("prophet").disabled = True
logging.getLogger("cmdstanpy").disabled = True

count = 0	# track number of trades

pd.set_option('display.max_rows', 500)		# pandas output config
pd.set_option('display.max_columns', 50)


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
	pred1hr = {}	# name : predicted 1hr % change
	
	idString = ""	# cmcID, cmcID, ...
	
	
	def __init__(self):
						
		global count
		count += 1
		
		start_time = time.time()	# track runtime
		print(count)
		
		# file output
		wireName = "wire" + str(count) + ".dat"
		wire = open(wireName, "w")
		
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
		wire.write("\nFinal Prediction -> " + str(self.pred1hr) + "\n")
		
		wire.close()	# close file output
		
		end_time = time.time()
		print("--- %s seconds ---" % (end_time - start_time))
		print("--- %s minutes ---" % ((end_time - start_time) / 60))

	
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
		
		# output
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

		self.oneHour = sorted(self.oneHour.items(), key = lambda x:x[1], reverse = True)

		# output
		wire.write("One Hour Percentage Change ->\n")
		wire.write(str(self.oneHour) + "\n\n")


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
				
				# wire.write(name + "\n")
				# wire.write(str(df) + "\n\n")

			except:
				wire.write(name + " Invalid\n\n")	

		# sort by volatility descending
		# only the top 22 most volatile cryptos
		self.volDict = sorted(self.volDict.items(), key = lambda x:x[1], reverse = True)[:22]
		
		# output
		# wire.write("Volatility ->\n")
		# wire.write(str(volOut) + "\n\n")	
		wire.write("Top 22 Most Volatile Cryptos ->\n")
		wire.write(str(self.volDict) + "\n\n")
		
		# return list of keys
		vList = []
		for key, value in self.volDict:
			vList.append(key)
		return vList
		
		
	def getHistorical(self, wire, num):
		
		response = self.cmc.cryptocurrency_quotes_historical(id = num, count = 288, convert='USD')

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
		
		obv = pl.Series(talib.OBV(polarsDF["price"], polarsDF["market_cap"]))
		polarsDF = polarsDF.with_columns(pl.Series(name="obv", values = obv))
		
		std = pl.Series(talib.STDDEV(polarsDF["price"]))
		polarsDF = polarsDF.with_columns(pl.Series(name="std", values = std))
		
		macd, macdsignal, macdhist = pl.Series(talib.MACD(polarsDF["price"]))
		polarsDF = polarsDF.with_columns(pl.Series(name="macd", values = macd))
		polarsDF = polarsDF.with_columns(pl.Series(name="macdsignal", values = macdsignal))
		polarsDF = polarsDF.with_columns(pl.Series(name="macdhist", values = macdhist))

		# rename for prophet
		polarsDF = polarsDF.rename({"timestamp" : "ds"})
		polarsDF = polarsDF.rename({"price" : "y"})

		# back to pandas and remove nan values
		df = polarsDF.to_pandas().dropna()
		
		# change to datetime
		df["ds"] = pd.to_datetime(df["ds"])
		
		# output
		# wire.write(str(df) + "\n\n")

		return df
		
		
	def predict(self, wire, df, name):

		# last timestamp
		last_observed_time = df['ds'].max()

		# one hour from the last timestamp
		future_time = last_observed_time + timedelta(hours=1)

		# create dataframe with the future timestamp
		future_date = pd.DataFrame({'ds': [future_time]})

		features = ["percent_change_1h", "volume_24h", "rsi", "upperband", "middleband", "lowerband", "fastk", "fastd", "ema", "sma", "obv", "std", "macd", "macdsignal", "macdhist"]

		# fill in the regressor values for the future timestamp
		for f in features:
			future_date[f] = df[f].iloc[-1]

		# modeling
		model = Prophet(yearly_seasonality = False, weekly_seasonality = False, daily_seasonality = False)
		model.add_seasonality(name = "hourly", period = (1/24), fourier_order = 10)
		
		for f in features:
			model.add_regressor(f)

		model.fit(df[["ds", "y"] + features])
		forecast = model.predict(future_date)

		percentPredict = (( float(forecast['yhat'].iloc[0]) - float(df['y'].iloc[-1]) ) / float(df['y'].iloc[-1]) ) * 100
		# percentPredict = round(percentPredict, 2)
		
		wire.write(name + " " + str(percentPredict) + "\n")
		
		self.pred1hr[name] = percentPredict
		

def run():
	_new = trade()


##############
###  MAIN  ###
##############


# SCHEDULE
schedule.every().hour.at("22:22").do(run)

# keep running 
while True:
	schedule.run_pending()
	time.sleep(1) # pause one second
