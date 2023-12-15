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
def getCBcandles(nameInput):
	
	wire.write("\nCandle Data:\n\n")
	
	for name in nameInput:
		try:
			# 300 second granularity * 300 rows -> appx 1 day
			raw = cbp.get_product_historic_rates(product_id = name + "-USD", granularity = 300)
			
			# chronological order, then send to pandas
			raw.reverse()
			df = pd.DataFrame(raw, columns = ["Date", "Open", "High", "Low", "Close", "Volume"]) 			
			
			# convert date from unix timestamp to readable format
			df['Date'] = pd.to_datetime(df['Date'], unit='s')
			
			wire.write("\n" + name + "\n")
			wire.write(str(df))
			wire.write("\n")
			
		except:
			wire.write("\n" + name + "Invalid\n")	




##############
###  MAIN  ###
##############


start_time = time.time()	# track runtime

cryptoNames = getNames()	# all cb wallets

# loop until no errors
while cryptoNames == 0:
	cryptoNames = getNames()
	
print(str(cryptoNames))		# console output

getCBcandles(cryptoNames)	# open high low close volume


end_time = time.time()
print("--- %s seconds ---" % (end_time - start_time))
print("--- %s minutes ---" % ((end_time - start_time) / 60))

# close file output
wire.close()
