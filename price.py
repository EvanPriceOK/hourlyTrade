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
	



##############
###  MAIN  ###
##############


start_time = time.time()	# track runtime


cryptoNames = getNames()

# loop until error free 
while cryptoNames == 0:
	cryptoNames = getNames()
	
print(str(cryptoNames))


end_time = time.time()
print("--- %s seconds ---" % (end_time - start_time))
print("--- %s minutes ---" % ((end_time - start_time) / 60))

# close file output
wire.close()
