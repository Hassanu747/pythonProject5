import pandas as pd
import numpy as np
import csv
import os
from dask.distributed import Client
import dask.dataframe as dd
import time
import re as regex
#commit test
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 150)
unwantedCharList = ['\'','\"','[',']']
timeArray = []
iterArray = []
difference = []
for i in range(100):
    for file in os.listdir():
            if file.endswith('.csv'):
                if file == '0.csv':
                    InventoryDF = pd.read_csv(file, sep=",")
                    timeArray.append(time.perf_counter())
                    iterArray.append(i)
                if file == '1.csv':
                    ReceiptsDF = pd.read_csv(file, sep=",")
                    timeArray.append(time.perf_counter())
                    iterArray.append(i)
                if file == '2.csv':
                    PurchaseDF = pd.read_csv(file, sep=",")
                    timeArray.append(time.perf_counter())
                    iterArray.append(i)

#=sumifs(RECEIPTS_NET, RECEIPTS_MFR,$B28, RECEIPTS_PART, $B29, RECEIPTS_BO, true, RECEIPTS_STATUS, "Purchase")
ReceiptsDF = ReceiptsDF.sort_values(by = 'id',ascending = False)
PurchaseDF
ReceiptsDF = ReceiptsDF[ReceiptsDF.product_id != 'False']
ReceiptsDF['product_id'] = ReceiptsDF['product_id'].str.split(' ')
#x = regex.findall('\[.+?\]',test['product_id'][1])
ReceiptsDF['partNum'] = [row['product_id'][1] for index,row in ReceiptsDF.iterrows()]
ReceiptsDF['partNum'] = ReceiptsDF['partNum'].str.replace('[','')
ReceiptsDF['partNum'] = ReceiptsDF['partNum'].str.replace(']','')
ReceiptsDF['partNum'] = ReceiptsDF['partNum'].str.replace('\'','')
ReceiptsDF['partNum'] = ReceiptsDF['partNum'].str.replace('\"','')
test = 1
