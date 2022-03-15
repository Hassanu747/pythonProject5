import pandas as pd
import numpy as np
import csv
import os
from dask.distributed import Client
import dask.dataframe as dd
import time
import re as regex
import datetime

#commit test
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 150)
unwantedCharList = ['\'','\"','[',']']
subsetColumns = ['product_qty','weekNum','partNum','date_planned','backorderStatus', 'netQty']
timeArray = []
iterArray = []
difference = []
dateArray = []
state = None
dateList = []
startDate = datetime.datetime(2022,2,26)
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
                    ManufacturingDF = pd.read_csv(file, sep=",")
                    timeArray.append(time.perf_counter())
                    iterArray.append(i)


ReceiptsDF = ReceiptsDF[ReceiptsDF.product_id != 'False']
ReceiptsDF['product_id'] = ReceiptsDF['product_id'].str.split(' ')

ReceiptsDF['partNum'] = [row['product_id'][1] for index,row in ReceiptsDF.iterrows()]
ReceiptsDF['partNum'] = ReceiptsDF['partNum'].str.replace('[','')
ReceiptsDF['partNum'] = ReceiptsDF['partNum'].str.replace(']','')
ReceiptsDF['partNum'] = ReceiptsDF['partNum'].str.replace('\'','')
ReceiptsDF['partNum'] = ReceiptsDF['partNum'].str.replace('\"','')
ReceiptsDF['weekNum'] = pd.to_datetime(ReceiptsDF['date_planned']).dt.strftime('%W')

for date in ReceiptsDF['date_planned']:

    extractedDate = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    if extractedDate < startDate:
        state = True

    else:
        state = False
    dateArray.append(state)

ReceiptsDF['backorderStatus'] = dateArray
ReceiptsDF['netQty'] = ReceiptsDF['product_qty'] - ReceiptsDF['qty_received']
ReceiptsDF = ReceiptsDF.loc[(ReceiptsDF['state'] == 'purchase')]
subsetReceiptsDF = ReceiptsDF[subsetColumns]

ManufacturingDF = ManufacturingDF[ManufacturingDF.product_id != 'False']
ManufacturingDF['product_id'] = ManufacturingDF['product_id'].str.split(' ')
ManufacturingDF['partNum'] = [row['product_id'][1] for index,row in ManufacturingDF.iterrows()]
ManufacturingDF['partNum'] = ManufacturingDF['partNum'].str.replace('[','')
ManufacturingDF['partNum'] = ManufacturingDF['partNum'].str.replace(']','')
ManufacturingDF['partNum'] = ManufacturingDF['partNum'].str.replace('\'','')
ManufacturingDF['partNum'] = ManufacturingDF['partNum'].str.replace('\"','')
ManufacturingDF['weekNum'] = ManufacturingDF['x_studio_linked_mrp_scheduled_date'].str.split(' ')
#ManufacturingDF['weekNum'] = pd.to_datetime(ManufacturingDF['x_studio_linked_mrp_scheduled_date']).dt.strftime('%W')
for row in ManufacturingDF['weekNum']:
    dateList.append(row[0])
ManufacturingDF = ManufacturingDF[ManufacturingDF['weekNum'] != 'False']
ManufacturingDF['weekNum'] = dateList



test = 1
