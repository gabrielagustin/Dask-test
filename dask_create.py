'''
Filename: .../dask_create.py
Path: .../Dask-test
Created Date: Sunday, January 26th 2020, 6:50:37 pm
Author: Gabriel Garcia

Copyright (c) 2020 Your Company
'''

import os

import dask.dataframe as daskDataFrame
import pandas


import os
path = os.path.dirname(os.path.abspath(__file__))

print(path)


personIDs = [1,2,3,4,5,6,7,8,9,10]
personLastNames = ['Smith', 'Williams', 'Williams','Jackson','Johnson','Smith','Anderson','Christiansen','Carter','Davidson']
personFirstNames = ['John', 'Bill', 'Jane','Cathy','Stuart','James','Felicity','Liam','Nancy','Christina']
personDOBs = ['1982-10-06', '1990-07-04', '1989-05-06', '1974-01-24', '1995-06-05', '1984-04-16', '1976-09-15', '1992-10-02', '1986-02-05', '1993-08-11']    

peoplePandasDataFrame = pandas.DataFrame({'Person_ID':personIDs, 
        'Last_Name': personLastNames, 
        'First_Name': personFirstNames,
        'Date_of_Birth': personDOBs},
        columns=['Person_ID', 'Last_Name', 'First_Name', 'Date_of_Birth'])    

peopleDaskDataFrame = daskDataFrame.from_pandas(peoplePandasDataFrame, npartitions=6)


print(peopleDaskDataFrame.head())

peopleDaskDataFrame.to_csv(path + '/myfiles.csv', single_file = True)
