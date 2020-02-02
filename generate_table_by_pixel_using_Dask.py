# -*- coding: utf-8 -*-
#!/usr/bin/python

'''
Filename: generate_table_by_pixel_using_Dask.py
Path: /Dask-test
Created Date: Saturday, February 1st 2020, 6:09:01 pm
Author: Gabriel Garcia

Copyright (c) 2020 Your Company
'''

import dask.dataframe as daskDataFrame
import numpy as np
import pandas as pd
from time import time

import functions

LST = []
NDVI = []
SWIR1 = []
SWIR2 = []
DEWPOINT = []
s1 = []
Month = []

pathIn = "/home/ggarcia/Documents/Satellogic/Data/Cataluña/Small_region_Cataluña/ET/Inputs/"

pathOut = "/home/ggarcia/Documents/Satellogic/Tests/GitHubTest/Dask-test/"


# for i in range (6,7):
for i in range (5,6):
    print("mes: "+str(i))
    if(i < 10):
        fileNDVI = pathIn +'Landsat_8_LST_NDVI_SWIR/' +'0'+ str(i)+'2019' + "_NDVI.tif"
        fileLST = pathIn +'Landsat_8_LST_NDVI_SWIR/'+'0'+ str(i)+'2019' + "_LST.tif"
        fileSWIR1 = pathIn +'Landsat_8_LST_NDVI_SWIR/'+'0'+ str(i)+'2019' + "_SWIR1.tif"
        fileSWIR2 = pathIn +'Landsat_8_LST_NDVI_SWIR/'+'0'+ str(i)+'2019' + "_SWIR2.tif"
        fileDEW = pathIn + 'Dewpoint_Temp/'+'0'+ str(i)+'2019' + "_DEWPOINT.tif"
    else:
        fileNDVI = path + str(i) + "_NDVI.tif"
        fileLST = path + str(i) + "_LST.tif"
        fileSWIR1 = path +'0'+ str(i) + "_SWIR1.tif"
        fileSWIR2 = path +'0'+ str(i) + "_SWIR2.tif"
        fileDEW = path +'0'+ str(i) + "_DEWPOINT.tif"

    # print(fileNDVI)
    # print(fileLST)
    src_ds_NDVI, bandNDVI, GeoTNDVI, ProjectNDVI = functions.openFileHDF(fileNDVI, 1)
    src_ds_LST, bandLST, GeoTLST, ProjectLST = functions.openFileHDF(fileLST, 1)
    src_ds_SWIR1, bandSWIR1, GeoTSWIR1, ProjectSWIR1 = functions.openFileHDF(fileSWIR1, 1)
    src_ds_SWIR2, bandSWIR2, GeoTSWIR2, ProjectSWIR2 = functions.openFileHDF(fileSWIR2, 1)
    src_ds_DEW, bandDEW, GeoTDEW, ProjectDEW = functions.openFileHDF(fileDEW, 1)
    # src_ds_s1, bands1, GeoTs1, Projects1 = functions.openFileHDF(fileS1, 1)
    
    
    ### unified resolution and sizes
    nRow, nCol = bandNDVI.shape
    type = "Nearest"
    
    data_src = src_ds_LST
    data_match = src_ds_NDVI
    match = functions.matchData(data_src, data_match, type, nRow, nCol)
    band_matchLST = match.ReadAsArray()
    
    data_src = src_ds_DEW
    data_match = src_ds_NDVI
    match = functions.matchData(data_src, data_match, type, nRow, nCol)
    band_matchDEW = match.ReadAsArray()



    NDVI.append(bandNDVI.flatten())
    LST.append(band_matchLST.flatten())
    SWIR1.append(bandSWIR1.flatten())
    SWIR2.append(bandSWIR2.flatten())
    DEWPOINT.append(band_matchDEW.flatten())
    # s1.append(bands1.flatten())
    Month.append(i)

_NDVI = np.array(NDVI).flatten()
_LST = np.array(LST).flatten()
_SWIR1 = np.array(SWIR1).flatten()
_SWIR2 = np.array(SWIR2).flatten()
_DEWPOINT = np.array(DEWPOINT).flatten()
# _s1 = np.array(s1).flatten()


_SWIR1[_SWIR1>6000] = 0
_SWIR2[_SWIR2>6000] = 0

_SWIR1 = (_SWIR1-np.min(_SWIR1))/(np.max(_SWIR1)-np.min(_SWIR1))
_SWIR2 = (_SWIR2-np.min(_SWIR2))/(np.max(_SWIR2)-np.min(_SWIR2))

# print(_NDVI.shape)
# print(_LST.shape)
# print(_SWIR1.shape)
# print(_SWIR2.shape)
# print(_LST.shape)
# print(_DEWPOINT.shape)


df = pd.DataFrame({'NDVI':_NDVI,
        'SWIR1':_SWIR1, 
        'SWIR2':_SWIR2,
        'LST': _LST,
        'DEWPOINT': _DEWPOINT},
        columns=['NDVI', 'SWIR1', 'SWIR2', 'LST', 'DEWPOINT'])   

df = df.replace(np.nan, -1)

#### test dataframe pandas create .csv file
start_time = time()

df.to_csv(pathOut + "tabla_Pandas_test.csv", decimal = ",")
print("Archivo Validacion creado con exito!")

elapsed_time = time() - start_time
print("Elapsed time for pandas: %.10f seconds." % elapsed_time)


#### test dataframe Dask create .csv file
### create a dask dataframe from pandas dataframe
daskDataFrame = daskDataFrame.from_pandas(df, npartitions=6)

print(daskDataFrame.head())

start_time = time()

daskDataFrame.to_csv(pathOut + "tabla_Dask_test.csv", single_file = True)
print("Archivo Validacion creado con exito!")

elapsed_time = time() - start_time
print("Elapsed time for Dask: %.10f seconds." % elapsed_time)