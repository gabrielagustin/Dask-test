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

path = "/media/ggarcia/Seagate Expansion Drive/Estancia_Italia_2018/Trabajo_Sentinel_NDVI_CONAE/"
pathOut = "/home/ggarcia/Documents/Satellogic/Tests/GitHubTest/Dask-test/"

fechaSMAP = []
fechaSentinel = []
fechaNDVI = []
fechaMYD = []


#
fechaSMAP.append("2015-04-19")
fechaSentinel.append("2015-04-18")
fechaNDVI.append("2015-04-23")
fechaMYD.append("2015-04-18")


fechaSMAP.append("2015-05-02")
fechaSentinel.append("2015-05-02")
fechaNDVI.append("2015-05-09")
fechaMYD.append("2015-05-02")


fechaSMAP.append("2015-05-10")
fechaSentinel.append("2015-05-12")
fechaNDVI.append("2015-05-09")
fechaMYD.append("2015-05-12")


fechaSMAP.append("2015-05-26")
fechaSentinel.append("2015-05-26")
fechaNDVI.append("2015-05-25")
fechaMYD.append("2015-05-25")

### quite porque SAR estaba cortada
#fechaSMAP.append("2015-06-03")
#fechaSentinel.append("2015-06-05")
#fechaNDVI.append("2015-06-10")
#fechaMYD.append("2015-06-05")

fechaSMAP.append("2015-06-19")
fechaSentinel.append("2015-06-19")
fechaNDVI.append("2015-06-26")
fechaMYD.append("2015-06-18")


fechaSMAP.append("2015-06-30")
fechaSentinel.append("2015-06-29")
fechaNDVI.append("2015-06-26")
fechaMYD.append("2015-07-03")


fechaSMAP.append("2015-07-24")
fechaSentinel.append("2015-07-23")
fechaNDVI.append("2015-07-28")
fechaMYD.append("2015-07-20")

fechaSMAP.append("2015-08-17")
fechaSentinel.append("2015-08-16")
fechaNDVI.append("2015-08-13")
fechaMYD.append("2015-08-13")
#
fechaSMAP.append("2015-08-30")
fechaSentinel.append("2015-08-30")
fechaNDVI.append("2015-08-29")
fechaMYD.append("2015-08-29")

fechaSMAP.append("2015-09-10")
fechaSentinel.append("2015-09-09")
fechaNDVI.append("2015-09-14")
fechaMYD.append("2015-09-06")

fechaSMAP.append("2015-09-23")
fechaSentinel.append("2015-09-23")
fechaNDVI.append("2015-09-30")
fechaMYD.append("2015-09-22")
#
#
fechaSMAP.append("2015-10-04")
fechaSentinel.append("2015-10-03")
fechaNDVI.append("2015-06-26")
fechaMYD.append("2015-10-07")


fechaSMAP.append("2015-10-28")
fechaSentinel.append("2015-10-27")
fechaNDVI.append("2015-10-16")
fechaMYD.append("2015-10-24")

fechaSMAP.append("2015-11-13")
fechaSentinel.append("2015-11-10")
fechaNDVI.append("2015-11-17")
fechaMYD.append("2015-11-09")
#
#
fechaSMAP.append("2015-11-21")
fechaSentinel.append("2015-11-20")
fechaNDVI.append("2015-11-17")
fechaMYD.append("2015-11-17")

fechaSMAP.append("2015-12-18")
fechaSentinel.append("2015-12-14")
fechaNDVI.append("2015-12-19")
fechaMYD.append("2015-12-11")
#
fechaSMAP.append("2015-12-28")
fechaSentinel.append("2015-12-28")
fechaNDVI.append("2016-01-01")
fechaMYD.append("2015-12-27")

fechaSMAP.append("2016-01-08")
fechaSentinel.append("2016-01-07")
fechaNDVI.append("2016-01-01")
fechaMYD.append("2016-01-09")

fechaSMAP.append("2016-01-19")
fechaSentinel.append("2016-01-21")
fechaNDVI.append("2016-01-17")
fechaMYD.append("2016-01-17")
#
#### quite porque SAR estaba cortada
##fechaSMAP.append("2016-01-27")
##fechaSentinel.append("2016-01-31")
##fechaNDVI.append("2016-02-02")
##fechaMYD.append("2016-01-25")
##
fechaSMAP.append("2016-02-14")
fechaSentinel.append("2016-02-14")
fechaNDVI.append("2016-02-18")
fechaMYD.append("2016-02-10")
#
#### FALTA MODIS
##fechaSMAP.append("2016-02-25")
##fechaSentinel.append("2016-02-24")
##fechaNDVI.append("2016-02-18")
##fechaMYD.append("2016-02-26")
##
fechaSMAP.append("2016-03-12")
fechaSentinel.append("2016-03-09")
fechaNDVI.append("2016-03-21")
fechaMYD.append("2016-03-13")

fechaSMAP.append("2016-03-20")
fechaSentinel.append("2016-03-19")
fechaNDVI.append("2016-03-21")
fechaMYD.append("2016-03-21")

fechaSMAP.append("2016-04-02")
fechaSentinel.append("2016-04-02")
fechaNDVI.append("2016-04-06")
fechaMYD.append("2016-04-06")
#
## quite por LST con nubes
##fechaSMAP.append("2016-04-13")
##fechaSentinel.append("2016-04-12")
##fechaNDVI.append("2016-04-22")
##fechaMYD.append("2016-04-22")
#
fechaSMAP.append("2016-04-24")
fechaSentinel.append("2016-04-26")
fechaNDVI.append("2016-04-22")
fechaMYD.append("2016-04-22")
#
fechaSMAP.append("2016-05-20")
fechaSentinel.append("2016-05-20")
fechaNDVI.append("2016-05-08")
fechaMYD.append("2016-05-20")




Smap = []
Ts = []
Ts2 = []
Ta = []
HR = []
PP = []
Ea = []
sigma0 = []
NDVI = []
Et = []
date = []
Latidud = []
Longitud = []
la = []
lo = []
pp=[]


print(len(fechaSMAP))

for i in range(0,len(fechaSMAP)):

    print(fechaSMAP[i])

    ####------------------------------------------------------------------------
    ### humedad de suelo de SMAP 9km
    fileSmap = path + "SMAP/SMAP-10km/"+fechaSMAP[i]+"/soil_moisture.img"
    src_ds_Smap, bandSmap, GeoTSmap, ProjectSmap = functions.openFileHDF(fileSmap, 1)
    ####------------------------------------------------------------------------
    ### temperatura de superficie de SMAP 9km
    fileTs = path + "SMAP/SMAP-10km/"+fechaSMAP[i]+"/surface_temperature.img"
    src_ds_Ts, bandTs, GeoTTs, ProjectTs = functions.openFileHDF(fileTs, 1)
    ####------------------------------------------------------------------------
    ##### SM recorte 
    fileSMAP_subset = path + "SMAP/SMAP-10km/subset_0_of_nueva.data/SM.img"    
    src_ds_Smap_subset, bandSmap_subset, GeoTSmap_subset, ProjectSmap_subset = functions.openFileHDF(fileSMAP_subset, 1)
    ####------------------------------------------------------------------------
    ### temperatura de suelo LST de MODIS a 1km
    fileLST = path +"MOD11A2/"+fechaSentinel[i]+"/subset.data/LST.img"
    src_ds_LST, bandLST, GeoTLST, ProjectLST = functions.openFileHDF(fileLST, 1)
    ####------------------------------------------------------------------------
    ### evapotranspiracion de MODIS a 500m
    fileEt = path + "MYD16/"+fechaMYD[i]+"/MYD16A_reprojected.data/ET_500m.img"
    src_ds_Et, bandEt, GeoTEt, ProjectEt = functions.openFileHDF(fileEt, 1)
    ####------------------------------------------------------------------------
    ### NDVI MODIS 
    fileNDVI = path + "MODIS/"+fechaNDVI[i]+"/NDVI_reproyectado_recortado"
    src_ds_NDVI, bandNDVI, GeoTNDVI, ProjectNDVI = functions.openFileHDF(fileNDVI, 1)
    ####------------------------------------------------------------------------
    ### GPM
    # filePP = path + "GPM/"+fechaSentinel[i]+"/recorte.img"
    # src_ds_PP, bandPP, GeoTPP, ProjectPP = functions.openFileHDF(filePP, 1)
    ####------------------------------------------------------------------------
    #### SAR data at 1km con multilooking
    fileSar = "/media/ggarcia/Seagate Expansion Drive/Imagenes_satelitales/Sentinel/Cordoba/Sentinel_30m_1km/" +fechaSentinel[i]+"/subset_1km/Sigma0_VV_db.img"
    src_ds_Sar, bandSar, GeoTSar, ProjectSar = functions.openFileHDF(fileSar, 1)



    nRow, nCol = bandSmap_subset.shape

    type = "Nearest"
    data_src = src_ds_Smap
    data_match = src_ds_Smap_subset
    match = functions.matchData(data_src, data_match, type, nRow, nCol)
    band_matchSM = match.ReadAsArray()#  

#    fig, ax = plt.subplots()
#    im0 = ax.imshow(band_matchSM, interpolation='None',cmap='gray')
    
    data_src = src_ds_Ts
    data_match = src_ds_Smap_subset
    match = functions.matchData(data_src, data_match, type, nRow, nCol)
    band_matchTs = match.ReadAsArray()#  
    
#    fig, ax = plt.subplots()
#    im1 = ax.imshow(band_matchTs, interpolation='None',cmap='gray')

    data_src = src_ds_Et
    data_match = src_ds_Smap_subset
    match = functions.matchData(data_src, data_match, type, nRow, nCol)
    band_matchEt = match.ReadAsArray()
    
#    fig, ax = plt.subplots()
#    im1 = ax.imshow(band_matchEt, interpolation='None',cmap='gray')

    data_src = src_ds_NDVI
    data_match = src_ds_Smap_subset
    match = functions.matchData(data_src, data_match, type, nRow, nCol)
    band_matchNDVI = match.ReadAsArray()
    
#    fig, ax = plt.subplots()
#    im1 = ax.imshow(band_matchNDVI, interpolation='None',cmap='gray')

    data_src = src_ds_LST
    data_match = src_ds_Smap_subset
    match = functions.matchData(data_src, data_match, type, nRow, nCol)
    band_matchLST = match.ReadAsArray()

#    fig, ax = plt.subplots()
#    im3 = ax.imshow(band_matchLST, interpolation='None',cmap='gray')


#    type = "Nearest"
    type = "Average"
    data_src = src_ds_Sar
    data_match = src_ds_Smap_subset
    match = functions.matchData(data_src, data_match, type, nRow, nCol)
    band_matchSar = match.ReadAsArray()


#    fig, ax = plt.subplots()
#    im1 = ax.imshow(band_matchSar, interpolation='None',cmap='gray')

#    plt.show()

    Smap.append(band_matchSM.flatten())
    Ts.append(band_matchTs.flatten())
    Ts2.append(band_matchLST.flatten())
    sigma0.append(band_matchSar.flatten())
    NDVI.append(band_matchNDVI.flatten())
    Et.append(band_matchEt.flatten())
    
    jj = band_matchEt.flatten().shape[0]
#    print(jj)
    pp = [''  for pp in range(0,jj)]
    #print pp[0]
    pp[0]= fechaSMAP[i]
    date.append(np.array(pp))


_SMAP = np.array(Smap).flatten()


_Ts = np.array(Ts).flatten()

_PP = np.array(PP).flatten()

_Ts2 = np.array(Ts2).flatten()


_Sigma0 = np.array(sigma0).flatten()

_NDVI = np.array(NDVI).flatten()

_Et = np.array(Et).flatten()

_date = np.array(date).flatten()


df = pd.DataFrame({'Date':_date, 
        'SM_SMAP':_SMAP, 
        'T_s' :_Ts,
        'Sigma0':_Sigma0,
        'NDVI':_NDVI,
        'Et': _Et,
        'Date':_date,
        'T_s_modis':_Ts2
        },
        columns=['Date','SM_SMAP','T_s','Sigma0','NDVI','Et','T_s_modis'])   

start_time = time()

df.to_csv(pathOut + "tabla_completa_Test_9km_2.csv", decimal = ",")
print("Archivo Validacion creado con exito!")

elapsed_time = time() - start_time
print("Elapsed time for pandas: %.10f seconds." % elapsed_time)

### create a dask dataframe from pandas dataframe
daskDataFrame = daskDataFrame.from_pandas(df, npartitions=6)

print(daskDataFrame.head())

start_time = time()

daskDataFrame.to_csv(pathOut + "tabla_completa_Test_9km.csv", single_file = True)
print("Archivo Validacion creado con exito!")

elapsed_time = time() - start_time
print("Elapsed time for Dask: %.10f seconds." % elapsed_time)