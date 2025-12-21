'''
######################################################################
## ARISTOTLE UNIVERSITY OF THESSALONIKI
## PERSLAB
## REMOTE SENSING AND EARTH OBSERVATION TEAM
##
## DATE:             Dec-2025
## SCRIPT:           dem_ingestion.py
## AUTHOR:           Vangelis Fotakidis (fotakidis@topo.auth.gr)
##
## DESCRIPTION:      Script to retrieve Elevation and Aspect from Copernicus DEM 30m
##
#######################################################################
'''

import datacube
from datacube.index.hl import Doc2Dataset
from eodatasets3 import serialise


import pystac_client
from planetary_computer import sign_inplace

import odc.stac
from rasterio.enums import Resampling

import geopandas as gpd
from odc.geo.geom import BoundingBox
from odc.geo.geobox import GeoBox

from dask.distributed import LocalCluster, Client
import tempfile

import xarray as xr
import rioxarray as rxr
from xrspatial import aspect
import numpy as np

import gc 
import json
import datetime, pytz
from pathlib import Path
import time

from utils.metadata import prepare_eo3_metadata_NAS
from utils.utils import mkdir, setup_logger

import argparse, json, sys, os, datetime, pytz

# Ignore warnings
import warnings
import logging
warnings.filterwarnings('ignore') 
logging.getLogger("distributed.worker.memory").setLevel(logging.ERROR)


def dem_writing_indexing(dem: xr.Dataset, tile: gpd.GeoDataFrame):
    """For a tile: clip DEM, write on disk, and index into datacube.

    Args:
        dem (xr.Dataset): The DEM dataset with elevation and aspect
    """
    try:
        start_time = time.time()
        tile_id = tile.iloc[0].tile_ids
       
        log = setup_logger(
            logger_name='dem_indexing_',
            logger_path=f'../logs/dem/dem_indexing_{tile_id}_{datetime.datetime.now(pytz.timezone("Europe/Athens")).strftime("%Y%m%dT%H%M%S")}.log',
            logger_format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
        )
       
        log.info('Create directories and naming conversions')   
        
        NASROOT='//nas-rs.topo.auth.gr/Latomeia/DROUGHT'
        PRODUCT_NAME = 'copdem'
        FOLDER=f'{PRODUCT_NAME}/{tile_id.split('_')[0]}/{tile_id.split('_')[1]}'
        DATASET= f'COPDEM30_{tile_id.replace('_','')}'
        
        tile_id = tile.iloc[0].tile_ids.replace('_','')
        
        collection_path = f"{NASROOT}/{PRODUCT_NAME}"
        dataset_path = f"{NASROOT}/{FOLDER}"
        mkdir(dataset_path)
        eo3_path = f'{dataset_path}/{DATASET}.odc-metadata.yaml'
        stac_path = f'{dataset_path}/{DATASET}.stac-metadata.json'
        log.info(f'Dataset location: {dataset_path}')
            
        log.info('Connecting to Data Cube')
        dc = datacube.Datacube(app='demingestion', env='drought')
        log.info(dc.index.url)   
        
        log.info('Sanity check that product exist')
        if PRODUCT_NAME in dc.list_products()['name']:
            log.info('Sanity check: PASS')
        else:
            log.error(f'Sanity check: The product {PRODUCT_NAME} has not been indexed')
            sys.exit(1)

        log.info(f'Processing tile {tile_id}')
        dem_tile = dem.rio.clip(tile.to_crs('EPSG:3035').iloc[0])
        
        log.info('Assign time range and tile ID in metadata')
        dem_tile.attrs['dtr:start_datetime']='2021-04-22'
        dem_tile.attrs['dtr:end_datetime']='2021-04-22'
        dem_tile.attrs['odc:region_code']=tile_id
        
        
        log.info(f'Writing GTiff (COG) to disk')
        gc.collect()
        name_measurements = []
        relative_name_measurements = []
        for var in list(dem_tile.data_vars):
            file_path = f'{dataset_path}/{DATASET}_{var}.tif'
            
            dem_tile[var].rio.to_raster(
                raster_path=file_path, 
                driver='COG',
                dtype=str(dem_tile[var].dtype),
                windowed=True
                )
            name_measurements.append(file_path)
            relative_name_measurements.append(f'{DATASET}_{var}.tif')
            
            log.info(f'Write {var.upper()} -> {file_path}')
        
        log.info('Prepare metadata YAML document')
        yyyy = 2021
        mm = 4
        dd = 22
        datetime_list = [yyyy, mm, dd]    
        eo3_doc, stac_doc = prepare_eo3_metadata_NAS(
            dc=dc,
            xr_cube=dem_tile, 
            collection_path=Path(NASROOT),
            dataset_name=DATASET,
            product_name=PRODUCT_NAME,
            product_family='ard',
            name_measurements=relative_name_measurements,
            datetime_list=datetime_list,
            set_range=False,
            lineage_path=None,
            version=1,
            )
        
        log.info('Write metadata YAML document to disk')
        serialise.to_path(Path(eo3_path), eo3_doc)
        with open(stac_path, 'w') as json_file:
            json.dump(stac_doc, json_file, indent=4, default=False)
        
        log.info('Create datacube.model.Dataset from eo3 metadata')
        WORKING_ON_CLOUD=False
        uri = eo3_path if WORKING_ON_CLOUD else f"file:///{eo3_path}"

        resolver = Doc2Dataset(dc.index)
        dataset_tobe_indexed, err  = resolver(doc_in=serialise.to_doc(eo3_doc), uri=uri)
        
        if err:
            msg=f'✖✖✖ FAILED loading for : Tile {tile_id} | with Exception: {err}' # ✗
            log.error(msg)
            log.info('#######################################################################')
            raise RuntimeError(msg)
            
        log.info(f'Index DEM of {tile_id} into datacube')
        dc.index.datasets.add(dataset=dataset_tobe_indexed, with_lineage=False)
        
        log.info(f'')
        log.info(f'✔✔✔ COMPLETED: Tile {tile_id} | In {round((time.time() - start_time)/60, 2)} minutes')
        log.info(f'')            
            
    except Exception as indexing_error:
        log.error(f'✖✖✖ FAILED for : Tile {tile_id} | with Exception: {indexing_error}') # ✗
        log.info('#######################################################################')                  
