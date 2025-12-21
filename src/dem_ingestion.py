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

# Ignore warnings
import warnings
import logging
warnings.filterwarnings('ignore') 
logging.getLogger("distributed.worker.memory").setLevel(logging.ERROR)


def copdem_ingestion(tiles: gpd.GeoDataFrame):
    """Using the total bounds of a gpd.GeoDataFrame, buffered by 0.01 deg, 
    retrieve COPDEM-30 from Microsoft Planetary Computer (https://planetarycomputer.microsoft.com/dataset/cop-dem-glo-30).
    By resampling and reprojecting to match the GeoBox of AOI defined by tiles

    Args:
        tiles (gpd.GeoDataFrame): The tiles defining the study area.
    """
    try:
        start_time = time.time()
        client = None
        cluster = None
        
        logging.info('#######################################################################')
        
        logging.info('Processing started')
        
        logging.info('Initializing Dask cluster')
        cluster = LocalCluster(
            n_workers=8, 
            threads_per_worker=1, 
            processes=True,
            memory_limit='auto', 
            local_directory=tempfile.mkdtemp(),
            dashboard_address=":8787",
            # silence_logs=logging.WARN,
            )
        client = Client(cluster)
        logging.info(f'Dask dashboard is available at: {client.dashboard_link}')

        
        logging.info('Extracting BoungBox and buffering it by 0.01 degrees')
        bbox = BoundingBox(
            left=tiles.geometry.total_bounds[0],
            bottom=tiles.geometry.total_bounds[1],
            right=tiles.geometry.total_bounds[2],
            top=tiles.geometry.total_bounds[3],
            crs='EPSG:4326'
            ).buffered(xbuff=0.01, ybuff=0.01)
        
        logging.info('Constructing the GeoBox')
        geobox_total = GeoBox.from_bbox(
            bbox.to_crs(crs='EPSG:3035'), 
            crs='EPSG:3035', 
            resolution=20)
        
        logging.info('Connecting to MPC')
        mpc_catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=sign_inplace,
        )

        logging.info('Searching for COP-DEM-GLO-30meter in MPC')
        items_dem = mpc_catalog.search(
            collections=["cop-dem-glo-30"], 
            bbox=bbox).item_collection()
        dem_band="data"
        
        logging.info('Loading with odc.stac ...')
        dem = odc.stac.load(
            items_dem,
            bands=[dem_band],
            like=geobox_total,
            chunks=dict(time=1, y=2048, x=2048),
            resampling=Resampling.cubic.name
        ).compute().isel(time=0)
        logging.info('Loading with odc.stac ... Completed')
        
        logging.info('Renaming elevation variable')
        dem = dem.rename({"data": "elevation"})
        
        logging.info('Computing the aspect ...')
        dem['aspect'] = aspect(dem.elevation)
        dem['aspect'] = dem.aspect.where(dem.aspect>0)
        logging.info('Computing the aspect ... Completed')
        
        logging.info(f'Configure metadata of each variable')
        for var in ['elevation', 'aspect']:
            dtype = 'float32'
            nodata = np.nan
            dem[var] = dem[var].astype(dtype)
            dem[var] = dem[var].rio.write_nodata(nodata, inplace=True)
            dem[var].encoding.update({"dtype": dtype})
        
        logging.info(f'')
        logging.info(f'✔✔✔ COMPLETED loading COP-DEM-GLO-30 | In {round((time.time() - start_time)/60, 2)} minutes')
        logging.info(f'')
        
        return dem
    
    except Exception as exc:
        msg=f'✖✖✖ FAILED loading COP-DEM-GLO-30 | with Exception: {exc}' # ✗
        logging.error(msg)
        raise
    finally:
        try:
            if client is not None:
                logging.info('Closing Dask client')
                client.close()
        finally:
            if cluster is not None:
                logging.info('Closing Dask cluster')
                logging.info('#######################################################################')
                cluster.close()
