'''
######################################################################
## ARISTOTLE UNIVERSITY OF THESSALONIKI
## PERSLAB
## REMOTE SENSING AND EARTH OBSERVATION TEAM
##
## DATE:             Dec-2025
## SCRIPT:           z_normalization.py
## AUTHOR:           Vangelis Fotakidis (fotakidis@topo.auth.gr)
##
## DESCRIPTION:      Script to Z-Normalize NDVI, EVI, PSRI2 monthly median composite and index into ODC
##
#######################################################################
'''

import datacube
from datacube.index.hl import Doc2Dataset
from eodatasets3 import serialise

import numpy as np
import xarray as xr
import rioxarray as rxr

import datetime
import calendar

import time
from pathlib import Path

from utils.metadata import prepare_eo3_metadata_NAS, reorder_measurements
from utils.utils import mkdir, setup_logger
from utils.utils import nas_patch

import warnings
import logging
import gc
warnings.filterwarnings('ignore') 
logging.getLogger("distributed.worker.memory").setLevel(logging.ERROR)


def z_normalization(year_month: str, tile_id: str):
    logging.info('#######################################################################')
    
    client = None
    cluster = None
    
    dt = datetime.datetime.strptime(year_month, "%Y-%m")
    start_date = dt.replace(day=1).strftime("%Y-%m-%d")
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    end_date = dt.replace(day=last_day).strftime("%Y-%m-%d")
    datetime_list = [int(start_date[:4]), int(start_date[5:7]), int(start_date[8:10])] 
    
    dc = datacube.Datacube(app='znorm', env='drought')
    
    start_time = time.time()
    logging.info(f'Starting Z-Normalization for {tile_id} | {year_month}')

    logging.info('Create directories and naming conversions')   
    NASROOT='//nas-rs.topo.auth.gr/Latomeia/DROUGHT'
    PRODUCT_NAME = 'z_normalized'
    FOLDER=f'{PRODUCT_NAME}/{tile_id.split('_')[0]}/{tile_id.split('_')[1]}/{datetime_list[0]}/{datetime_list[1]}/01'
    DATASET= f'S2L2A_znorm_{tile_id.replace('_','')}_{datetime_list[0]}{datetime_list[1]}'
    
    collection_path = f"{NASROOT}/{PRODUCT_NAME}"
    dataset_path = f"{NASROOT}/{FOLDER}"
    mkdir(dataset_path)
    eo3_path = f'{dataset_path}/{DATASET}.odc-metadata.yaml'
    stac_path = f'{dataset_path}/{DATASET}.stac-metadata.json'
    log.info(f'Dataset location: {dataset_path}')
    
    try:
        z_norm_vi_list = []
        for spectral_index in ["NDVI", "EVI", "PSRI2"]:
            logging.info(f'Z-Normalization | {spectral_index}')
            
            ds_base = dc.load(
                product='baseline',
                region_code=tile_id.replace('_',''),
                measurements=[f'{spectral_index}_mean', f'{spectral_index}_std'],
                # dask_chunks=dict(x=512, y=512),
                patch_url=nas_patch
            ).squeeze()
            logging.info(f'Loaded baseline')
            
            for var in list(ds_base.data_vars):
                logging.info(f'Set up values | {var}')
                ds_base[var] = (
                    ds_base[var]
                    .where(ds_base[var] != ds_base[var].attrs.get('nodata', -32768))
                    .astype('float16')
                )

            ds_comp = dc.load(
                product='composites',
                region_code=tile_id.replace('_',''),
                time=year_month,
                measurements=spectral_index,
                # dask_chunks=dict(x=512, y=512),
                patch_url=nas_patch
            )
            logging.info(f'Loaded composite')
            
            logging.info(f'Set up values | {spectral_index}')
            ds_comp[spectral_index] = (
                ds_comp[spectral_index]
                .where(ds_comp[spectral_index] != ds_comp[spectral_index].attrs.get('nodata', -32768))
                .astype('float16')
            )
            
            logging.info(f'Setting up normalization ... ')
            x = ds_comp[spectral_index]
            mu = ds_base[f'{spectral_index}_mean']
            sigma = ds_base[f'{spectral_index}_std']
            logging.info(f'Setting up normalization ... done')

            logging.info(f'Normalizing ... ')
            ds_znorm_vi = ((x - mu) / sigma).astype('float32')
            del x, mu, sigma
            ds_znorm_vi.name = f"{spectral_index}_z"
            ds_znorm_vi = ds_znorm_vi.to_dataset()
            logging.info(f'Normalizing ... done')
            
            z_norm_vi_list.append(ds_znorm_vi)
            del ds_znorm_vi
            gc.collect()
        
        logging.info(f'Merging into one dataset')
        ds_znorm = xr.merge(z_norm_vi_list)
        if len(ds_znorm.dims):
            ds_znorm = ds_znorm.squeeze()
        
        
        logging.info('Scale and Define data types & nodata per band')
        for var in list(ds_znorm.data_vars):
            scale = 1 
            dtype = 'float32'
            nodata = np.nan #-32768
            # ds_znorm[var] = (ds_znorm[var]*scale).round()
            # ds_znorm[var] = ds_znorm[var].fillna(nodata).astype(dtype)
            ds_znorm[var] = ds_znorm[var].rio.write_nodata(nodata, inplace=True) # _FillValue 
            ds_znorm[var].encoding.update({"dtype": dtype})
            # composite[si].encoding["scale_factor"] = 1/scale


        logging.info('Assign time range and tile ID in metadata')
        ds_znorm.attrs['dtr:start_datetime']=start_date
        ds_znorm.attrs['dtr:end_datetime']=end_date
        ds_znorm.attrs['odc:region_code']=tile_id
        
        
        logging.info('Write bands to raster COG files')
        name_measurements = []
        for var in list(ds_znorm.data_vars):
            file_path = f'{dataset_path}/{DATASET}_{var}.tif'
            
            ds_znorm[var].rio.to_raster(
                raster_path=file_path, 
                driver='COG',
                dtype=str(ds_znorm[var].dtype),
                windowed=True
                )
            name_measurements.append(file_path)
            
            logging.info(f'Write {var.upper()} -> {file_path}')
            
        logging.info(f'Assert relative paths and product measurements are matched')
        relative_name_measurements = reorder_measurements(
            product=PRODUCT_NAME, 
            relative_name_measurements=relative_name_measurements)
        
        logging.info('Prepare metadata YAML document')
        relative_name_measurements = [p.split("/")[-1] for p in name_measurements]
        eo3_doc, stac_doc = prepare_eo3_metadata_NAS(
            dc=dc,
            xr_cube=ds_znorm, 
            collection_path=Path(NASROOT),
            dataset_name=DATASET,
            product_name=PRODUCT_NAME,
            product_family='ard',
            bands=list(ds_znorm.data_vars),
            name_measurements=relative_name_measurements,
            datetime_list=datetime_list,
            set_range=False,
            lineage_path=None,
            version=1,
            )
        
        del ds_znorm
        gc.collect()
        
        
        logging.info('Write metadata YAML document to disk')
        serialise.to_path(Path(eo3_path), eo3_doc)
        with open(stac_path, 'w') as json_file:
            json.dump(stac_doc, json_file, indent=4, default=False)
        
        logging.info('Create datacube.model.Dataset from eo3 metadata')
        WORKING_ON_CLOUD=False
        uri = eo3_path if WORKING_ON_CLOUD else f"file:///{eo3_path}"

        resolver = Doc2Dataset(dc.index)
        dataset_tobe_indexed, err  = resolver(doc_in=serialise.to_doc(eo3_doc), uri=uri)
        
        if err:
            msg=f'             ✖✖✖ FAILED : Tile {tile_id} | Time: {year_month} | with Exception: {err}' # ✗
            logging.error(msg)
            logging.info('#######################################################################')
            raise RuntimeError(msg)
            
        logging.info('Index to datacube')
        dc.index.datasets.add(dataset=dataset_tobe_indexed, with_lineage=False)
        
        logging.info(f'')
        logging.info(f'             ✔✔✔ COMPLETED: Tile {tile_id} | Time: {year_month} | In {round((time.time() - start_time)/60, 2)} minutes')
        logging.info(f'')
    except Exception as VI_error:
        logging.error(f'✖✖✖ FAILED for : Tile {tile_id} | with Exception: {VI_error}') # ✗
        logging.info('#######################################################################')
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
 

if __name__ == "__main__":
    import argparse, json, sys, os, pytz
    import datetime
    from utils.utils import setup_logger

    p = argparse.ArgumentParser(description="Run ONE z-normalization from a single .geojson and exit.")
    p.add_argument("--geojson", required=True, help="Path to a single GeoJSON file")
    args = p.parse_args()

    try:
        with open(args.geojson, "r", encoding="utf-8") as f:
            d = json.load(f)
        year_month = d["properties"]["year_month"]
        tile_id    = d["properties"]["tile_id"]
        tile_geom  = d["geometry"]
        
        log = setup_logger(
            logger_name='znorm_',
            logger_path=f'../logs/znorm/znorm_{year_month}_{tile_id}_{datetime.datetime.now(pytz.timezone("Europe/Athens")).strftime("%Y%m%dT%H%M%S")}.log',
            logger_format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
        )

        z_normalization(year_month=year_month, tile_id=tile_id)
        sys.exit(0)         # success (including "skipped" is still success)
    except Exception:
        import logging
        logging.exception("Fatal error in composites.py")
        sys.exit(1)        # fail