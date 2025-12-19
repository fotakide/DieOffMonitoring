'''
######################################################################
## ARISTOTLE UNIVERSITY OF THESSALONIKI
## PERSLAB
## REMOTE SENSING AND EARTH OBSERVATION TEAM
##
## DATE:             Aug-2025
## SCRIPT:           composites.py
## AUTHOR:           Vangelis Fotakidis (fotakidis@topo.auth.gr)
##
## DESCRIPTION:      Script to compute the Mean and Standard deviation of the Baseline period (2020-2022)
#                       from Median Sentinel-2 L2A NDVI, EVI, and PSRI2 Composites and index into ODC
##
#######################################################################
'''

import datacube
from datacube.index.hl import Doc2Dataset
from eodatasets3 import serialise

import numpy as np
import rioxarray as rxr
import xarray as xr

import time
from dask.distributed import LocalCluster, Client
import tempfile
from pathlib import Path

from utils.metadata import prepare_eo3_metadata_NAS
from utils.utils import mkdir, setup_logger
from utils.utils import nas_patch

# Ignore warnings
import warnings
import logging
import gc
warnings.filterwarnings('ignore') 
logging.getLogger("distributed.worker.memory").setLevel(logging.ERROR)


def baseline_metrics(baseline_start: str, baseline_end: str, tile_id: str):
    
    logging.info('#######################################################################')
    
    client = None
    cluster = None
    
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
    
    dc = datacube.Datacube(app='basecomp', env='drought')
    
    try:
        ds_vi_list = []
        for spectral_index in ["NDVI", "EVI", "PSRI2"]:
            start_time = time.time()
            logging.info(f'Computing baseline M and SD for {spectral_index}')
        
            logging.info('Create directories and naming conversions')   
            NASROOT='//nas-rs.topo.auth.gr/Latomeia/DROUGHT'
            PRODUCT_NAME = 'baseline'
            FOLDER=f'{PRODUCT_NAME}/{tile_id.split('_')[0]}/{tile_id.split('_')[1]}/{baseline_start.replace('-','')}_{baseline_end.replace('-','')}'
            DATASET= f'S2L2A_baseline_{tile_id.replace('_','')}_{baseline_start.replace('-','')}_{baseline_end.replace('-','')}'
            
            collection_path = f"{NASROOT}/{PRODUCT_NAME}"
            dataset_path = f"{NASROOT}/{FOLDER}"
            mkdir(dataset_path)
            eo3_path = f'{dataset_path}/{DATASET}.odc-metadata.yaml'
            stac_path = f'{dataset_path}/{DATASET}.stac-metadata.json'
            log.info(f'Dataset location: {dataset_path}')
            
            logging.info(f'Lazy loading time series')
            ds = dc.load(
                product='composites',
                region_code=tile_id.replace('_',''),
                time=(baseline_start, baseline_end),
                measurements=[spectral_index],
                dask_chunks=dict(x=512, y=512),
                patch_url=nas_patch
            ).compute()
            
            logging.info(f'Lazy applying nodata mask')
            ds[spectral_index] = (
                ds[spectral_index]
                .where(ds[spectral_index] != ds[spectral_index].attrs.get('nodata', -9999))
                .astype('float16')
            )
            
            logging.info(f'Computing Mean (M)')
            base_vi_mean = ds[spectral_index].mean(dim="time", skipna=True).astype('float16')
            logging.info(f'Computing Standard Deviation (SD)')
            base_vi_std = ds[spectral_index].std(dim="time", skipna=True).astype('float16')
            
            logging.info(f'Assigning names to xr.DataArrays')
            base_vi_mean.name = f'{spectral_index}_mean'
            base_vi_std.name = f'{spectral_index}_std'
            
            logging.info(f'Merge into xr.Dataset')
            base_vi_ds = xr.merge([base_vi_mean.to_dataset(), base_vi_std.to_dataset()])
            
            logging.info(f'Scale and nodata -32768')
            logging.info(f'Configure metadata of baseline dataset')
            for var in [f'{spectral_index}_mean', f'{spectral_index}_std']:
                scale = 1
                dtype = 'int16'
                nodata = np.iinfo(np.int16).min #-32768
                base_vi_ds[var] = (base_vi_ds[var]*scale).round()
                base_vi_ds[var] = base_vi_ds[var].fillna(nodata).astype(dtype)
                base_vi_ds[var] = base_vi_ds[var].rio.write_nodata(nodata, inplace=True)
                base_vi_ds[var].encoding.update({"dtype": dtype})
    
            logging.info(f'Append baseline dataset of {spectral_index} in list')
            ds_vi_list.append(base_vi_ds[f'{spectral_index}_mean'])
            ds_vi_list.append(base_vi_ds[f'{spectral_index}_std'])
        
        logging.info('Done with computing baseline of all spectral indices')
        logging.info('Now creating the overall baseline dataset')
        base_ds = xr.merge(ds_vi_list)
        logging.info('Overall baseline dataset constructed')
        
        logging.info('Assign time range and tile ID in metadata')
        base_ds.attrs['dtr:start_datetime']=baseline_start
        base_ds.attrs['dtr:end_datetime']=baseline_end
        base_ds.attrs['odc:region_code']=tile_id
        
        logging.info(f'Writing GTiff (COG) to disk')
        gc.collect()
        name_measurements = []
        relative_name_measurements = []
        for var in list(base_ds.data_vars):
            file_path = f'{dataset_path}/{DATASET}_{var}.tif'
            
            base_ds[var].rio.to_raster(
                raster_path=file_path, 
                driver='COG',
                dtype=str(base_ds[var].dtype),
                windowed=True
                )
            name_measurements.append(file_path)
            relative_name_measurements.append(f'{DATASET}_{var}.tif')
            
            logging.info(f'Write {var.upper()} -> {file_path}')
        
        
        logging.info('Prepare metadata YAML document')
        yyyy = int(baseline_start[0:4])
        mm = int(baseline_start[5:7])
        dd = int(baseline_start[8:10])
        datetime_list = [yyyy, mm, dd]    
        eo3_doc, stac_doc = prepare_eo3_metadata_NAS(
            dc=dc,
            xr_cube=base_ds, 
            collection_path=Path(NASROOT),
            dataset_name=DATASET,
            product_name=PRODUCT_NAME,
            product_family='ard',
            name_measurements=relative_name_measurements,
            datetime_list=datetime_list,
            set_range=True,
            lineage_path=None,
            version=1,
            )
        
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
            msg=f'✖✖✖ FAILED loading for : Tile {tile_id} | with Exception: {err}' # ✗
            logging.error(msg)
            logging.info('#######################################################################')
            raise RuntimeError(msg)
            
        logging.info('Index to datacube')
        dc.index.datasets.add(dataset=dataset_tobe_indexed, with_lineage=False)
        
        logging.info(f'')
        logging.info(f'✔✔✔ COMPLETED: Tile {tile_id} | In {round((time.time() - start_time)/60, 2)} minutes')
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
    import argparse, json, sys, os, datetime, pytz
    from utils.utils import setup_logger
    import logging

    p = argparse.ArgumentParser(description="Run ONE composite from a single .geojson and exit.")
    p.add_argument("--tile", required=True, help="Tile ID")
    args = p.parse_args()

    try:
        tile_id    = args.tile
        
        log = setup_logger(
            logger_name='baseline_',
            logger_path=f'../logs/baseline/baseline_{tile_id}_{datetime.datetime.now(pytz.timezone("Europe/Athens")).strftime("%Y%m%dT%H%M%S")}.log',
            logger_format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
        )

        baseline_start = "2020-01-01"
        baseline_end = "2023-03-31"
        
        baseline_metrics(
            baseline_start=baseline_start,
            baseline_end=baseline_end,
            tile_id=tile_id
            )
        sys.exit(0)         # success (including "skipped" is still success)
    except Exception:
        logging.exception("Fatal error in composites.py")
        sys.exit(1)        # fail