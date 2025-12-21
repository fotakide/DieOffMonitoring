'''
######################################################################
## ARISTOTLE UNIVERSITY OF THESSALONIKI
## PERSLAB
## REMOTE SENSING AND EARTH OBSERVATION TEAM
##
## DATE:             Dec-2025
## SCRIPT:           run_tcd_ingestion.py
## AUTHOR:           Vangelis Fotakidis (fotakidis@topo.auth.gr)
##
## DESCRIPTION:      Script to run Ingest Tree Canopy Density  >>> (venv) python run_tcd_ingestion.py
##
#######################################################################
'''

import datacube
from datacube.index.hl import Doc2Dataset
from eodatasets3 import serialise

import zipfile

import xarray as xr
import rioxarray as rxr
import numpy as np

import gc 
from pathlib import Path
import time

from utils.metadata import prepare_eo3_metadata_NAS
from utils.utils import mkdir, setup_logger

import argparse, json, sys, os, datetime, pytz
import subprocess

# Ignore warnings
import warnings
import logging
warnings.filterwarnings('ignore') 
logging.getLogger("distributed.worker.memory").setLevel(logging.ERROR)


def batch_tcd_ingestion():
    """_summary_
    """
    
    
if __name__ == "__main__":   
    # Set up logger.
    mkdir("../logs/tcd")
    log = setup_logger(logger_name='admin_tcd_',
                        logger_path=f'../logs/tcd/admin_tcd_{datetime.datetime.now(pytz.timezone("Europe/Athens")).strftime("%Y%m%dT%H%M%S")}.log', 
                        logger_format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
                        )
    # -------------------
    #       CONFIG
    # -------------------
    start_time = time.time()
    dc = datacube.Datacube(app='tcdingest', env='drought')
    
    SRC_DIR = Path("../anciliary/TCD_2023_10m/wekeo_zips")
    NASROOT = "//nas-rs.topo.auth.gr/Latomeia/DROUGHT"
    PRODUCT_NAME = "tcd2023"

    collection_path = f"{NASROOT}/{PRODUCT_NAME}"

    OUT_ROOT = Path(NASROOT) / PRODUCT_NAME
    # -------------------
    #       PROCESS
    # -------------------
    zip_files = sorted(SRC_DIR.glob("*.zip"))
    
    for zip_path in zip_files:
        try:
            DATASET = zip_path.stem
            dataset_path = f"{collection_path}/{DATASET}"
            dataset_path_obj = OUT_ROOT / DATASET
            mkdir(dataset_path)

            eo3_path = f'{dataset_path}/{DATASET}.odc-metadata.yaml'
            stac_path = f'{dataset_path}/{DATASET}.stac-metadata.json'

            expected_tif = dataset_path_obj / f"{DATASET}.tif"
            if expected_tif.exists():
                log.info(f"⏭ Skip (tif exists): {expected_tif.name}")
                tifs = [expected_tif]  # keep downstream code working
            else:
                log.info(f"\n→ Unzipping: {zip_path.name}")
                log.info(f"  Dataset  : {DATASET}")
                log.info(f"  Target   : {dataset_path}")

                with zipfile.ZipFile(zip_path, "r") as zf:
                    zf.extractall(dataset_path_obj)

                # Optional: sanity check for GeoTIFF
                tifs = list(dataset_path_obj.rglob("*.tif"))
                if not tifs:
                    log.info("  ⚠ WARNING: no .tif found")
                else:
                    log.info(f"  ✓ Found {len(tifs)} tif(s)")
                
                log.info('Edit metadata for indexing')
                xr_cube = rxr.open_rasterio(tifs[0]).squeeze()
                xr_cube.name = 'tcd'
                dtype = 'uint8'
                nodata = 255
                xr_cube = xr_cube.rio.write_nodata(nodata, inplace=True)
                xr_cube = xr_cube.to_dataset()
                xr_cube.attrs['odc:region_code']=DATASET.split('_')[5]

                log.info('Writing metadata files')
                datetime_list = [2023, 1, 1]    
                eo3_doc, stac_doc = prepare_eo3_metadata_NAS(
                    dc=dc,
                    xr_cube=xr_cube, 
                    collection_path=Path(NASROOT),
                    dataset_name=DATASET,
                    product_name=PRODUCT_NAME,
                    product_family='ard',
                    name_measurements=[tifs[0].name],
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
                    msg=f'✖✖✖ FAILED indexing for : {DATASET} | with Exception: {err}' # ✗
                    log.error(msg)
                    log.info('#######################################################################')
                    raise RuntimeError(msg)
                    
                log.info(f'Index TCD {DATASET} into datacube')
                log.info(dataset_tobe_indexed.uri)
                dc.index.datasets.add(dataset=dataset_tobe_indexed, with_lineage=False)
                
                log.info(f'')
                log.info(f'✔✔✔ COMPLETED: {DATASET} | In {round((time.time() - start_time)/60, 2)} minutes')
                log.info(f'')  
        except Exception as indexing_error:
            log.error(f'✖✖✖ FAILED for : {DATASET} | with Exception: {indexing_error}') # ✗
            log.info('#######################################################################')                  

    # ---------------- OWS UPDATE ----------------
    try:
        log.info("Triggering OWS update")

        # 1) run datacube-ows-update --views
        rc_views = subprocess.run(
            ["docker", "exec", "drought-drought_ows-1", "bash", "-lc", "datacube-ows-update --views"],
            check=False
        ).returncode

        if rc_views != 0:
            log.error(f"datacube-ows-update --views failed (rc={rc_views})")

        # 2) run datacube-ows-update
        rc_main = subprocess.run(
            ["docker", "exec", "drought-drought_ows-1", "bash", "-lc", "datacube-ows-update"],
            check=False
        ).returncode
        
        # 3) Restart OWS
        rc_restart = subprocess.run(
            ["docker", "restart", "drought-drought_ows-1"],
            check=False
        ).returncode
        
        if rc_main != 0:
            log.error(f"datacube-ows-update failed (rc={rc_main})")
        else:
            log.info("OWS update completed successfully.")

    except Exception as e:
        log.exception(f"Error while updating OWS: {e}")
    # ------------------------------------------------