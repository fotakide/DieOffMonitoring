'''
######################################################################
## ARISTOTLE UNIVERSITY OF THESSALONIKI
## PERSLAB
## REMOTE SENSING AND EARTH OBSERVATION TEAM
##
## DATE:             Dec-2025
## SCRIPT:           run_dem_ingestion.py
## AUTHOR:           Vangelis Fotakidis (fotakidis@topo.auth.gr)
##
## DESCRIPTION:      Script to run CLI subprocesses of the `dem_ingestion.py` and `dem_indexing.py` modules  >>> (venv) python run_dem_ingestion.py
##
#######################################################################
'''

import dem_ingestion
import dem_indexing

from utils.utils import setup_logger, mkdir

import datetime, pytz
import gc, os, sys, time
import json

import geopandas as gpd

from pathlib import Path

import subprocess

if __name__ == "__main__":   
    # Set up logger.
    mkdir("../logs/dem")
    log = setup_logger(logger_name='admin_dem_',
                        logger_path=f'../logs/dem/admin_dem_{datetime.datetime.now(pytz.timezone("Europe/Athens")).strftime("%Y%m%dT%H%M%S")}.log', 
                        logger_format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
                        )
    
    # 1) load AOI
    tile_geojson_filepath='../anciliary/grid_20_v2.geojson'
    tiles = gpd.read_file(tile_geojson_filepath)
    requested_len = len(tiles)
        
    done_file = Path("../logs/dem/admin_completed_geojsons.txt")
    already_done = set()
    if done_file.exists():
        already_done = set(x for x in done_file.read_text().splitlines() if x)

    # 2) filter out completed tiles
    mask_done = tiles["tile_ids"].isin(already_done)
    done_tiles = tiles.loc[mask_done, "tile_ids"]

    n_done = len(done_tiles)
    for i, tile_id in enumerate(done_tiles, start=1):
        log.info(f"Skip already completed: {tile_id}")
        
    tiles = tiles.loc[~mask_done].reset_index(drop=True)
    
    if len(tiles)>0:
        # 3) Ingest DEM
        log.info(f"[>] Ingesting DEM covering total bounds of {requested_len} tiles")
        try:
            dem = dem_ingestion.copdem_ingestion(tiles=tiles)
            log.info('DEM loaded successfully')
        except:
            log.exception("Fatal error in dem_ingestion.py")
            sys.exit(1)
        
        # 4) run each in a fresh interpreter, sequentially
        log.info('Start the iteration of all tiles intersecting')
        for i, tile_geoseries in tiles.iterrows():
            tile_id = tile_geoseries['tile_ids']
            
            tile_gdf = tiles.iloc[[i]].copy()
            
            log.info(f"[>] Launching: {tile_id} [{i+1}/{len(tiles)}]")
            
            try:
                dem_indexing.dem_writing_indexing(
                    dem=dem,
                    tile=tile_gdf
                    )
                
                with done_file.open("a", encoding="utf-8") as df:
                    df.write(tile_id + "\n")
                log.info(f"✔ Processed {tile_id} | [{i+1} / {len(tiles)}] ({round(100*((i)/len(tiles)),2)}%)")
            except Exception as e:
                rc = 1
                log.error(f"✖ Failed {tile_id} with exit code {rc} | [{i+1} / {len(tiles)}] ({round(100*((i)/len(tiles)),2)}%)")
                time.sleep(2)
    else:
        log.info(f"All {len(tiles)} requested tiles are already processed.")
        pass
            
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