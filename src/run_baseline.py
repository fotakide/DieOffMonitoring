'''
######################################################################
## ARISTOTLE UNIVERSITY OF THESSALONIKI
## PERSLAB
## REMOTE SENSING AND EARTH OBSERVATION TEAM
##
## DATE:             Dec-2025
## SCRIPT:           run_baseline.py
## AUTHOR:           Vangelis Fotakidis (fotakidis@topo.auth.gr)
##
## DESCRIPTION:      Script to run CLI subprocesses of the `baseline.py` module  >>> (venv) python run_baseline.py
##
#######################################################################
'''

from utils.utils import setup_logger, mkdir

import datetime, pytz
import gc, os, sys, time
import json

import geopandas as gpd

from pathlib import Path

import subprocess

if __name__ == "__main__":   
    # Set up logger.
    mkdir("../logs/baseline")
    log = setup_logger(logger_name='admin_baseline_',
                        logger_path
                        
                        =f'../logs/baseline/admin_baseline_{datetime.datetime.now(pytz.timezone("Europe/Athens")).strftime("%Y%m%dT%H%M%S")}.log', 
                        logger_format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
                        )
    
    # 1) generate/refresh the .geojson tasks
    tile_geojson_filepath='../anciliary/grid_20_v2.geojson'
    
    # 2) collect tasks
    tiles = gpd.read_file(tile_geojson_filepath)
        
    done_file = Path("../logs/baseline/admin_completed_geojsons.txt")
    already_done = set()
    if done_file.exists():
        already_done = set(x for x in done_file.read_text().splitlines() if x)

    # 3) run each in a fresh interpreter, sequentially
    for i, row in tiles.iterrows():
        tile_id = row['tile_ids']
        geometry = row['geometry']
        
        if tile_id in already_done:
            log.info(f"Skip already completed: {tile_id} [{i}/{len(tiles)}]")
            continue

        log.info(f"[>] Launching single-shot: {tile_id} [{i}/{len(tiles)}]")
        
        rc = subprocess.run(
            [sys.executable, "baseline.py", "--tile", tile_id],
            check=False,
        ).returncode

        if rc == 0:
            with done_file.open("a", encoding="utf-8") as df:
                df.write(tile_id + "\n")
            log.info(f"✔ Processed {tile_id} | [{i} / {len(tiles)}] ({round(100*((i)/len(tiles)),2)}%)")
        else:
            log.error(f"✖ Failed {tile_id} with exit code {rc} | [{i} / {len(tiles)}] ({round(100*((i)/len(tiles)),2)}%)")
            # optional small backoff to avoid rapid-fire restarts on a flaky machine
            time.sleep(2)
