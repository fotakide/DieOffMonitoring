from utils.utils import setup_logger, mkdir, generate_geojson_files_for_composites

import datetime, pytz
import gc, os, sys, time
import json

import geopandas as gpd

from pathlib import Path

import subprocess

if __name__ == "__main__":   
    # ---------------- OWS UPDATE ----------------
    try:
        print("Triggering OWS update")

        # 1) run datacube-ows-update --views
        rc_views = subprocess.run(
            ["docker", "exec", "drought-drought_ows-1", "bash", "-lc", "datacube-ows-update --views"],
            check=False
        ).returncode

        if rc_views != 0:
            print(f"datacube-ows-update --views failed (rc={rc_views})")

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
            print(f"datacube-ows-update failed (rc={rc_main})")
        else:
            print("OWS update completed successfully.")

    except Exception as e:
        print(f"Error while updating OWS: {e}")
    # ------------------------------------------------