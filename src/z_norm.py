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
## DESCRIPTION:      Script to Normalize NDVI, EVI, and PSRI2 time series to the Baseline period (2020-2022) and index into ODC
##
#######################################################################
'''

import datacube
from utils.utils import nas_patch

def baseline_metrics(monitor_start: str, monitor_end: str, tile_id: str):
    
    dc = datacube.Datacube(app='znorm', env='drought')
    
    for spectral_index in ["NDVI", "EVI", "PSRI2"]:
        
        ds_base = dc.load(
            product='baseline',
            tile_id=tile_id,
            measurements=[f'{spectral_index}_mean', f'{spectral_index}_std'],
            dask_chunks=dict(x=512, y=512),
            patch_url=nas_patch
        )

        ds = dc.load(
            product='composites',
            tile_id=tile_id,
            time=(monitor_start, monitor_end),
            measurements=spectral_index,
            dask_chunks=dict(x=512, y=512),
            patch_url=nas_patch
        )

        ds_znorm = ((ds - ds_base[f'{spectral_index}_mean']) / ds_base[f'{spectral_index}_std']).compute()