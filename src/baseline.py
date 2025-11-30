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
from utils.utils import nas_patch

def baseline_metrics(baseline_start: str, baseline_end: str, tile_id: str):
    
    dc = datacube.Datacube(app='basecomp', env='drought')
    
    for spectral_index in ["NDVI", "EVI", "PSRI2"]:
        
        ds = dc.load(
            product='composites',
            region_code=tile_id,
            time=(baseline_start, baseline_end),
            measurements=spectral_index,
            dask_chunks=dict(x=512, y=512),
            patch_url=nas_patch
        )
        
        base_mean = ds.mean(dim="time", skipna=True).compute()
        base_std = ds.std(dim="time", skipna=True).compute()
        
        base_mean.name = f'{spectral_index}_mean'
        base_std.name = f'{spectral_index}_std'