# pylint: skip-file

bands_s2l2a_compos = {
    "B02": ["blue"],
    "B03": ["green"],
    "B04": ["red"],
    "B05": ["rededge1"],
    "B07": ["rededge3"],
    "B8A": ["nir8A"],
    "NDVI": ["ndvi"],
    "EVI": ["evi"],
    "PSRI2": ["psri2"],
}

style_rgb = {
    "name": "simple_rgb",
    "title": "Simple RGB",
    "abstract": "Simple true-colour image, using the red, green and blue bands",
    # The component keys MUST be "red", "green" and "blue" (and optionally "alpha")
    "components": {
        "red": {
            "B04": 1.0
        },
        "green": {
            "B03": 1.0
        },
        "blue": {
            "B02": 1.0
        }
    },
    "scale_range": [200.0, 3000.0],
    "legend": {
        "show_legend": True,
    }
}

style_false = {
    "name": "simple_false_rgb",
    "title": "False colour",
    "abstract": "False-colour image, using the B8A, B4, and B3 bands",
    # The component keys MUST be "red", "green" and "blue" (and optionally "alpha")
    "components": {
        "red": {
            "B8A": 1.0
        },
        "green": {
            "B04": 1.0
        },
        "blue": {
            "B03": 1.0
        }
    },
    "scale_range": [200.0, 3000.0],
    "legend": {
        "show_legend": True,
    }
}

# style_swir = {
#     "name": "style_swir",
#     "title": "SWIR",
#     "abstract": "False-colour image, using the B12, B8A, and B4 bands",
#     # The component keys MUST be "red", "green" and "blue" (and optionally "alpha")
#     "components": {
#         "red": {
#             "B12": 1.0
#         },
#         "green": {
#             "B8A": 1.0
#         },
#         "blue": {
#             "B04": 1.0
#         }
#     },
#     "scale_range": [200.0, 3000.0],
#     "legend": {
#         "show_legend": True,
#     }
# }

style_ndvi = {
    "name": "ndvi",
    "title": "NDVI",
    "abstract": "Normalised Difference Vegetation Index - a derived index that correlates well with the existence of vegetation",
    "needed_bands": ["NDVI"],
    "index_function": {
        "function": "datacube_ows.band_utils.single_band",
        "mapped_bands": True,
        "kwargs": {
            "band": "ndvi",
        }
    },
    "color_ramp": [
        {
            "value": -0.0,
            "color": "#8F3F20",
            "alpha": 0.0
        },
        {
            "value": 0.0,
            "color": "#8F3F20",
            "alpha": 1.0
        },
        {
            "value": 100,
            "color": "#A35F18"
        },
        {
            "value": 200,
            "color": "#B88512"
        },
        {
            "value": 300,
            "color": "#CEAC0E"
        },
        {
            "value": 400,
            "color": "#E5D609"
        },
        {
            "value": 500,
            "color": "#FFFF0C"
        },
        {
            "value": 600,
            "color": "#C3DE09"
        },
        {
            "value": 700,
            "color": "#88B808"
        },
        {
            "value": 800,
            "color": "#529400"
        },
        {
            "value": 900,
            "color": "#237100"
        },
        {
            "value": 1000,
            "color": "#114D04"
        }
    ],
    # "include_in_feature_info": True,
    # "legend": {
    #     "show_legend": True,
    # }
}

bands_dem = {
    "elevation": ["z"],
    "aspect": ["a"],
}

style_elevation = {
    "name": "elevation",
    "title": "Elevation",
    "abstract": "DEM elevation (m) displayed as a continuous colour ramp.",
    # Use the single band directly as the index value
    "index_expression": "elevation",
    "needed_bands": ["elevation"],

    "range": [1.0, 3000.0],

    "mpl_ramp": "terrain",

    "legend": {
        "title": "Elevation (m)",
        "begin": "1",
        "end": "3000",
        "ticks": ["1", "250", "500", "1000", "1500", "2000", "2500", "3000"],
    },
}

EPS = 1e-6  # prevents interpolation across class boundaries

style_aspect = {
    "name": "aspect",
    "title": "Aspect (classes)",
    "abstract": "Aspect (degrees) grouped into 8 directional classes.",
    "needed_bands": ["aspect"],
    "index_expression": "aspect",
    "range": [0.0, 360.0],

    # Piecewise-constant bins encoded as a flat colour ramp
    "color_ramp": [
        # North: [0, 22.5) and [337.5, 360]
        {"value": 0.0,          "color": "#1f77b4"},
        {"value": 22.5 - EPS,   "color": "#1f77b4"},

        # Northeast: [22.5, 67.5)
        {"value": 22.5,         "color": "#17becf"},
        {"value": 67.5 - EPS,   "color": "#17becf"},

        # East: [67.5, 112.5)
        {"value": 67.5,         "color": "#2ca02c"},
        {"value": 112.5 - EPS,  "color": "#2ca02c"},

        # Southeast: [112.5, 157.5)
        {"value": 112.5,        "color": "#bcbd22"},
        {"value": 157.5 - EPS,  "color": "#bcbd22"},

        # South: [157.5, 202.5)
        {"value": 157.5,        "color": "#ff7f0e"},
        {"value": 202.5 - EPS,  "color": "#ff7f0e"},

        # Southwest: [202.5, 247.5)
        {"value": 202.5,        "color": "#d62728"},
        {"value": 247.5 - EPS,  "color": "#d62728"},

        # West: [247.5, 292.5)
        {"value": 247.5,        "color": "#9467bd"},
        {"value": 292.5 - EPS,  "color": "#9467bd"},

        # Northwest: [292.5, 337.5)
        {"value": 292.5,        "color": "#8c564b"},
        {"value": 337.5 - EPS,  "color": "#8c564b"},

        # North wrap: [337.5, 360]
        {"value": 337.5,        "color": "#1f77b4"},
        {"value": 360.0,        "color": "#1f77b4"},
    ],

    # Legend: OWS will still draw a strip (because it's a ramp),
    # but we can label ticks at class midpoints.
    "legend": {
        "title": "Aspect",
        "ticks": ["0", "45", "90", "135", "180", "225", "270", "315"],
        "tick_labels": {
            "0":   {"label": "N"},
            "45":  {"label": "NE"},
            "90":  {"label": "E"},
            "135": {"label": "SE"},
            "180": {"label": "S"},
            "225": {"label": "SW"},
            "270": {"label": "W"},
            "315": {"label": "NW"},
        },
    },
}

bands_tcd2023 = {
    "tcd": ["tcd"],
}

style_tcd = {
    "name": "tcd2023",
    "title": "Tree Canopy Density (2023)",
    "abstract": "",
    # Use the single band directly as the index value
    "index_expression": "tcd",
    "needed_bands": ["tcd"],
    "index_function": {
        "function": "datacube_ows.band_utils.single_band",
        "mapped_bands": True,
        "kwargs": {
            "band": "tcd",
        }
    },
    "color_ramp": [
        {
            "value": 0.0,
            "color": "#f0f0f0",
            "alpha": 1.0
        },        
        {
            "value": 1.0,
            "color": "#fdff73",
            "alpha": 1.0
        },
        {
            "value": 100,
            "color": "#1c5c24"
        },
        {
            "value": 255.0,
            "color": "#000000",
            "alpha": 0.0
        },
    ],
    "range": [0.0, 100.0],
    "legend": {
        "title": "Density (%)",
        "begin": "0",
        "end": "100",
        "ticks": ["0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"],
    },
}

bands_znorm = {
    "NDVI_z": ["ndvi_z"],
    "EVI_z": ["evi_z"],
    "PSRI2_z": ["psri2_z"]
}

style_ndvi_znorm = {
    "name": "style_ndvi_znorm",
    "title": "Z-normalized NDVI",
    "abstract": "",
    "index_function": {
        "function": "datacube_ows.band_utils.single_band",
        "mapped_bands": True,
        "kwargs": {
            "band": "NDVI_z",
        }
    },
    "needed_bands": ["NDVI_z"],
    "mpl_ramp": "RdBu",
    "range": [-3.0, 3.0],
    "include_in_feature_info": True,
}

style_evi_znorm = {
    "name": "style_evi_znorm",
    "title": "Z-normalized EVI",
    "abstract": "",
    "index_function": {
        "function": "datacube_ows.band_utils.single_band",
        "mapped_bands": True,
        "kwargs": {
            "band": "EVI_z",
        }
    },
    "needed_bands": ["EVI_z"],
    "mpl_ramp": "RdBu",
    "range": [-3.0, 3.0],
    "include_in_feature_info": True,
}

style_psri2_znorm = {
    "name": "style_psri2_znorm",
    "title": "Z-normalized PSRI2",
    "abstract": "",
    "index_function": {
        "function": "datacube_ows.band_utils.single_band",
        "mapped_bands": True,
        "kwargs": {
            "band": "PSRI2_z",
        }
    },
    "needed_bands": ["PSRI2_z"],
    "mpl_ramp": "RdBu_r",
    "range": [-3.0, 3.0],
    "include_in_feature_info": True,
}


#############
# RESOURCES #
#############
standard_resource_limits = {
    "wms": {
        "zoomed_out_fill_colour": [150, 180, 200, 160],
        "min_zoom_factor": 500.0,
        "max_datasets": 10,
        "dataset_cache_rules": [
            {
                "min_datasets": 4, # Must be greater than zero.  Blank tiles (0 datasets) are NEVER cached
                # The cache-control max-age for this rule, in seconds.
                "max_age": 86400,  # 86400 seconds = 24 hours
            },
            {
                # Rules must be sorted in ascending order of min_datasets values.
                "min_datasets": 8,
                "max_age": 604800,  # 604800 seconds = 1 week
            },
        ]
    },
    "wcs": {
        "max_datasets": 16,
    }
}


# MAIN CONFIGURATION OBJECT

ows_cfg = {
    "global": {
        "response_headers": {
            "Access-Control-Allow-Origin": "*",  # CORS header (strongly recommended)
        },
        "services": {
            "wms": True,
            "wmts": True,
            "wcs": True
        },
        "title": "Open web-services for ODC in Drought Monitoring in Greece",
        "allowed_urls": ["http://localhost:9000",
                         "http://localhost:9000/wms",
                        #  "https://emt-datacube-ows.ngrok.app/wms",
                        #  "https://emt-datacube-ows.ngrok.app"
                          ],
        "info_url": "https://github.com/fotakide",
        "abstract": """This research project analyzes drought impacts on Greek mountain ecosystems, focusing on fir forest (Abies cephalonica) die-off in regions such as Chelmos, Mainalo, Taygetos, Parnonas, and Epirus. The work includes environmental monitoring, GIS mapping, climate data analysis, and the development of strategies for resilience and restoration.""",
        "keywords": [
            "ard",
        ],
        "contact_info": {
            "person": "Vangelis Fotakidis",
            "organisation": "AUTH",
            "position": "PhD Candidate",
            "address": {
                "type": "University",
                "address": "Aristotle University Campus",
                "city": "Thessaloniki",
                "country": "Greece",
            },
            "telephone": "+30 2310",
            "email": "fotakidis@topo.auth.gr",
        },
        "attribution": {
            "title": "Acme Satellites",
            "url": "http://www.acme.com/satellites",
            "logo": {
                "width": 370,
                "height": 247,
                "url": "https://www.auth.gr/wp-content/uploads/banner-horizontal-default-en.png",
                "format": "image/png",
            }
        },
        "fees": "",
        "access_constraints": "",
        "published_CRSs": {
            "EPSG:3035": {  # ETRS89-extended / LAEA Europe
                "geographic": False,
                "horizontal_coord": "x",
                "vertical_coord": "y",
            },
            "EPSG:3857": {  # Web Mercator
                "geographic": False,
                "horizontal_coord": "x",
                "vertical_coord": "y",
            },
            "EPSG:4326": {  # WGS-84
                "geographic": True,
                "vertical_coord_first": True
            },
        }
    },   #### End of "global" section.

    
    "wms": {
        "s3_url": "http://data.au",
        "s3_bucket": "s3_bucket_name",
        "s3_aws_zone": "ap-southeast-2",
        "max_width": 512,
        "max_height": 512,

        "authorities": {
            "auth": "https://authoritative-authority.com",
            "idsrus": "https://www.identifiers-r-us.com",
        }
    }, ####  End of "wms" section.


    "wmts": {
        "tile_matrix_sets": {
            "eeagrid": {
                # The CRS of the Tile Matrix Set
                "crs": "EPSG:3035",
                # The coordinates (in the CRS above) of the upper-left
                # corner of the tile matrix set.
                # My edit: Changed from https://epsg.io/map#srs=32634&x=502338.404794&y=3876270.085061&z=8&layer=streets
                "matrix_origin": (5000000.0, 2300000.0),
                # "matrix_origin": (4321000.0, 3210000.0),
                "tile_size": (256, 256),
                "scale_set": [
                    3779769.4643008336,
                    1889884.7321504168,
                    944942.3660752084,
                    472471.1830376042,
                    236235.5915188021,
                    94494.23660752083,
                    47247.11830376041,
                    23623.559151880207,
                    9449.423660752083,
                    4724.711830376042,
                    2362.355915188021,
                    1181.1779575940104,
                    755.9538928601667,
                ],
                "matrix_exponent_initial_offsets": (1, 0),
            },
        }
    },

    # Config items in the "wcs" section apply to the WCS service to all WCS coverages
    "wcs": {
        "formats": {
            "GeoTIFF": {
                "renderers": {
                    "1": "datacube_ows.wcs1_utils.get_tiff",
                    "2": "datacube_ows.wcs2_utils.get_tiff",
                },
                "mime": "image/geotiff",
                "extension": "tif",
                "multi-time": False
            },
            "netCDF": {
                "renderers": {
                    "1": "datacube_ows.wcs1_utils.get_netcdf",
                    "2": "datacube_ows.wcs2_utils.get_netcdf",
                },
                "mime": "application/x-netcdf",
                "extension": "nc",
                "multi-time": True,
            }
        },
        "native_format": "GeoTIFF",
    }, ###### End of "wcs" section

    # Products published by this datacube_ows instance.
    # The layers section is a list of layer definitions.  Each layer may be either:
    # 1) A folder-layer.  Folder-layers are not named and can contain a list of child layers.  Folder-layers are
    #    only used by WMS and WMTS - WCS does not support a hierarchical index of coverages.
    # 2) A mappable named layer that can be requested in WMS GetMap or WMTS GetTile requests.  A mappable named layer
    #    is also a coverage, that may be requested in WCS DescribeCoverage or WCS GetCoverage requests.

    "layers": [
        {
            # NOTE: This layer IS a mappable "named layer" that can be selected in GetMap requests
            "name": "composites",
            "title": "Sentinel-2 L2A Composites",
            "abstract": "Sentinel-2 L2A monthly median composites from imagery retrieved by Microsoft Planetary Computer",
            "product_name": "composites",
            "bands": bands_s2l2a_compos,
            "resource_limits": standard_resource_limits,
            "native_crs": "EPSG:3035",
            "native_resolution": [20.0, -20.0],
            "flags": None,
            "dynamic": True,
            "patch_url_function":  "datacube_ows.ogc_utils.nas_patch",
                # https://datacube-ows.readthedocs.io/en/latest/cfg_layers.html#url-patching-patch-url-function
                # https://github.com/digitalearthpacific/pacific-cube-in-a-box/blob/main/ows/ows_config/radar_backscatter/ows_s1_cfg.py#L88
            "image_processing": {
                "extent_mask_func": "datacube_ows.ogc_utils.mask_by_val",
                "always_fetch_bands": [],
                "fuse_func": None,
                "manual_merge": False,
                "apply_solar_corrections": False,
            },
            "styling": {
                "styles": [
                    style_rgb, style_false, style_ndvi
                    ],
            },
        },
        {
            "name": "cop-dem-30",
            "title": "Copernicus DEM",
            "abstract": "Elevation and Aspect of Copernicus DEM",
            "product_name": "copdem",
            "bands": bands_dem,
            "resource_limits": standard_resource_limits,
            "native_crs": "EPSG:3035",
            "native_resolution": [20.0, -20.0],
            "flags": None,
            "dynamic": True,
            "patch_url_function":  "datacube_ows.ogc_utils.nas_patch",
                # https://datacube-ows.readthedocs.io/en/latest/cfg_layers.html#url-patching-patch-url-function
                # https://github.com/digitalearthpacific/pacific-cube-in-a-box/blob/main/ows/ows_config/radar_backscatter/ows_s1_cfg.py#L88
            "image_processing": {
                "extent_mask_func": "datacube_ows.ogc_utils.mask_by_val",
                "extent_mask_args": {
                    "band": "elevation",
                    "val": 0
                },
            },
            "styling": {
                "styles": [
                    style_elevation, style_aspect
                    ],
            },
        },
        {
            "name": "tcd2023",
            "title": "Tree Canopy Density 2023",
            "abstract": "Provides at pan-Hellenic level in the spatial resolution of 10 m the level of tree cover density in a range from 0% to 100% for the 2023 reference year. DOI (raster 10m):https://doi.org/10.2909/e677441e-fb94-431c-b4f9-304f10e4dfd8",
            "product_name": "tcd2023",
            "bands": bands_tcd2023,
            "resource_limits": standard_resource_limits,
            "native_crs": "EPSG:3035",
            "native_resolution": [10.0, -10.0],
            "flags": None,
            "dynamic": True,
            "patch_url_function":  "datacube_ows.ogc_utils.nas_patch",
                # https://datacube-ows.readthedocs.io/en/latest/cfg_layers.html#url-patching-patch-url-function
                # https://github.com/digitalearthpacific/pacific-cube-in-a-box/blob/main/ows/ows_config/radar_backscatter/ows_s1_cfg.py#L88
            "image_processing": {
                "extent_mask_func": "datacube_ows.ogc_utils.mask_by_val",
                "always_fetch_bands": [],
                "fuse_func": None,
                "manual_merge": False,
                "apply_solar_corrections": False,
            },
            "styling": {
                "styles": [
                    style_tcd
                    ],
            },
        },
        {
            "name": "znormalized",
            "title": "Z-Normalized Spectral Indices",
            "abstract": "For each month from April 2023 onwards (monitoring period), the values of NDVI, EVI, and PSRI2 median composite images were normalized based on 2020-Jan to 2023-Mar. The time series of z-values forms the basis for the main analysis.",
            "product_name": "z_normalized",
            "bands": bands_znorm,
            "resource_limits": standard_resource_limits,
            "native_crs": "EPSG:3035",
            "native_resolution": [20.0, -20.0],
            "flags": None,
            "dynamic": True,
            "patch_url_function":  "datacube_ows.ogc_utils.nas_patch",
                # https://datacube-ows.readthedocs.io/en/latest/cfg_layers.html#url-patching-patch-url-function
                # https://github.com/digitalearthpacific/pacific-cube-in-a-box/blob/main/ows/ows_config/radar_backscatter/ows_s1_cfg.py#L88
            "image_processing": {
                "extent_mask_func": "datacube_ows.ogc_utils.mask_by_val",
                "always_fetch_bands": [],
                "fuse_func": None,
                "manual_merge": False,
                "apply_solar_corrections": False,
            },
            "styling": {
                "styles": [
                    style_ndvi_znorm, style_evi_znorm, style_psri2_znorm
                    ],
            },
        }
    ] 
} #### End of configuration object
