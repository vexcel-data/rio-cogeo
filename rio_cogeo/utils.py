"""rio_cogeo.utils: Utility functions."""

import rasterio
from rasterio.enums import MaskFlags, ColorInterp


def get_maximum_overview_level(src_path, minsize=512):
    """Calculate the maximum overview level."""
    with rasterio.open(src_path) as src:
        width = src.width
        height = src.height

    nlevel = 0
    overview = 1

    while min(width // overview, height // overview) > minsize:
        overview *= 2
        nlevel += 1

    return nlevel


def has_alpha_band(src_dst):
    """Check for alpha band or mask in source."""
    if (
        any([MaskFlags.alpha in flags for flags in src_dst.mask_flag_enums])
        or ColorInterp.alpha in src_dst.colorinterp
    ):
        return True
    return False


def has_mask_band(src_dst):
    """Check for mask band in source."""
    if any([MaskFlags.per_dataset in flags for flags in src_dst.mask_flag_enums]):
        return True
    return False
