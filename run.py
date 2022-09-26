from sia.satellite import sentinel2
from sia.utils.raster import *
import os
import errno
from collections import defaultdict
import numpy as np

np.seterr(divide='ignore', invalid='ignore')
s2 = sentinel2.Sentinel2()


def main(start_date, end_date, cloud_threshold, data_days_interval, shape_file=None, bbox=None):
    pids = s2.get_product_ids(start_date, end_date, cloud_threshold,
                              data_days_interval, shape_file, bbox)


