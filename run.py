from sia.satellite import sentinel2
from sia.indices import indice
from sia.utils.raster import *
import os
import errno
from collections import defaultdict
import numpy as np
from multiprocessing import Pool, cpu_count

np.seterr(divide='ignore', invalid='ignore')
s2 = sentinel2.Sentinel2()
ind = indice.Indice()


def main(start_date, end_date, cloud_threshold, data_days_interval, shape_file=None, bbox=None):
    pids = s2.get_product_ids(start_date, end_date, cloud_threshold,
                              data_days_interval, shape_file, bbox)

    args_list = [(key, val, shape_file) for key, val in pids.items()]
    pool = Pool(cpu_count() - 2)
    with pool:
        pool.map(ind.indices_generator, args_list)
