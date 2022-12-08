#  python run.py -sf /Users/sumitmaan/work/dehaat/india_shapefile/districts/khagaria/file.shp -ag
#  /Users/sumitmaan/work/dehaat/crop-health-monitor/khagaria.img -sd 2021-10-01 -ed 2022-03-31

import errno
import os
import shutil

from sia.satellite import sentinel2
from sia.indices import indice
import numpy as np
from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta
from sia.utils.raster import raster_sampling
import argparse

np.seterr(divide='ignore', invalid='ignore')
s2 = sentinel2.Sentinel2()
ig = indice.Indice()

parser = argparse.ArgumentParser()
parser.add_argument('-sd', '--start_date', help="Enter valid start date in yyyy-mm-dd format",
                    default=str(datetime.today().date() - timedelta(30)))
parser.add_argument('-ed', '--end_date', help="Enter valid end date in yyyy-mm-dd format",
                    default=str(datetime.today().date()))
parser.add_argument('-c', '--cloud_threshold', help="Enter max cloud threshold", default=10)
parser.add_argument('-di', '--data_interval', help="Enter required data interval in days", default=5)
parser.add_argument('-sf', '--shape_file', help="Enter a valid shapefile path! Skip if passing bbox or s2 tile",
                    default=None)
parser.add_argument('-bb', '--bbox', help="Enter valid bounding box in order - minx miny maxx maxy! Skip if passing "
                                          "shapefile or s2 tile", default=None)
parser.add_argument('-t', '--tiles', help="Enter valid space separated s2 tiles, Skip if passing shapefile or bbox",
                    default=None)
parser.add_argument('-ag', '--agrimask', help="Enter valid path of agrimask for given AOI", default=None)
args = parser.parse_args()


def main(start_date, end_date, cloud_threshold, data_days_interval, shape_file=None, bbox=None, tiles=None,
         agrimask=None):
    try:
        shutil.rmtree('data')
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))
    pids = s2.get_product_ids(start_date, end_date, cloud_threshold, data_days_interval, shape_file, bbox, tiles)
    height = None
    width = None
    if agrimask:
        value0_list = list(pids.values())[0]
        temp_band_rasters = []
        for item in value0_list:
            temp_band_rasters.append(s2.pid_to_path(item, 'B04'))
        height, width = raster_sampling(temp_band_rasters, agrimask, shape_file)
    if args.shape_file:
        args_list = [(key, val, args.shape_file, height, width) for key, val in pids.items()]
    else:
        args_list = [(key, val, args.bbox, height, width) for key, val in pids.items()]

    pool = Pool(cpu_count() - 2)
    with pool:
        pool.map(ig.indices_generator, args_list)
    pool.close()


if __name__ == '__main__':
    main(args.start_date, args.end_date, args.cloud_threshold, args.data_interval,
         args.shape_file, args.bbox, args.tiles, args.agrimask)
    print('Cheers !')
