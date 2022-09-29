from sia.satellite import sentinel2
from sia.indices import indice
import numpy as np
from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta
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
parser.add_argument('-sf', '--shape_file', help="Enter a valid shapefile path", default=None)
parser.add_argument('-bb', '--bbox', help="Enter valid bounding box in order - minx miny maxx maxy", default=None)
args = parser.parse_args()


def main(start_date, end_date, cloud_threshold, data_days_interval, shape_file=None, bbox=None):
    pids = s2.get_product_ids(start_date, end_date, cloud_threshold, data_days_interval, shape_file, bbox)

    if args.shape_file:
        args_list = [(key, val, args.shape_file) for key, val in pids.items()]
    else:
        args_list = [(key, val, args.bbox) for key, val in pids.items()]

    pool = Pool(cpu_count() - 2)
    with pool:
        pool.map(ig.indices_generator, args_list)


if __name__ == '__main__':
    main(args.start_date, args.end_date, args.cloud_threshold, args.data_interval,
         args.shape_file, args.bbox)
    print('Cheers !')
