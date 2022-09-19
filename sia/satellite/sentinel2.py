import json
import os
from collections import defaultdict
from subprocess import call
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from shapely.geometry import Polygon
from osgeo import gdal, ogr
import pkg_resources

from sia.utils.helper import *

s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
s3_client = boto3.client('s3')
gdal.SetConfigOption('AWS_NO_SIGN_REQUEST', 'YES')

stream = pkg_resources.resource_stream('sia', 'satellite/satellite_tiles/s2_tile.shp')


class Sentinel2:
    def __init__(self):
        self.source_bucket = 'sentinel-cogs'
        self.source_s3_folder = 'sentinel-s2-l2a-cogs'
        self.bands = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B8A', 'B08', 'B09', 'B11', 'B12']
        self.crs = 'EPSG:4326'
        self.sat_name = 'sentinel2'
        self.sat_id = 'S2'
        self.cloud = 10
        self.s2_tile_shp = str(stream).split("'")[1]

    def get_mgrs_shp(self, aoi_shp, bbox):
        aoi_mgrs_shp = 'sentinel2_bbox_mgrs.shp'
        try:
            os.remove(aoi_mgrs_shp)
        except OSError:
            pass
        if aoi_shp:
            call(str('ogr2ogr -clipsrc ' + aoi_shp + ' ' + aoi_mgrs_shp + ' ' + self.s2_tile_shp), shell=True)
        else:
            min_x, min_y, max_x, max_y = bbox[0], bbox[1], bbox[2], bbox[3]
            call(str('ogr2ogr -f "ESRI Shapefile" ' + aoi_mgrs_shp + ' ' + self.s2_tile_shp + ' -clipsrc ' + str(min_x)
                     + ' ' + str(min_y) + ' ' + str(max_x) + ' ' + str(max_y)), shell=True)
        return aoi_mgrs_shp

    def shape_to_tiles(self, aoi_shp=None, bbox=None):
        aoi_mgrs_shp = self.get_mgrs_shp(aoi_shp, bbox)
        driver = ogr.GetDriverByName('ESRI Shapefile')
        ds = driver.Open(aoi_mgrs_shp)
        layer = ds.GetLayer(0)
        mgrs_list = []
        for feat in layer:
            mgrs_list.append(feat.GetField('tile'))
        del layer
        del ds
        return mgrs_list

    def get_product_ids(self, start_date, end_date, cloud_threshold, data_days_interval, shape_file=None, bbox=None):
        tile_list = self.shape_to_tiles(aoi_shp=shape_file, bbox=bbox)
        tile_list = ['43RDM']
        poly = shape_to_polygon(shp_file=shape_file, bbox=bbox)
        print(f'Tiles found for the given AOI : {tile_list}')
        final_dict = {'single_tile': {}, 'merge_tile': {}}
        for tile in tile_list:
            utm_zone, lat_band, grid_square = str(tile)[:2], str(tile)[2], str(tile)[3:5]
            for _date in datetime_iterator(start_date, end_date):
                _year = int(_date.year)
                _month = int(_date.month)
                _PREFIX = os.path.join(self.source_s3_folder, str(utm_zone), str(lat_band), str(grid_square),
                                       str(_year), str(_month), '')
                _PREFIX = _PREFIX.replace('\\', '/')
                response = s3_client.list_objects(Bucket=self.source_bucket, Prefix=_PREFIX)
                for content in response.get('Contents', []):
                    key = content['Key']
                    if key.endswith('.json'):
                        product_id = str(key).split('/')[-2]
                        pid_date = '-'.join([str(_year), str(_month).zfill(2), str(product_id[16:18]).zfill(2)])
                        if not validate_date(pid_date, start_date, end_date):
                            continue
                        result = s3.Object(self.source_bucket, key)
                        data = json.load(result.get()['Body'])
                        tile_coord = data['geometry']['coordinates']
                        tile_cloud = data['properties']['eo:cloud_cover']
                        tile_poly = Polygon(tile_coord[0])
                        percent_area = poly.intersection(tile_poly).area / poly.area
                        print(f'Date: {pid_date}   Tile:{tile}   Cloud: {tile_cloud}   Area AOI/Tile: {percent_area}')
                        if percent_area <= 0.05:
                            continue
                        elif percent_area >= 0.99:
                            if not final_dict['single_tile'].get(str(pid_date)):
                                final_dict['single_tile'][str(pid_date)] = {}
                            final_dict['single_tile'][str(pid_date)][str('pids')] = [product_id]
                            final_dict['single_tile'][str(pid_date)][str('cloud_percentages')] = tile_cloud
                        else:
                            if not final_dict['merge_tile'].get(str(pid_date)):
                                final_dict['merge_tile'][str(pid_date)] = defaultdict(list)
                            final_dict['merge_tile'][str(pid_date)][str('pids')].append(product_id)
                            final_dict['merge_tile'][str(pid_date)][str('tile_ids')].append(tile)
                            final_dict['merge_tile'][str(pid_date)][str('cloud_percentages')].append(tile_cloud)
                            final_dict['merge_tile'][str(pid_date)][str('percent_areas')].append(percent_area)
        diff = dates_dif(start_date, end_date)
        diff = diff // data_days_interval
        diff = diff // 2  # Data interval Buffer for user's input
        all_pids = {}
        if len(final_dict['single_tile']) >= diff:
            data = final_dict['single_tile']
            _dates = list(sorted(data.keys()))
            for date in _dates:
                print(f'Single Tile Metadata: {data[date]}')
                tile_cloud = data[date]['cloud_percentages']
                if tile_cloud > cloud_threshold:
                    continue
                else:
                    all_pids[str(date)] = data[date]['pids']
        else:
            data = final_dict['merge_tile']
            prev_date = None
            skip_one = False
            _dates = list(sorted(data.keys()))
            for date in _dates:
                if not prev_date:
                    prev_date = date
                    continue
                if skip_one:
                    prev_date = date
                    skip_one = False
                    continue
                date_diff = dt.strptime(str(date), "%Y-%m-%d") - dt.strptime(str(prev_date), "%Y-%m-%d")
                days_diff = date_diff.days
                weighted_sum = 0
                sum_area_percents = 0
                for i in range(len(data[prev_date]['tile_ids'])):
                    weighted_sum += data[prev_date]['cloud_percentages'][i] * data[prev_date]['percent_areas'][i]
                    sum_area_percents += data[prev_date]['percent_areas'][i]

                for i in range(len(data[date]['tile_ids'])):
                    weighted_sum += data[date]['cloud_percentages'][i] * data[date]['percent_areas'][i]
                    sum_area_percents += data[date]['percent_areas'][i]

                avg_cloud_percent = weighted_sum / sum_area_percents
                if avg_cloud_percent > cloud_threshold:
                    prev_date = date
                    continue
                if days_diff >= 5:
                    all_pids[str(prev_date)] = data[prev_date]['pids']
                elif days_diff < 5:
                    all_pids[str(date)] = data[prev_date]['pids'] + data[date]['pids']
                    skip_one = True
                prev_date = date
        final_pids = data_difference_days(all_pids, data_days_interval)
        return final_pids

    def pid_to_path(self, prod_id, band):
        lst = prod_id.split('_')
        tile = lst[1]
        utm_zone, lat_band, grid_sq = str(tile)[:2], str(tile)[2], str(tile)[3:5]
        _year, _month, _day = lst[2][0:4], lst[2][4:6], lst[2][6:8]
        vsi_ext = '/vsis3/'
        band_tif = os.path.join(vsi_ext, self.source_bucket, self.source_s3_folder, str(utm_zone), str(lat_band),
                                str(grid_sq), str(_year), str(int(_month)), str(prod_id), str(band) + '.tif')
        band_tif = band_tif.replace('\\', '/')
        return band_tif
