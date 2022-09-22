from sia.satellite import sentinel2
from sia.utils.raster import *
import os
import errno
from collections import defaultdict
import numpy as np

np.seterr(divide='ignore', invalid='ignore')
s2 = sentinel2.Sentinel2()


class Indice:
    def __init__(self):
        self.satellite = 'sentinel2'

    def indices_generator(self, pid_set):
        key = pid_set[0]
        val = pid_set[1]
        shape_file = pid_set[2]
        base_name = os.path.basename(shape_file)
        root_fname = os.path.splitext(base_name)[0]
        bands_path = os.path.join('data', str(root_fname), 'bands', str(key))
        indices_path = os.path.join('data', str(root_fname), 'indices', str(key))
        try:
            os.makedirs(bands_path)
            os.makedirs(indices_path)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
            pass
        raster_list = defaultdict(list)
        for item in val:
            raster_list['b4_path'].append(s2.pid_to_path(item, 'B04'))
            raster_list['b8_path'].append(s2.pid_to_path(item, 'B08'))
            raster_list['b8a_path'].append(s2.pid_to_path(item, 'B8A'))
            raster_list['b11_path'].append(s2.pid_to_path(item, 'B11'))

        b4_file = merge_clip_raster(raster_file_list=raster_list['b4_path'], output_file=f'{bands_path}/b4.tif',
                                    shp_file=shape_file)
        b8_file = merge_clip_raster(raster_file_list=raster_list['b8_path'], output_file=f'{bands_path}/b8.tif',
                                    shp_file=shape_file)

        b4 = raster_to_array(b4_file, 'int16')
        b8 = raster_to_array(b8_file, 'int16')
        ndvi = self.get_ndvi(b4, b8)
        ndvi = np.around(ndvi, decimals=2, out=None)
        write_raster(f'{bands_path}/b4.tif', ndvi, f'{indices_path}/ndvi.tif', gdal.GDT_Float32)

        savi = self.get_savi(b4, b8, 0.428)
        savi = np.around(savi, decimals=2, out=None)
        write_raster(f'{bands_path}/b4.tif', savi, f'{indices_path}/savi.tif', gdal.GDT_Float32)
        b4 = b8 = ndvi = savi = None

        b8a_file = merge_clip_raster(raster_file_list=raster_list['b8a_path'], output_file=f'{bands_path}/b8a.tif',
                                     shp_file=shape_file)
        b11_file = merge_clip_raster(raster_file_list=raster_list['b11_path'], output_file=f'{bands_path}/b11.tif',
                                     shp_file=shape_file)
        b8a = raster_to_array(b8a_file, 'int16')
        b11 = raster_to_array(b11_file, 'int16')

        lswi = self.get_lswi(b8a, b11)
        lswi = np.around(lswi, decimals=2, out=None)
        write_raster(f'{bands_path}/b8a.tif', lswi, f'{indices_path}/lswi.tif', gdal.GDT_Float32)
        b8a = b11 = lswi = None

    @staticmethod
    def get_ndvi(red, nir):
        arr = (nir - red) / (nir + red)
        arr[arr > 1] = 0
        arr[arr < -1] = 0
        return arr

    @staticmethod
    def get_savi(red, nir, l):  ## l = soil brightness correction factor could range from (0 -1)
        arr = (1.0 + l) * (nir - red) / (nir + red + l)
        return arr

    @staticmethod
    def get_lswi(narrow_nir, swir):
        arr = (narrow_nir - swir) / (narrow_nir + swir)
        arr[arr > 1] = 0
        arr[arr < -1] = 0
        return arr