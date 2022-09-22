from osgeo import gdal


def raster_to_array(raster_file, data_type):
    ds = gdal.Open(raster_file)
    band = ds.GetRasterBand(1)
    arr = band.ReadAsArray()
    arr = arr.astype(data_type)
    return arr


def merge_clip_raster(raster_file_list, output_file=None, shp_file=None, bbox=None, out_width=None, out_height=None):
    ds_lst = list()
    for raster in raster_file_list:
        ds = gdal.Warp('', raster, format='vrt', dstNodata=0,
                       dstSRS="+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0",
                       cutlineDSName=shp_file, cropToCutline=True)
        ds_lst.append(ds)
    del ds
    if shp_file:
        ds = gdal.BuildVRT('', ds_lst, VRTNodata=0, srcNodata=0)
        gdal.Warp(output_file, ds, format='GTiff', dstNodata=0,
                  dstSRS="+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0",
                  cutlineDSName=shp_file, cropToCutline=True)
    else:
        if out_width:
            ds = gdal.BuildVRT('', ds_lst, VRTNodata=0, srcNodata=0)
            gdal.Warp(output_file, ds, format='GTiff', dstNodata=0,
                      dstSRS="+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0",
                      outputBounds=tuple(bbox), cropToCutline=True, width=out_width, height=out_height)
        else:
            ds = gdal.BuildVRT('', ds_lst, VRTNodata=0, srcNodata=0)
            gdal.Warp(output_file, ds, format='GTiff', dstNodata=0,
                      dstSRS="+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0",
                      outputBounds=tuple(bbox), cropToCutline=True)
    return output_file


def write_raster(ref_raster, array, dst_filename, gdal_GDT_datatype):
    driver = gdal.GetDriverByName('GTiff')
    cols = array.shape[1]
    rows = array.shape[0]
    out_ds = driver.Create(dst_filename, cols, rows, 1, gdal_GDT_datatype)
    out_ds.GetRasterBand(1).WriteArray(array)

    # adding GeoTransform and Projection
    data0 = gdal.Open(ref_raster)
    geo_trans = data0.GetGeoTransform()
    proj = data0.GetProjection()
    del data0
    out_ds.SetGeoTransform(geo_trans)
    out_ds.SetProjection(proj)
    out_ds.FlushCache()
    del out_ds
    return dst_filename
