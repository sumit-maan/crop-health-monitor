from datetime import timedelta
from datetime import datetime as dt
import shapely.wkt
from osgeo import ogr
from shapely.geometry import box
from dateutil.relativedelta import relativedelta


def shape_to_polygon(shp_file=None, bbox=None):
    poly = None
    if bbox:
        poly = box(*bbox, ccw=True)
    elif shp_file:
        ds = ogr.Open(shp_file)
        layer = ds.GetLayer(0)
        wkt_poly = None
        for feat in layer:
            wkt_poly = feat.geometry().ExportToWkt()
        poly = shapely.wkt.loads(wkt_poly)
    return poly


def dates_dif(date1, date2):
    d0 = dt.strptime(date1, '%Y-%m-%d').date()
    d1 = dt.strptime(date2, '%Y-%m-%d').date()
    delta = abs(d1 - d0).days
    return delta


def validate_date(original_date, start_date, end_date):
    final_date = dt.strptime(str(original_date), '%Y-%m-%d')
    s_date = dt.strptime(str(start_date), '%Y-%m-%d')
    e_date = dt.strptime(str(end_date), '%Y-%m-%d')
    return s_date <= final_date <= e_date


def datetime_iterator(start_date=None, end_date=None):
    if not end_date:
        end_date = dt.today().date()
    if not start_date:
        start_date = end_date - timedelta(30)
    start_date = dt.strptime(str(start_date), '%Y-%m-%d').date()
    start_date = start_date.replace(day=1)
    end_date = dt.strptime(str(end_date), '%Y-%m-%d').date()
    while start_date <= end_date:
        yield start_date
        start_date = start_date + relativedelta(months=1)


def data_difference_days(product_dictionary, days_interval):
    prev_date = None
    _dates = list(sorted(product_dictionary.keys()))
    n = len(_dates)
    for i in range(n - 1):
        if not prev_date:
            prev_date = _dates[i]
        date = _dates[i + 1]
        date_diff = dt.strptime(date, "%Y-%m-%d") - dt.strptime(prev_date, "%Y-%m-%d")
        days_diff = date_diff.days
        if days_diff < days_interval:
            product_dictionary.pop(date)
        else:
            prev_date = None
    return product_dictionary
