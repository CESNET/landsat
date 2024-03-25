def convert_geojson_to_bbox(geojson):
    from geojson.utils import coords
    from shapely.geometry import LineString

    return list(LineString(coords(geojson)).bounds)
