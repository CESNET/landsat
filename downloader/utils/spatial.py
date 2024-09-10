def convert_geojson_to_bbox(geojson):
    """
    Method creates a bounding box from geojson
    :param geojson: processed geojson
    :return: list of coords defining bounding box which belongs to geojson polygon
    """

    from geojson.utils import coords
    from shapely.geometry import LineString

    return list(LineString(coords(geojson)).bounds)
