-- TODO: This doesn't work at all

CREATE OR REPLACE FUNCTION get_raster(
  mid INT, vid INT, north FLOAT, east FLOAT, south FLOAT, west FLOAT)
  RETURNS TEXT
AS $$
  # centroids = []
  data = []
  centroids = plpy.execute(
    'SELECT get_raster_centroids({}, {}, {}, {}, {}, {}) LIMIT 1'.format(
        mid, vid, north, east, south, west
    ))
  print type(centroids[0]['get_raster_centroids'])
  values = plpy.execute(
    'SELECT get_raster_values({}, {}, {}, {}, {}, {}) LIMIT 1'.format(
      mid, vid, north, east, south, west
    ))
  chunk_size = plpy.execute(
    'SELECT get_max_nband({}, {})'.format(
      mid, vid, north, east, south, west))
  for c in centroids:
    tile_x, tile_y, v = c['get_raster_centroids'].split(',')
    tile_x = int(tile_x.replace('(', ''))
    tile_y = int(tile_y)
    x, y = v.split(' ')
    x = float(x.replace('"POINT(', ''))
    y = float(y.replace(')")', ''))
    this_point = dict(tile_x=[tile_x, tile_y], x=x, y=y, values=[])
    print this_point

  for i in xrange(0, len(values), chunk_size):
    chunk = values[i:i+chunk_size]
    print(chunk)


  return None
$$ LANGUAGE plpythonu;