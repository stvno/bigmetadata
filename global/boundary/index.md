

  
# <a name="boundaries"></a>Boundaries

Political, administrative, and census-based boundaries.

- [Continents](#continents)

- [Countries](#countries)

- [Disputed Areas](#disputed-areas)

- [Marine Areas](#marine-areas)

- [Regions (First-level Administrative)](#regions-first-level-administrative)


## <a name="continents"></a><a name="whosonfirst-wof-continent-geom"></a>Continents

[<img alt="../../_images/whosonfirst.wof_continent_geom.png" src="../../_images/whosonfirst.wof_continent_geom.png" style="width: 100%;" />](../../_images/whosonfirst.wof_continent_geom.png)Obtain &quot;Continents&quot; geometry at one point:

    INSERT INTO {tablename}(the_geom)
      SELECT OBS_GetBoundary(
        CDB_LatLng(40.7, -73.9),
        'whosonfirst.wof_continent_geom'
      )

Obtain all &quot;Continents&quot; geometries within an area:

    INSERT INTO {new_table_name} (the_geom, {geo_id_column})
      SELECT *
      FROM OBS_GetBoundariesByGeometry(
        ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
        'whosonfirst.wof_continent_geom'
      )


## <a name="countries"></a><a name="whosonfirst-wof-country-geom"></a>Countries

[<img alt="../../_images/whosonfirst.wof_country_geom.png" src="../../_images/whosonfirst.wof_country_geom.png" style="width: 100%;" />](../../_images/whosonfirst.wof_country_geom.png)Obtain &quot;Countries&quot; geometry at one point:

    INSERT INTO {tablename}(the_geom)
      SELECT OBS_GetBoundary(
        CDB_LatLng(40.7, -73.9),
        'whosonfirst.wof_country_geom'
      )

Obtain all &quot;Countries&quot; geometries within an area:

    INSERT INTO {new_table_name} (the_geom, {geo_id_column})
      SELECT *
      FROM OBS_GetBoundariesByGeometry(
        ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
        'whosonfirst.wof_country_geom'
      )


## <a name="disputed-areas"></a><a name="whosonfirst-wof-disputed-geom"></a>Disputed Areas

Obtain &quot;Disputed Areas&quot; geometry at one point:

    INSERT INTO {tablename}(the_geom)
      SELECT OBS_GetBoundary(
        CDB_LatLng(33.78, 76.57),
        'whosonfirst.wof_disputed_geom'
      )

Obtain all &quot;Disputed Areas&quot; geometries within an area:

    INSERT INTO {new_table_name} (the_geom, {geo_id_column})
      SELECT *
      FROM OBS_GetBoundariesByGeometry(
        ST_Buffer(CDB_LatLng(33.78, 76.57), 0.01),
        'whosonfirst.wof_disputed_geom'
      )


## <a name="marine-areas"></a><a name="whosonfirst-wof-marinearea-geom"></a>Marine Areas

Obtain &quot;Marine Areas&quot; geometry at one point:

    INSERT INTO {tablename}(the_geom)
      SELECT OBS_GetBoundary(
        CDB_LatLng(43.33, -68.47),
        'whosonfirst.wof_marinearea_geom'
      )

Obtain all &quot;Marine Areas&quot; geometries within an area:

    INSERT INTO {new_table_name} (the_geom, {geo_id_column})
      SELECT *
      FROM OBS_GetBoundariesByGeometry(
        ST_Buffer(CDB_LatLng(43.33, -68.47), 0.01),
        'whosonfirst.wof_marinearea_geom'
      )


## <a name="regions-first-level-administrative"></a><a name="whosonfirst-wof-region-geom"></a>Regions (First-level Administrative)

[<img alt="../../_images/whosonfirst.wof_region_geom.png" src="../../_images/whosonfirst.wof_region_geom.png" style="width: 100%;" />](../../_images/whosonfirst.wof_region_geom.png)Obtain &quot;Regions (First-level Administrative)&quot; geometry at one point:

    INSERT INTO {tablename}(the_geom)
      SELECT OBS_GetBoundary(
        CDB_LatLng(40.7, -73.9),
        'whosonfirst.wof_region_geom'
      )

Obtain all &quot;Regions (First-level Administrative)&quot; geometries within an area:

    INSERT INTO {new_table_name} (the_geom, {geo_id_column})
      SELECT *
      FROM OBS_GetBoundariesByGeometry(
        ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
        'whosonfirst.wof_region_geom'
      )


