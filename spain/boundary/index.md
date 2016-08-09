

  
# <a name="boundaries"></a>Boundaries

Political, administrative, and census-based boundaries.

- [Autonomous Community](#autonomous-community)

- [Municipality](#municipality)

- [Province](#province)

- [Sección Censal](#seccion-censal)


## <a name="autonomous-community"></a><a name="es-cnig-ccaa"></a>Autonomous Community

The first-level administrative subdivision of Spain.

Obtain &quot;Autonomous Community&quot; geometry at one point:

    INSERT INTO {tablename}(the_geom)
      SELECT OBS_GetBoundary(
        CDB_LatLng(40.7, -73.9),
        'es.cnig.ccaa'
      )

Obtain all &quot;Autonomous Community&quot; geometries within an area:

    INSERT INTO {new_table_name} (the_geom, {geo_id_column})
      SELECT *
      FROM OBS_GetBoundariesByGeometry(
        ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
        'es.cnig.ccaa'
      )


## <a name="municipality"></a><a name="es-cnig-muni"></a>Municipality

The lowest level of territorial organization in Spain. Municipal boundaries do not cross between provinces.

Obtain &quot;Municipality&quot; geometry at one point:

    INSERT INTO {tablename}(the_geom)
      SELECT OBS_GetBoundary(
        CDB_LatLng(40.7, -73.9),
        'es.cnig.muni'
      )

Obtain all &quot;Municipality&quot; geometries within an area:

    INSERT INTO {new_table_name} (the_geom, {geo_id_column})
      SELECT *
      FROM OBS_GetBoundariesByGeometry(
        ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
        'es.cnig.muni'
      )


## <a name="province"></a><a name="es-cnig-prov"></a>Province

The second-level administrative subdivision of Spain, used primarily as electoral districts and geographic references.  Provinces do not cross between autonomous communities.

Obtain &quot;Province&quot; geometry at one point:

    INSERT INTO {tablename}(the_geom)
      SELECT OBS_GetBoundary(
        CDB_LatLng(40.7, -73.9),
        'es.cnig.prov'
      )

Obtain all &quot;Province&quot; geometries within an area:

    INSERT INTO {new_table_name} (the_geom, {geo_id_column})
      SELECT *
      FROM OBS_GetBoundariesByGeometry(
        ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
        'es.cnig.prov'
      )


## <a name="seccion-censal"></a><a name="es-ine-the-geom"></a>Sección Censal

The smallest division of the Spanish Census.

Obtain &quot;Sección Censal&quot; geometry at one point:

    INSERT INTO {tablename}(the_geom)
      SELECT OBS_GetBoundary(
        CDB_LatLng(40.39, -3.7),
        'es.ine.the_geom'
      )

Obtain all &quot;Sección Censal&quot; geometries within an area:

    INSERT INTO {new_table_name} (the_geom, {geo_id_column})
      SELECT *
      FROM OBS_GetBoundariesByGeometry(
        ST_Buffer(CDB_LatLng(40.39, -3.7), 0.01),
        'es.ine.the_geom'
      )


