

  
# <a name="boundaries"></a>Boundaries

Political, administrative, and census-based boundaries.

- [Census Output Areas](#census-output-areas)


## <a name="census-output-areas"></a><a name="uk-cdrc-the-geom"></a>Census Output Areas

The smallest unit for which census data are published in the UK.  They contain at least 40 households and 100 persons, the target size being 125 households. They were built up from postcode blocks after the census data were available, with the intention of standardising population sizes, geographical shape and social homogeneity (in terms of dwelling types and housing tenure). The OAs generated in 2001 were retained as far as possible for the publication of outputs from the 2011 Census (less than 3% were changed). -[Wikipedia](https://en.wikipedia.org/wiki/ONS_coding_system#Geography_of_the_UK_Census)

Obtain &quot;Census Output Areas&quot; geometry at one point:

    INSERT INTO {tablename}(the_geom)
      SELECT OBS_GetBoundary(
        CDB_LatLng(40.7, -73.9),
        'uk.cdrc.the_geom'
      )

Obtain all &quot;Census Output Areas&quot; geometries within an area:

    INSERT INTO {new_table_name} (the_geom, {geo_id_column})
      SELECT *
      FROM OBS_GetBoundariesByGeometry(
        ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
        'uk.cdrc.the_geom'
      )


