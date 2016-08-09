

  
# <a name="race-and-ethnicity"></a>Race and Ethnicity

Population breakdowns by race and ethnicity.

- [Total Population](#total-population)

    * [American Indian and Alaska Native Population](#american-indian-and-alaska-native-population)

    * [Asian Population](#asian-population)

    * [Black or African American Population](#black-or-african-american-population)

    * [Hispanic Population](#hispanic-population)

    * [Other Race population](#other-race-population)

    * [Population not Hispanic](#population-not-hispanic)

    * [Two or more races population](#two-or-more-races-population)

    * [White Population](#white-population)


## <a name="total-population"></a><a name="us-census-acs-b01003001"></a>Total Population

[<img alt="../../_images/us.census.acs.B01003001.png" src="../../_images/us.census.acs.B01003001.png" style="width: 100%;" />](../../_images/us.census.acs.B01003001.png)The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.

Measure &quot;Total Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01003001'
        );

Measure &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01003001'
        );

Subcolumns of Total Population

- [American Indian and Alaska Native Population](#american-indian-and-alaska-native-population)

- [Asian Population](#asian-population)

- [Black or African American Population](#black-or-african-american-population)

- [Hispanic Population](#hispanic-population)

- [Other Race population](#other-race-population)

- [Population not Hispanic](#population-not-hispanic)

- [Two or more races population](#two-or-more-races-population)

- [White Population](#white-population)


### <a name="american-indian-and-alaska-native-population"></a><a name="us-census-acs-b03002005"></a>American Indian and Alaska Native Population

[<img alt="../../_images/us.census.acs.B03002005.png" src="../../_images/us.census.acs.B03002005.png" style="width: 100%;" />](../../_images/us.census.acs.B03002005.png)The number of people identifying as American Indian or Alaska native in each geography.

Measure &quot;American Indian and Alaska Native Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002005'
        );

Measure &quot;American Indian and Alaska Native Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002005'
        );

Measure &quot;American Indian and Alaska Native Population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002005',
          'denominator'
        );

Measure &quot;American Indian and Alaska Native Population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002005',
          'denominator'
        );

* denominator: [Total Population](#us-census-acs-b01003001)


### <a name="asian-population"></a><a name="us-census-acs-b03002006"></a>Asian Population

[<img alt="../../_images/us.census.acs.B03002006.png" src="../../_images/us.census.acs.B03002006.png" style="width: 100%;" />](../../_images/us.census.acs.B03002006.png)The number of people identifying as Asian, non-Hispanic in each geography.

Measure &quot;Asian Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002006'
        );

Measure &quot;Asian Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002006'
        );

Measure &quot;Asian Population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002006',
          'denominator'
        );

Measure &quot;Asian Population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002006',
          'denominator'
        );

* denominator: [Total Population](#us-census-acs-b01003001)


### <a name="black-or-african-american-population"></a><a name="us-census-acs-b03002004"></a>Black or African American Population

[<img alt="../../_images/us.census.acs.B03002004.png" src="../../_images/us.census.acs.B03002004.png" style="width: 100%;" />](../../_images/us.census.acs.B03002004.png)The number of people identifying as black or African American, non-Hispanic in each geography.

Measure &quot;Black or African American Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002004'
        );

Measure &quot;Black or African American Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002004'
        );

Measure &quot;Black or African American Population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002004',
          'denominator'
        );

Measure &quot;Black or African American Population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002004',
          'denominator'
        );

* denominator: [Total Population](#us-census-acs-b01003001)


### <a name="hispanic-population"></a><a name="us-census-acs-b03002012"></a>Hispanic Population

[<img alt="../../_images/us.census.acs.B03002012.png" src="../../_images/us.census.acs.B03002012.png" style="width: 100%;" />](../../_images/us.census.acs.B03002012.png)The number of people identifying as Hispanic or Latino in each geography.

Measure &quot;Hispanic Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002012'
        );

Measure &quot;Hispanic Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002012'
        );

Measure &quot;Hispanic Population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002012',
          'denominator'
        );

Measure &quot;Hispanic Population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002012',
          'denominator'
        );

* denominator: [Total Population](#us-census-acs-b01003001)


### <a name="other-race-population"></a><a name="us-census-acs-b03002008"></a>Other Race population

[<img alt="../../_images/us.census.acs.B03002008.png" src="../../_images/us.census.acs.B03002008.png" style="width: 100%;" />](../../_images/us.census.acs.B03002008.png)The number of people identifying as another race in each geography

Measure &quot;Other Race population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002008'
        );

Measure &quot;Other Race population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002008'
        );

Measure &quot;Other Race population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002008',
          'denominator'
        );

Measure &quot;Other Race population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002008',
          'denominator'
        );

* denominator: [Total Population](#us-census-acs-b01003001)


### <a name="population-not-hispanic"></a><a name="us-census-acs-b03002002"></a>Population not Hispanic

[<img alt="../../_images/us.census.acs.B03002002.png" src="../../_images/us.census.acs.B03002002.png" style="width: 100%;" />](../../_images/us.census.acs.B03002002.png)The number of people not identifying as Hispanic or Latino in each geography.

Measure &quot;Population not Hispanic&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002002'
        );

Measure &quot;Population not Hispanic&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002002'
        );

Measure &quot;Population not Hispanic&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002002',
          'denominator'
        );

Measure &quot;Population not Hispanic&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002002',
          'denominator'
        );

* denominator: [Total Population](#us-census-acs-b01003001)


### <a name="two-or-more-races-population"></a><a name="us-census-acs-b03002009"></a>Two or more races population

[<img alt="../../_images/us.census.acs.B03002009.png" src="../../_images/us.census.acs.B03002009.png" style="width: 100%;" />](../../_images/us.census.acs.B03002009.png)The number of people identifying as two or more races in each geography

Measure &quot;Two or more races population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002009'
        );

Measure &quot;Two or more races population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002009'
        );

Measure &quot;Two or more races population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002009',
          'denominator'
        );

Measure &quot;Two or more races population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002009',
          'denominator'
        );

* denominator: [Total Population](#us-census-acs-b01003001)


### <a name="white-population"></a><a name="us-census-acs-b03002003"></a>White Population

[<img alt="../../_images/us.census.acs.B03002003.png" src="../../_images/us.census.acs.B03002003.png" style="width: 100%;" />](../../_images/us.census.acs.B03002003.png)The number of people identifying as white, non-Hispanic in each geography.

Measure &quot;White Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002003'
        );

Measure &quot;White Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002003'
        );

Measure &quot;White Population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B03002003',
          'denominator'
        );

Measure &quot;White Population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B03002003',
          'denominator'
        );

* denominator: [Total Population](#us-census-acs-b01003001)


