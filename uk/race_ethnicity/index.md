

  
# <a name="race-and-ethnicity"></a>Race and Ethnicity

Population breakdowns by race and ethnicity.

- [Asian/Asian British population](#asian-asian-british-population)

- [Black/African/Caribbean/Black British population](#black-african-caribbean-black-british-population)

- [Mixed/multiple ethnic group population](#mixed-multiple-ethnic-group-population)

- [Other ethnic group population](#other-ethnic-group-population)

- [White population](#white-population)

    * [White English/Welsh/Scottish/Northern Irish/British population](#white-english-welsh-scottish-northern-irish-british-population)

    * [White Irish population](#white-irish-population)


## <a name="asian-asian-british-population"></a><a name="uk-ons-lc2201ew0061"></a>Asian/Asian British population

Measure &quot;Asian/Asian British population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0061'
        );

Measure &quot;Asian/Asian British population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0061'
        );

Measure &quot;Asian/Asian British population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0061',
          'denominator'
        );

Measure &quot;Asian/Asian British population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0061',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


## <a name="black-african-caribbean-black-british-population"></a><a name="uk-ons-lc2201ew0071"></a>Black/African/Caribbean/Black British population

Measure &quot;Black/African/Caribbean/Black British population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0071'
        );

Measure &quot;Black/African/Caribbean/Black British population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0071'
        );

Measure &quot;Black/African/Caribbean/Black British population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0071',
          'denominator'
        );

Measure &quot;Black/African/Caribbean/Black British population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0071',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


## <a name="mixed-multiple-ethnic-group-population"></a><a name="uk-ons-lc2201ew0051"></a>Mixed/multiple ethnic group population

Measure &quot;Mixed/multiple ethnic group population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0051'
        );

Measure &quot;Mixed/multiple ethnic group population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0051'
        );

Measure &quot;Mixed/multiple ethnic group population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0051',
          'denominator'
        );

Measure &quot;Mixed/multiple ethnic group population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0051',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


## <a name="other-ethnic-group-population"></a><a name="uk-ons-lc2201ew0081"></a>Other ethnic group population

Measure &quot;Other ethnic group population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0081'
        );

Measure &quot;Other ethnic group population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0081'
        );

Measure &quot;Other ethnic group population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0081',
          'denominator'
        );

Measure &quot;Other ethnic group population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0081',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


## <a name="white-population"></a><a name="uk-ons-lc2201ew0011"></a>White population

Measure &quot;White population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0011'
        );

Measure &quot;White population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0011'
        );

Measure &quot;White population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0011',
          'denominator'
        );

Measure &quot;White population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0011',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)

Subcolumns of White population

- [White English/Welsh/Scottish/Northern Irish/British population](#white-english-welsh-scottish-northern-irish-british-population)

- [White Irish population](#white-irish-population)


### <a name="white-english-welsh-scottish-northern-irish-british-population"></a><a name="uk-ons-lc2201ew0021"></a>White English/Welsh/Scottish/Northern Irish/British population

Measure &quot;White English/Welsh/Scottish/Northern Irish/British population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0021'
        );

Measure &quot;White English/Welsh/Scottish/Northern Irish/British population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0021'
        );

Measure &quot;White English/Welsh/Scottish/Northern Irish/British population&quot; percent of &quot;White population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0021',
          'denominator'
        );

Measure &quot;White English/Welsh/Scottish/Northern Irish/British population&quot; percent of &quot;White population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0021',
          'denominator'
        );

* denominator: [White population](#uk-ons-lc2201ew0011)


### <a name="white-irish-population"></a><a name="uk-ons-lc2201ew0031"></a>White Irish population

Measure &quot;White Irish population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0031'
        );

Measure &quot;White Irish population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0031'
        );

Measure &quot;White Irish population&quot; percent of &quot;White population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2201EW0031',
          'denominator'
        );

Measure &quot;White Irish population&quot; percent of &quot;White population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2201EW0031',
          'denominator'
        );

* denominator: [White population](#uk-ons-lc2201ew0011)


