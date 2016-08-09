

  
# <a name="age-and-gender"></a>Age and Gender

Population breakdowns by age and gender.

- [Children under 18 Years of Age](#children-under-18-years-of-age)

- [Median Age](#median-age)

- [Men age 45 to 64 (&quot;middle aged&quot;)](#men-age-45-to-64-middle-aged)

- [Population 1 year and over](#population-1-year-and-over)

- [Population 15 Years and Over](#population-15-years-and-over)

- [Population age 16 and over](#population-age-16-and-over)

- [Total Population](#total-population)

    * [Female age 5 to 9](#female-age-5-to-9)

    * [Female age 10 to 14](#female-age-10-to-14)

    * [Female age 15 to 17](#female-age-15-to-17)

    * [Female age 18 and 19](#female-age-18-and-19)

    * [Female age 20](#female-age-20)

    * [Female age 21](#female-age-21)

    * [Female age 22 to 24](#female-age-22-to-24)

    * [Female age 25 to 29](#female-age-25-to-29)

    * [Female age 30 to 34](#female-age-30-to-34)

    * [Female age 35 to 39](#female-age-35-to-39)

    * [Female age 40 to 44](#female-age-40-to-44)

    * [Female age 45 to 49](#female-age-45-to-49)

    * [Female age 50 to 54](#female-age-50-to-54)

    * [Female age 55 to 59](#female-age-55-to-59)

    * [Female age 60 and 61](#female-age-60-and-61)

    * [Female age 62 to 64](#female-age-62-to-64)

    * [Female age 65 to 66](#female-age-65-to-66)

    * [Female age 67 to 69](#female-age-67-to-69)

    * [Female age 70 to 74](#female-age-70-to-74)

    * [Female age 75 to 79](#female-age-75-to-79)

    * [Female age 80 to 84](#female-age-80-to-84)

    * [Female age 85 and over](#female-age-85-and-over)

    * [Female Population](#female-population)

    * [Female under 5 years](#female-under-5-years)

    * [Male age 10 to 14](#male-age-10-to-14)

    * [Male age 15 to 17](#male-age-15-to-17)

    * [Male age 18 and 19](#male-age-18-and-19)

    * [Male age 20](#male-age-20)

    * [Male age 21](#male-age-21)

    * [Male age 22 to 24](#male-age-22-to-24)

    * [Male age 25 to 29](#male-age-25-to-29)

    * [Male age 30 to 34](#male-age-30-to-34)

    * [Male age 35 to 39](#male-age-35-to-39)

    * [Male age 40 to 44](#male-age-40-to-44)

    * [Male age 65 to 66](#male-age-65-to-66)

    * [Male age 67 to 69](#male-age-67-to-69)

    * [Male age 70 to 74](#male-age-70-to-74)

    * [Male age 75 to 79](#male-age-75-to-79)

    * [Male age 80 to 84](#male-age-80-to-84)

    * [Male age 85 and over](#male-age-85-and-over)

    * [Male Population](#male-population)

    * [Male under 5 years](#male-under-5-years)

    * [Men age 45 to 49](#men-age-45-to-49)

    * [Men age 50 to 54](#men-age-50-to-54)

    * [Men age 55 to 59](#men-age-55-to-59)

    * [Men age 60 to 61](#men-age-60-to-61)

    * [Men age 62 to 64](#men-age-62-to-64)


## <a name="children-under-18-years-of-age"></a><a name="us-census-acs-b09001001"></a>Children under 18 Years of Age

[<img alt="../../_images/us.census.acs.B09001001.png" src="../../_images/us.census.acs.B09001001.png" style="width: 100%;" />](../../_images/us.census.acs.B09001001.png)The number of people within each geography who are under 18 years of age.

Measure &quot;Children under 18 Years of Age&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B09001001'
        );

Measure &quot;Children under 18 Years of Age&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B09001001'
        );


## <a name="median-age"></a><a name="us-census-acs-b01002001"></a>Median Age

[<img alt="../../_images/us.census.acs.B01002001.png" src="../../_images/us.census.acs.B01002001.png" style="width: 100%;" />](../../_images/us.census.acs.B01002001.png)The median age of all people in a given geographic area.

Measure &quot;Median Age&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01002001'
        );

Median Age is only available for point lookups.


## <a name="men-age-45-to-64-middle-aged"></a><a name="us-census-acs-b15001027"></a>Men age 45 to 64 (&quot;middle aged&quot;)

[<img alt="../../_images/us.census.acs.B15001027.png" src="../../_images/us.census.acs.B15001027.png" style="width: 100%;" />](../../_images/us.census.acs.B15001027.png)The male population between the age of fourty-five years to sixty-four years within the specified area.

Measure &quot;Men age 45 to 64 (&quot;middle aged&quot;)&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B15001027'
        );

Measure &quot;Men age 45 to 64 (&quot;middle aged&quot;)&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B15001027'
        );


## <a name="population-1-year-and-over"></a><a name="us-census-acs-b07204001"></a>Population 1 year and over

[<img alt="../../_images/us.census.acs.B07204001.png" src="../../_images/us.census.acs.B07204001.png" style="width: 100%;" />](../../_images/us.census.acs.B07204001.png)All people, male and female, child and adult, living in a given geographic area that are 1 year and older.

Measure &quot;Population 1 year and over&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B07204001'
        );

Measure &quot;Population 1 year and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B07204001'
        );


## <a name="population-15-years-and-over"></a><a name="us-census-acs-b12005001"></a>Population 15 Years and Over

[<img alt="../../_images/us.census.acs.B12005001.png" src="../../_images/us.census.acs.B12005001.png" style="width: 100%;" />](../../_images/us.census.acs.B12005001.png)The number of people in a geographic area who are over the age of 15.  This is used mostly as a denominator of marital status.

Measure &quot;Population 15 Years and Over&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B12005001'
        );

Measure &quot;Population 15 Years and Over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B12005001'
        );


## <a name="population-age-16-and-over"></a><a name="us-census-acs-b23025001"></a>Population age 16 and over

[<img alt="../../_images/us.census.acs.B23025001.png" src="../../_images/us.census.acs.B23025001.png" style="width: 100%;" />](../../_images/us.census.acs.B23025001.png)The number of people in each geography who are age 16 or over.

Measure &quot;Population age 16 and over&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B23025001'
        );

Measure &quot;Population age 16 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B23025001'
        );


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

- [Female age 5 to 9](#female-age-5-to-9)

- [Female age 10 to 14](#female-age-10-to-14)

- [Female age 15 to 17](#female-age-15-to-17)

- [Female age 18 and 19](#female-age-18-and-19)

- [Female age 20](#female-age-20)

- [Female age 21](#female-age-21)

- [Female age 22 to 24](#female-age-22-to-24)

- [Female age 25 to 29](#female-age-25-to-29)

- [Female age 30 to 34](#female-age-30-to-34)

- [Female age 35 to 39](#female-age-35-to-39)

- [Female age 40 to 44](#female-age-40-to-44)

- [Female age 45 to 49](#female-age-45-to-49)

- [Female age 50 to 54](#female-age-50-to-54)

- [Female age 55 to 59](#female-age-55-to-59)

- [Female age 60 and 61](#female-age-60-and-61)

- [Female age 62 to 64](#female-age-62-to-64)

- [Female age 65 to 66](#female-age-65-to-66)

- [Female age 67 to 69](#female-age-67-to-69)

- [Female age 70 to 74](#female-age-70-to-74)

- [Female age 75 to 79](#female-age-75-to-79)

- [Female age 80 to 84](#female-age-80-to-84)

- [Female age 85 and over](#female-age-85-and-over)

- [Female Population](#female-population)

- [Female under 5 years](#female-under-5-years)

- [Male age 10 to 14](#male-age-10-to-14)

- [Male age 15 to 17](#male-age-15-to-17)

- [Male age 18 and 19](#male-age-18-and-19)

- [Male age 20](#male-age-20)

- [Male age 21](#male-age-21)

- [Male age 22 to 24](#male-age-22-to-24)

- [Male age 25 to 29](#male-age-25-to-29)

- [Male age 30 to 34](#male-age-30-to-34)

- [Male age 35 to 39](#male-age-35-to-39)

- [Male age 40 to 44](#male-age-40-to-44)

- [Male age 65 to 66](#male-age-65-to-66)

- [Male age 67 to 69](#male-age-67-to-69)

- [Male age 70 to 74](#male-age-70-to-74)

- [Male age 75 to 79](#male-age-75-to-79)

- [Male age 80 to 84](#male-age-80-to-84)

- [Male age 85 and over](#male-age-85-and-over)

- [Male Population](#male-population)

- [Male under 5 years](#male-under-5-years)

- [Men age 45 to 49](#men-age-45-to-49)

- [Men age 50 to 54](#men-age-50-to-54)

- [Men age 55 to 59](#men-age-55-to-59)

- [Men age 60 to 61](#men-age-60-to-61)

- [Men age 62 to 64](#men-age-62-to-64)


### <a name="female-age-5-to-9"></a><a name="us-census-acs-b01001028"></a>Female age 5 to 9

[<img alt="../../_images/us.census.acs.B01001028.png" src="../../_images/us.census.acs.B01001028.png" style="width: 100%;" />](../../_images/us.census.acs.B01001028.png)The female population between the age of five years to nine years within the specified area.

Measure &quot;Female age 5 to 9&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001028'
        );

Measure &quot;Female age 5 to 9&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001028'
        );

Measure &quot;Female age 5 to 9&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001028',
          'denominator'
        );

Measure &quot;Female age 5 to 9&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001028',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-10-to-14"></a><a name="us-census-acs-b01001029"></a>Female age 10 to 14

[<img alt="../../_images/us.census.acs.B01001029.png" src="../../_images/us.census.acs.B01001029.png" style="width: 100%;" />](../../_images/us.census.acs.B01001029.png)The female population between the age of ten years to fourteen years within the specified area.

Measure &quot;Female age 10 to 14&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001029'
        );

Measure &quot;Female age 10 to 14&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001029'
        );

Measure &quot;Female age 10 to 14&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001029',
          'denominator'
        );

Measure &quot;Female age 10 to 14&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001029',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-15-to-17"></a><a name="us-census-acs-b01001030"></a>Female age 15 to 17

[<img alt="../../_images/us.census.acs.B01001030.png" src="../../_images/us.census.acs.B01001030.png" style="width: 100%;" />](../../_images/us.census.acs.B01001030.png)The female population between the age of fifteeen years to seventeen years within the specified area.

Measure &quot;Female age 15 to 17&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001030'
        );

Measure &quot;Female age 15 to 17&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001030'
        );

Measure &quot;Female age 15 to 17&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001030',
          'denominator'
        );

Measure &quot;Female age 15 to 17&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001030',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-18-and-19"></a><a name="us-census-acs-b01001031"></a>Female age 18 and 19

[<img alt="../../_images/us.census.acs.B01001031.png" src="../../_images/us.census.acs.B01001031.png" style="width: 100%;" />](../../_images/us.census.acs.B01001031.png)The female population between the age of eighteen years to nineteen years within the specified area.

Measure &quot;Female age 18 and 19&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001031'
        );

Measure &quot;Female age 18 and 19&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001031'
        );

Measure &quot;Female age 18 and 19&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001031',
          'denominator'
        );

Measure &quot;Female age 18 and 19&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001031',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-20"></a><a name="us-census-acs-b01001032"></a>Female age 20

[<img alt="../../_images/us.census.acs.B01001032.png" src="../../_images/us.census.acs.B01001032.png" style="width: 100%;" />](../../_images/us.census.acs.B01001032.png)The female population with an age of twenty years within the specified area.

Measure &quot;Female age 20&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001032'
        );

Measure &quot;Female age 20&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001032'
        );

Measure &quot;Female age 20&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001032',
          'denominator'
        );

Measure &quot;Female age 20&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001032',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-21"></a><a name="us-census-acs-b01001033"></a>Female age 21

[<img alt="../../_images/us.census.acs.B01001033.png" src="../../_images/us.census.acs.B01001033.png" style="width: 100%;" />](../../_images/us.census.acs.B01001033.png)The female population with an age of twenty-one years within the specified area.

Measure &quot;Female age 21&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001033'
        );

Measure &quot;Female age 21&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001033'
        );

Measure &quot;Female age 21&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001033',
          'denominator'
        );

Measure &quot;Female age 21&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001033',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-22-to-24"></a><a name="us-census-acs-b01001034"></a>Female age 22 to 24

[<img alt="../../_images/us.census.acs.B01001034.png" src="../../_images/us.census.acs.B01001034.png" style="width: 100%;" />](../../_images/us.census.acs.B01001034.png)The female population between the age of twenty-two years to twenty-four years within the specified area.

Measure &quot;Female age 22 to 24&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001034'
        );

Measure &quot;Female age 22 to 24&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001034'
        );

Measure &quot;Female age 22 to 24&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001034',
          'denominator'
        );

Measure &quot;Female age 22 to 24&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001034',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-25-to-29"></a><a name="us-census-acs-b01001035"></a>Female age 25 to 29

[<img alt="../../_images/us.census.acs.B01001035.png" src="../../_images/us.census.acs.B01001035.png" style="width: 100%;" />](../../_images/us.census.acs.B01001035.png)The female population between the age of twenty-five years to twenty-nine years within the specified area.

Measure &quot;Female age 25 to 29&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001035'
        );

Measure &quot;Female age 25 to 29&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001035'
        );

Measure &quot;Female age 25 to 29&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001035',
          'denominator'
        );

Measure &quot;Female age 25 to 29&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001035',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-30-to-34"></a><a name="us-census-acs-b01001036"></a>Female age 30 to 34

[<img alt="../../_images/us.census.acs.B01001036.png" src="../../_images/us.census.acs.B01001036.png" style="width: 100%;" />](../../_images/us.census.acs.B01001036.png)The female population between the age of thirty years to thirty-four years within the specified area.

Measure &quot;Female age 30 to 34&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001036'
        );

Measure &quot;Female age 30 to 34&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001036'
        );

Measure &quot;Female age 30 to 34&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001036',
          'denominator'
        );

Measure &quot;Female age 30 to 34&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001036',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-35-to-39"></a><a name="us-census-acs-b01001037"></a>Female age 35 to 39

[<img alt="../../_images/us.census.acs.B01001037.png" src="../../_images/us.census.acs.B01001037.png" style="width: 100%;" />](../../_images/us.census.acs.B01001037.png)The female population between the age of thirty-five years to thirty-nine years within the specified area.

Measure &quot;Female age 35 to 39&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001037'
        );

Measure &quot;Female age 35 to 39&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001037'
        );

Measure &quot;Female age 35 to 39&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001037',
          'denominator'
        );

Measure &quot;Female age 35 to 39&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001037',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-40-to-44"></a><a name="us-census-acs-b01001038"></a>Female age 40 to 44

[<img alt="../../_images/us.census.acs.B01001038.png" src="../../_images/us.census.acs.B01001038.png" style="width: 100%;" />](../../_images/us.census.acs.B01001038.png)The female population between the age of fourty years to fourty-four years within the specified area.

Measure &quot;Female age 40 to 44&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001038'
        );

Measure &quot;Female age 40 to 44&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001038'
        );

Measure &quot;Female age 40 to 44&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001038',
          'denominator'
        );

Measure &quot;Female age 40 to 44&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001038',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-45-to-49"></a><a name="us-census-acs-b01001039"></a>Female age 45 to 49

[<img alt="../../_images/us.census.acs.B01001039.png" src="../../_images/us.census.acs.B01001039.png" style="width: 100%;" />](../../_images/us.census.acs.B01001039.png)The female population between the age of fourty-five years to fourty-nine years within the specified area.

Measure &quot;Female age 45 to 49&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001039'
        );

Measure &quot;Female age 45 to 49&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001039'
        );

Measure &quot;Female age 45 to 49&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001039',
          'denominator'
        );

Measure &quot;Female age 45 to 49&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001039',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-50-to-54"></a><a name="us-census-acs-b01001040"></a>Female age 50 to 54

[<img alt="../../_images/us.census.acs.B01001040.png" src="../../_images/us.census.acs.B01001040.png" style="width: 100%;" />](../../_images/us.census.acs.B01001040.png)The female population between the age of fifty years to fifty-four years within the specified area.

Measure &quot;Female age 50 to 54&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001040'
        );

Measure &quot;Female age 50 to 54&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001040'
        );

Measure &quot;Female age 50 to 54&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001040',
          'denominator'
        );

Measure &quot;Female age 50 to 54&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001040',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-55-to-59"></a><a name="us-census-acs-b01001041"></a>Female age 55 to 59

[<img alt="../../_images/us.census.acs.B01001041.png" src="../../_images/us.census.acs.B01001041.png" style="width: 100%;" />](../../_images/us.census.acs.B01001041.png)The female population between the age of fifty-five years to fifty-nine years within the specified area.

Measure &quot;Female age 55 to 59&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001041'
        );

Measure &quot;Female age 55 to 59&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001041'
        );

Measure &quot;Female age 55 to 59&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001041',
          'denominator'
        );

Measure &quot;Female age 55 to 59&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001041',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-60-and-61"></a><a name="us-census-acs-b01001042"></a>Female age 60 and 61

[<img alt="../../_images/us.census.acs.B01001042.png" src="../../_images/us.census.acs.B01001042.png" style="width: 100%;" />](../../_images/us.census.acs.B01001042.png)The female population between the age of sixty years to sixty-one years within the specified area.

Measure &quot;Female age 60 and 61&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001042'
        );

Measure &quot;Female age 60 and 61&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001042'
        );

Measure &quot;Female age 60 and 61&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001042',
          'denominator'
        );

Measure &quot;Female age 60 and 61&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001042',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-62-to-64"></a><a name="us-census-acs-b01001043"></a>Female age 62 to 64

[<img alt="../../_images/us.census.acs.B01001043.png" src="../../_images/us.census.acs.B01001043.png" style="width: 100%;" />](../../_images/us.census.acs.B01001043.png)The female population between the age of sixty-two years to sixty-four years within the specified area.

Measure &quot;Female age 62 to 64&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001043'
        );

Measure &quot;Female age 62 to 64&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001043'
        );

Measure &quot;Female age 62 to 64&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001043',
          'denominator'
        );

Measure &quot;Female age 62 to 64&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001043',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-65-to-66"></a><a name="us-census-acs-b01001044"></a>Female age 65 to 66

[<img alt="../../_images/us.census.acs.B01001044.png" src="../../_images/us.census.acs.B01001044.png" style="width: 100%;" />](../../_images/us.census.acs.B01001044.png)The female population between the age of sixty-five years to sixty-six years within the specified area.

Measure &quot;Female age 65 to 66&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001044'
        );

Measure &quot;Female age 65 to 66&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001044'
        );

Measure &quot;Female age 65 to 66&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001044',
          'denominator'
        );

Measure &quot;Female age 65 to 66&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001044',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-67-to-69"></a><a name="us-census-acs-b01001045"></a>Female age 67 to 69

[<img alt="../../_images/us.census.acs.B01001045.png" src="../../_images/us.census.acs.B01001045.png" style="width: 100%;" />](../../_images/us.census.acs.B01001045.png)The female population between the age of sixty-seven years to sixty-nine years within the specified area.

Measure &quot;Female age 67 to 69&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001045'
        );

Measure &quot;Female age 67 to 69&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001045'
        );

Measure &quot;Female age 67 to 69&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001045',
          'denominator'
        );

Measure &quot;Female age 67 to 69&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001045',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-70-to-74"></a><a name="us-census-acs-b01001046"></a>Female age 70 to 74

[<img alt="../../_images/us.census.acs.B01001046.png" src="../../_images/us.census.acs.B01001046.png" style="width: 100%;" />](../../_images/us.census.acs.B01001046.png)The female population between the age of seventy years to seventy-four years within the specified area.

Measure &quot;Female age 70 to 74&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001046'
        );

Measure &quot;Female age 70 to 74&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001046'
        );

Measure &quot;Female age 70 to 74&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001046',
          'denominator'
        );

Measure &quot;Female age 70 to 74&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001046',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-75-to-79"></a><a name="us-census-acs-b01001047"></a>Female age 75 to 79

[<img alt="../../_images/us.census.acs.B01001047.png" src="../../_images/us.census.acs.B01001047.png" style="width: 100%;" />](../../_images/us.census.acs.B01001047.png)The female population between the age of seventy-five years to seventy-nine years within the specified area.

Measure &quot;Female age 75 to 79&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001047'
        );

Measure &quot;Female age 75 to 79&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001047'
        );

Measure &quot;Female age 75 to 79&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001047',
          'denominator'
        );

Measure &quot;Female age 75 to 79&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001047',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-80-to-84"></a><a name="us-census-acs-b01001048"></a>Female age 80 to 84

[<img alt="../../_images/us.census.acs.B01001048.png" src="../../_images/us.census.acs.B01001048.png" style="width: 100%;" />](../../_images/us.census.acs.B01001048.png)The female population between the age of eighty years to eighty-four years within the specified area.

Measure &quot;Female age 80 to 84&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001048'
        );

Measure &quot;Female age 80 to 84&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001048'
        );

Measure &quot;Female age 80 to 84&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001048',
          'denominator'
        );

Measure &quot;Female age 80 to 84&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001048',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-age-85-and-over"></a><a name="us-census-acs-b01001049"></a>Female age 85 and over

[<img alt="../../_images/us.census.acs.B01001049.png" src="../../_images/us.census.acs.B01001049.png" style="width: 100%;" />](../../_images/us.census.acs.B01001049.png)The female population of the age of eighty-five years and over within the specified area.

Measure &quot;Female age 85 and over&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001049'
        );

Measure &quot;Female age 85 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001049'
        );

Measure &quot;Female age 85 and over&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001049',
          'denominator'
        );

Measure &quot;Female age 85 and over&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001049',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-population"></a><a name="us-census-acs-b01001026"></a>Female Population

[<img alt="../../_images/us.census.acs.B01001026.png" src="../../_images/us.census.acs.B01001026.png" style="width: 100%;" />](../../_images/us.census.acs.B01001026.png)The number of people within each geography who are female.

Measure &quot;Female Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001026'
        );

Measure &quot;Female Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001026'
        );

Measure &quot;Female Population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001026',
          'denominator'
        );

Measure &quot;Female Population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001026',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="female-under-5-years"></a><a name="us-census-acs-b01001027"></a>Female under 5 years

[<img alt="../../_images/us.census.acs.B01001027.png" src="../../_images/us.census.acs.B01001027.png" style="width: 100%;" />](../../_images/us.census.acs.B01001027.png)The female population over the age of five years within the specified area.

Measure &quot;Female under 5 years&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001027'
        );

Measure &quot;Female under 5 years&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001027'
        );

Measure &quot;Female under 5 years&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001027',
          'denominator'
        );

Measure &quot;Female under 5 years&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001027',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-10-to-14"></a><a name="us-census-acs-b01001004"></a>Male age 10 to 14

[<img alt="../../_images/us.census.acs.B01001004.png" src="../../_images/us.census.acs.B01001004.png" style="width: 100%;" />](../../_images/us.census.acs.B01001004.png)The male population between the age of ten years to fourteen years within the specified area.

Measure &quot;Male age 10 to 14&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001004'
        );

Measure &quot;Male age 10 to 14&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001004'
        );

Measure &quot;Male age 10 to 14&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001004',
          'denominator'
        );

Measure &quot;Male age 10 to 14&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001004',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-15-to-17"></a><a name="us-census-acs-b01001006"></a>Male age 15 to 17

[<img alt="../../_images/us.census.acs.B01001006.png" src="../../_images/us.census.acs.B01001006.png" style="width: 100%;" />](../../_images/us.census.acs.B01001006.png)The male population between the age of fifteeen years to seventeen years within the specified area.

Measure &quot;Male age 15 to 17&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001006'
        );

Measure &quot;Male age 15 to 17&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001006'
        );

Measure &quot;Male age 15 to 17&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001006',
          'denominator'
        );

Measure &quot;Male age 15 to 17&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001006',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-18-and-19"></a><a name="us-census-acs-b01001007"></a>Male age 18 and 19

[<img alt="../../_images/us.census.acs.B01001007.png" src="../../_images/us.census.acs.B01001007.png" style="width: 100%;" />](../../_images/us.census.acs.B01001007.png)The male population between the age of eighteen years to nineteen years within the specified area.

Measure &quot;Male age 18 and 19&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001007'
        );

Measure &quot;Male age 18 and 19&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001007'
        );

Measure &quot;Male age 18 and 19&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001007',
          'denominator'
        );

Measure &quot;Male age 18 and 19&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001007',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-20"></a><a name="us-census-acs-b01001008"></a>Male age 20

[<img alt="../../_images/us.census.acs.B01001008.png" src="../../_images/us.census.acs.B01001008.png" style="width: 100%;" />](../../_images/us.census.acs.B01001008.png)The male population with an age of twenty years within the specified area.

Measure &quot;Male age 20&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001008'
        );

Measure &quot;Male age 20&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001008'
        );

Measure &quot;Male age 20&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001008',
          'denominator'
        );

Measure &quot;Male age 20&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001008',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-21"></a><a name="us-census-acs-b01001009"></a>Male age 21

[<img alt="../../_images/us.census.acs.B01001009.png" src="../../_images/us.census.acs.B01001009.png" style="width: 100%;" />](../../_images/us.census.acs.B01001009.png)The male population with an age of twenty-one years within the specified area.

Measure &quot;Male age 21&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001009'
        );

Measure &quot;Male age 21&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001009'
        );

Measure &quot;Male age 21&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001009',
          'denominator'
        );

Measure &quot;Male age 21&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001009',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-22-to-24"></a><a name="us-census-acs-b01001010"></a>Male age 22 to 24

[<img alt="../../_images/us.census.acs.B01001010.png" src="../../_images/us.census.acs.B01001010.png" style="width: 100%;" />](../../_images/us.census.acs.B01001010.png)The male population between the age of twenty-two years to twenty-four years within the specified area.

Measure &quot;Male age 22 to 24&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001010'
        );

Measure &quot;Male age 22 to 24&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001010'
        );

Measure &quot;Male age 22 to 24&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001010',
          'denominator'
        );

Measure &quot;Male age 22 to 24&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001010',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-25-to-29"></a><a name="us-census-acs-b01001011"></a>Male age 25 to 29

[<img alt="../../_images/us.census.acs.B01001011.png" src="../../_images/us.census.acs.B01001011.png" style="width: 100%;" />](../../_images/us.census.acs.B01001011.png)The male population between the age of twenty-five years to twenty-nine years within the specified area.

Measure &quot;Male age 25 to 29&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001011'
        );

Measure &quot;Male age 25 to 29&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001011'
        );

Measure &quot;Male age 25 to 29&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001011',
          'denominator'
        );

Measure &quot;Male age 25 to 29&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001011',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-30-to-34"></a><a name="us-census-acs-b01001012"></a>Male age 30 to 34

[<img alt="../../_images/us.census.acs.B01001012.png" src="../../_images/us.census.acs.B01001012.png" style="width: 100%;" />](../../_images/us.census.acs.B01001012.png)The male population between the age of thirty years to thirty-four years within the specified area.

Measure &quot;Male age 30 to 34&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001012'
        );

Measure &quot;Male age 30 to 34&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001012'
        );

Measure &quot;Male age 30 to 34&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001012',
          'denominator'
        );

Measure &quot;Male age 30 to 34&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001012',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-35-to-39"></a><a name="us-census-acs-b01001013"></a>Male age 35 to 39

[<img alt="../../_images/us.census.acs.B01001013.png" src="../../_images/us.census.acs.B01001013.png" style="width: 100%;" />](../../_images/us.census.acs.B01001013.png)The male population between the age of thirty-five years to thirty-nine years within the specified area.

Measure &quot;Male age 35 to 39&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001013'
        );

Measure &quot;Male age 35 to 39&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001013'
        );

Measure &quot;Male age 35 to 39&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001013',
          'denominator'
        );

Measure &quot;Male age 35 to 39&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001013',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-40-to-44"></a><a name="us-census-acs-b01001014"></a>Male age 40 to 44

[<img alt="../../_images/us.census.acs.B01001014.png" src="../../_images/us.census.acs.B01001014.png" style="width: 100%;" />](../../_images/us.census.acs.B01001014.png)The male population between the age of fourty years to fourty-four years within the specified area.

Measure &quot;Male age 40 to 44&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001014'
        );

Measure &quot;Male age 40 to 44&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001014'
        );

Measure &quot;Male age 40 to 44&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001014',
          'denominator'
        );

Measure &quot;Male age 40 to 44&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001014',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-65-to-66"></a><a name="us-census-acs-b01001020"></a>Male age 65 to 66

[<img alt="../../_images/us.census.acs.B01001020.png" src="../../_images/us.census.acs.B01001020.png" style="width: 100%;" />](../../_images/us.census.acs.B01001020.png)The male population between the age of sixty-five years to sixty-six years within the specified area.

Measure &quot;Male age 65 to 66&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001020'
        );

Measure &quot;Male age 65 to 66&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001020'
        );

Measure &quot;Male age 65 to 66&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001020',
          'denominator'
        );

Measure &quot;Male age 65 to 66&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001020',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-67-to-69"></a><a name="us-census-acs-b01001021"></a>Male age 67 to 69

[<img alt="../../_images/us.census.acs.B01001021.png" src="../../_images/us.census.acs.B01001021.png" style="width: 100%;" />](../../_images/us.census.acs.B01001021.png)The male population between the age of sixty-seven years to sixty-nine years within the specified area.

Measure &quot;Male age 67 to 69&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001021'
        );

Measure &quot;Male age 67 to 69&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001021'
        );

Measure &quot;Male age 67 to 69&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001021',
          'denominator'
        );

Measure &quot;Male age 67 to 69&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001021',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-70-to-74"></a><a name="us-census-acs-b01001022"></a>Male age 70 to 74

[<img alt="../../_images/us.census.acs.B01001022.png" src="../../_images/us.census.acs.B01001022.png" style="width: 100%;" />](../../_images/us.census.acs.B01001022.png)The male population between the age of seventy years to seventy-four years within the specified area.

Measure &quot;Male age 70 to 74&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001022'
        );

Measure &quot;Male age 70 to 74&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001022'
        );

Measure &quot;Male age 70 to 74&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001022',
          'denominator'
        );

Measure &quot;Male age 70 to 74&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001022',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-75-to-79"></a><a name="us-census-acs-b01001023"></a>Male age 75 to 79

[<img alt="../../_images/us.census.acs.B01001023.png" src="../../_images/us.census.acs.B01001023.png" style="width: 100%;" />](../../_images/us.census.acs.B01001023.png)The male population between the age of seventy-five years to seventy-nine years within the specified area.

Measure &quot;Male age 75 to 79&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001023'
        );

Measure &quot;Male age 75 to 79&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001023'
        );

Measure &quot;Male age 75 to 79&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001023',
          'denominator'
        );

Measure &quot;Male age 75 to 79&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001023',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-80-to-84"></a><a name="us-census-acs-b01001024"></a>Male age 80 to 84

[<img alt="../../_images/us.census.acs.B01001024.png" src="../../_images/us.census.acs.B01001024.png" style="width: 100%;" />](../../_images/us.census.acs.B01001024.png)The male population between the age of eighty years to eighty-four years within the specified area.

Measure &quot;Male age 80 to 84&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001024'
        );

Measure &quot;Male age 80 to 84&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001024'
        );

Measure &quot;Male age 80 to 84&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001024',
          'denominator'
        );

Measure &quot;Male age 80 to 84&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001024',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-age-85-and-over"></a><a name="us-census-acs-b01001025"></a>Male age 85 and over

[<img alt="../../_images/us.census.acs.B01001025.png" src="../../_images/us.census.acs.B01001025.png" style="width: 100%;" />](../../_images/us.census.acs.B01001025.png)The male population of the age of eighty-five years and over within the specified area.

Measure &quot;Male age 85 and over&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001025'
        );

Measure &quot;Male age 85 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001025'
        );

Measure &quot;Male age 85 and over&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001025',
          'denominator'
        );

Measure &quot;Male age 85 and over&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001025',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-population"></a><a name="us-census-acs-b01001002"></a>Male Population

[<img alt="../../_images/us.census.acs.B01001002.png" src="../../_images/us.census.acs.B01001002.png" style="width: 100%;" />](../../_images/us.census.acs.B01001002.png)The number of people within each geography who are male.

Measure &quot;Male Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001002'
        );

Measure &quot;Male Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001002'
        );

Measure &quot;Male Population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001002',
          'denominator'
        );

Measure &quot;Male Population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001002',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="male-under-5-years"></a><a name="us-census-acs-b01001003"></a>Male under 5 years

[<img alt="../../_images/us.census.acs.B01001003.png" src="../../_images/us.census.acs.B01001003.png" style="width: 100%;" />](../../_images/us.census.acs.B01001003.png)The male population over the age of five years within the specified area.

Measure &quot;Male under 5 years&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001003'
        );

Measure &quot;Male under 5 years&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001003'
        );

Measure &quot;Male under 5 years&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001003',
          'denominator'
        );

Measure &quot;Male under 5 years&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001003',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="men-age-45-to-49"></a><a name="us-census-acs-b01001015"></a>Men age 45 to 49

[<img alt="../../_images/us.census.acs.B01001015.png" src="../../_images/us.census.acs.B01001015.png" style="width: 100%;" />](../../_images/us.census.acs.B01001015.png)The male population between the age of fourty-five years to fourty-nine years within the specified area.

Measure &quot;Men age 45 to 49&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001015'
        );

Measure &quot;Men age 45 to 49&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001015'
        );

Measure &quot;Men age 45 to 49&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001015',
          'denominator'
        );

Measure &quot;Men age 45 to 49&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001015',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="men-age-50-to-54"></a><a name="us-census-acs-b01001016"></a>Men age 50 to 54

[<img alt="../../_images/us.census.acs.B01001016.png" src="../../_images/us.census.acs.B01001016.png" style="width: 100%;" />](../../_images/us.census.acs.B01001016.png)The male population between the age of fifty years to fifty-four years within the specified area.

Measure &quot;Men age 50 to 54&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001016'
        );

Measure &quot;Men age 50 to 54&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001016'
        );

Measure &quot;Men age 50 to 54&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001016',
          'denominator'
        );

Measure &quot;Men age 50 to 54&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001016',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="men-age-55-to-59"></a><a name="us-census-acs-b01001017"></a>Men age 55 to 59

[<img alt="../../_images/us.census.acs.B01001017.png" src="../../_images/us.census.acs.B01001017.png" style="width: 100%;" />](../../_images/us.census.acs.B01001017.png)The male population between the age of fifty-five years to fifty-nine years within the specified area.

Measure &quot;Men age 55 to 59&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001017'
        );

Measure &quot;Men age 55 to 59&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001017'
        );

Measure &quot;Men age 55 to 59&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001017',
          'denominator'
        );

Measure &quot;Men age 55 to 59&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001017',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="men-age-60-to-61"></a><a name="us-census-acs-b01001018"></a>Men age 60 to 61

[<img alt="../../_images/us.census.acs.B01001018.png" src="../../_images/us.census.acs.B01001018.png" style="width: 100%;" />](../../_images/us.census.acs.B01001018.png)The male population between the age of sixty years to sixty-one years within the specified area.

Measure &quot;Men age 60 to 61&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001018'
        );

Measure &quot;Men age 60 to 61&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001018'
        );

Measure &quot;Men age 60 to 61&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001018',
          'denominator'
        );

Measure &quot;Men age 60 to 61&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001018',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


### <a name="men-age-62-to-64"></a><a name="us-census-acs-b01001019"></a>Men age 62 to 64

[<img alt="../../_images/us.census.acs.B01001019.png" src="../../_images/us.census.acs.B01001019.png" style="width: 100%;" />](../../_images/us.census.acs.B01001019.png)The male population between the age of sixty-two years to sixty-four years within the specified area.

Measure &quot;Men age 62 to 64&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001019'
        );

Measure &quot;Men age 62 to 64&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001019'
        );

Measure &quot;Men age 62 to 64&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B01001019',
          'denominator'
        );

Measure &quot;Men age 62 to 64&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B01001019',
          'denominator'
        );

* denominator: [Total Population](../race_ethnicity/#us-census-acs-b01003001)


