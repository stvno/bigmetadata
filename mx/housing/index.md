

  
# <a name="housing"></a>Housing

What type of housing exists and how do people live in it?

- [Average number of people per private occupied dwellings](#average-number-of-people-per-private-occupied-dwellings)

- [Average number of people per room in private occupied dwellings](#average-number-of-people-per-room-in-private-occupied-dwellings)

- [Dwellings](#dwellings)

    * [Occupied dwellings](#occupied-dwellings)

        - [Occupied private dwellings](#occupied-private-dwellings)

        - [One room private occupied dwellings](#one-room-private-occupied-dwellings)

        - [Private occupied dwellings with 2.5 people per bedroom or more](#private-occupied-dwellings-with-2-5-people-per-bedroom-or-more)

        - [Private occupied dwellings with 3 people per bedroom or more](#private-occupied-dwellings-with-3-people-per-bedroom-or-more)

        - [Private occupied dwellings with a bathroom or toilet](#private-occupied-dwellings-with-a-bathroom-or-toilet)

        - [Private occupied dwellings with a car or van](#private-occupied-dwellings-with-a-car-or-van)

        - [Private occupied dwellings with a cellphone](#private-occupied-dwellings-with-a-cellphone)

        - [Private occupied dwellings with a computer](#private-occupied-dwellings-with-a-computer)

        - [Private occupied dwellings with a landline](#private-occupied-dwellings-with-a-landline)

        - [Private occupied dwellings with a radio](#private-occupied-dwellings-with-a-radio)

        - [Private occupied dwellings with a refrigator](#private-occupied-dwellings-with-a-refrigator)

        - [Private occupied dwellings with a television](#private-occupied-dwellings-with-a-television)

        - [Private occupied dwellings with a washer](#private-occupied-dwellings-with-a-washer)

        - [Private occupied dwellings with drainage](#private-occupied-dwellings-with-drainage)

        - [Private occupied dwellings with earthen floor](#private-occupied-dwellings-with-earthen-floor)

        - [Private occupied dwellings with electric light](#private-occupied-dwellings-with-electric-light)

        - [Private occupied dwellings with electric light, running water and drainage](#private-occupied-dwellings-with-electric-light-running-water-and-drainage)

        - [Private occupied dwellings with Internet access](#private-occupied-dwellings-with-internet-access)

        - [Private occupied dwellings with neither a computer nor Internet access](#private-occupied-dwellings-with-neither-a-computer-nor-internet-access)

        - [Private occupied dwellings with neither a landline nor a cellphone](#private-occupied-dwellings-with-neither-a-landline-nor-a-cellphone)

        - [Private occupied dwellings with neither electric light, running water nor drainage](#private-occupied-dwellings-with-neither-electric-light-running-water-nor-drainage)

        - [Private occupied dwellings with neither refigerator nor washer](#private-occupied-dwellings-with-neither-refigerator-nor-washer)

        - [Private occupied dwellings with neither refigerator, washer, nor car or van](#private-occupied-dwellings-with-neither-refigerator-washer-nor-car-or-van)

        - [Private occupied dwellings with neither television nor radio](#private-occupied-dwellings-with-neither-television-nor-radio)

        - [Private occupied dwellings with no physical property](#private-occupied-dwellings-with-no-physical-property)

        - [Private occupied dwellings with one bedroom](#private-occupied-dwellings-with-one-bedroom)

        - [Private occupied dwellings with running water](#private-occupied-dwellings-with-running-water)

        - [Private occupied dwellings with two bedrooms or more](#private-occupied-dwellings-with-two-bedrooms-or-more)

        - [Private occupied dwellings without a bathroom or toilet](#private-occupied-dwellings-without-a-bathroom-or-toilet)

        - [Private occupied dwellings without any information or communication technology](#private-occupied-dwellings-without-any-information-or-communication-technology)

        - [Private occupied dwellings without drainage](#private-occupied-dwellings-without-drainage)

        - [Private occupied dwellings without electric light](#private-occupied-dwellings-without-electric-light)

        - [Private occupied dwellings without running water](#private-occupied-dwellings-without-running-water)

        - [Three or more room private occupied dwellings](#three-or-more-room-private-occupied-dwellings)

        - [Two room private occupied dwellings](#two-room-private-occupied-dwellings)

- [Population in occupied private dwellings](#population-in-occupied-private-dwellings)

- [Population in private occupied dwellings with a bathroom or toilet](#population-in-private-occupied-dwellings-with-a-bathroom-or-toilet)

- [Population in private occupied dwellings with running water](#population-in-private-occupied-dwellings-with-running-water)


## <a name="average-number-of-people-per-private-occupied-dwellings"></a><a name="mx-inegi-columns-viv4-r"></a>Average number of people per private occupied dwellings

[<img alt="../../_images/mx.inegi_columns.VIV4_R.png" src="../../_images/mx.inegi_columns.VIV4_R.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV4_R.png)Measure &quot;Average number of people per private occupied dwellings&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV4_R'
        );

Average number of people per private occupied dwellings is only available for point lookups.


## <a name="average-number-of-people-per-room-in-private-occupied-dwellings"></a><a name="mx-inegi-columns-viv5-r"></a>Average number of people per room in private occupied dwellings

[<img alt="../../_images/mx.inegi_columns.VIV5_R.png" src="../../_images/mx.inegi_columns.VIV5_R.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV5_R.png)Measure &quot;Average number of people per room in private occupied dwellings&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV5_R'
        );

Average number of people per room in private occupied dwellings is only available for point lookups.


## <a name="dwellings"></a><a name="mx-inegi-columns-viv0"></a>Dwellings

[<img alt="../../_images/mx.inegi_columns.VIV0.png" src="../../_images/mx.inegi_columns.VIV0.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV0.png)Measure &quot;Dwellings&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV0'
        );

Measure &quot;Dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV0'
        );

Subcolumns of Dwellings

- [Occupied dwellings](#occupied-dwellings)


### <a name="occupied-dwellings"></a><a name="mx-inegi-columns-viv1"></a>Occupied dwellings

[<img alt="../../_images/mx.inegi_columns.VIV1.png" src="../../_images/mx.inegi_columns.VIV1.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV1.png)Measure &quot;Occupied dwellings&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV1'
        );

Measure &quot;Occupied dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV1'
        );

Measure &quot;Occupied dwellings&quot; percent of &quot;Dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV1',
          'denominator'
        );

Measure &quot;Occupied dwellings&quot; percent of &quot;Dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV1',
          'denominator'
        );

* denominator: [Dwellings](#mx-inegi-columns-viv0)

Subcolumns of Occupied dwellings

- [Occupied private dwellings](#occupied-private-dwellings)

- [One room private occupied dwellings](#one-room-private-occupied-dwellings)

- [Private occupied dwellings with 2.5 people per bedroom or more](#private-occupied-dwellings-with-2-5-people-per-bedroom-or-more)

- [Private occupied dwellings with 3 people per bedroom or more](#private-occupied-dwellings-with-3-people-per-bedroom-or-more)

- [Private occupied dwellings with a bathroom or toilet](#private-occupied-dwellings-with-a-bathroom-or-toilet)

- [Private occupied dwellings with a car or van](#private-occupied-dwellings-with-a-car-or-van)

- [Private occupied dwellings with a cellphone](#private-occupied-dwellings-with-a-cellphone)

- [Private occupied dwellings with a computer](#private-occupied-dwellings-with-a-computer)

- [Private occupied dwellings with a landline](#private-occupied-dwellings-with-a-landline)

- [Private occupied dwellings with a radio](#private-occupied-dwellings-with-a-radio)

- [Private occupied dwellings with a refrigator](#private-occupied-dwellings-with-a-refrigator)

- [Private occupied dwellings with a television](#private-occupied-dwellings-with-a-television)

- [Private occupied dwellings with a washer](#private-occupied-dwellings-with-a-washer)

- [Private occupied dwellings with drainage](#private-occupied-dwellings-with-drainage)

- [Private occupied dwellings with earthen floor](#private-occupied-dwellings-with-earthen-floor)

- [Private occupied dwellings with electric light](#private-occupied-dwellings-with-electric-light)

- [Private occupied dwellings with electric light, running water and drainage](#private-occupied-dwellings-with-electric-light-running-water-and-drainage)

- [Private occupied dwellings with Internet access](#private-occupied-dwellings-with-internet-access)

- [Private occupied dwellings with neither a computer nor Internet access](#private-occupied-dwellings-with-neither-a-computer-nor-internet-access)

- [Private occupied dwellings with neither a landline nor a cellphone](#private-occupied-dwellings-with-neither-a-landline-nor-a-cellphone)

- [Private occupied dwellings with neither electric light, running water nor drainage](#private-occupied-dwellings-with-neither-electric-light-running-water-nor-drainage)

- [Private occupied dwellings with neither refigerator nor washer](#private-occupied-dwellings-with-neither-refigerator-nor-washer)

- [Private occupied dwellings with neither refigerator, washer, nor car or van](#private-occupied-dwellings-with-neither-refigerator-washer-nor-car-or-van)

- [Private occupied dwellings with neither television nor radio](#private-occupied-dwellings-with-neither-television-nor-radio)

- [Private occupied dwellings with no physical property](#private-occupied-dwellings-with-no-physical-property)

- [Private occupied dwellings with one bedroom](#private-occupied-dwellings-with-one-bedroom)

- [Private occupied dwellings with running water](#private-occupied-dwellings-with-running-water)

- [Private occupied dwellings with two bedrooms or more](#private-occupied-dwellings-with-two-bedrooms-or-more)

- [Private occupied dwellings without a bathroom or toilet](#private-occupied-dwellings-without-a-bathroom-or-toilet)

- [Private occupied dwellings without any information or communication technology](#private-occupied-dwellings-without-any-information-or-communication-technology)

- [Private occupied dwellings without drainage](#private-occupied-dwellings-without-drainage)

- [Private occupied dwellings without electric light](#private-occupied-dwellings-without-electric-light)

- [Private occupied dwellings without running water](#private-occupied-dwellings-without-running-water)

- [Three or more room private occupied dwellings](#three-or-more-room-private-occupied-dwellings)

- [Two room private occupied dwellings](#two-room-private-occupied-dwellings)


#### <a name="occupied-private-dwellings"></a><a name="mx-inegi-columns-viv2"></a>Occupied private dwellings

[<img alt="../../_images/mx.inegi_columns.VIV2.png" src="../../_images/mx.inegi_columns.VIV2.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV2.png)Measure &quot;Occupied private dwellings&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV2'
        );

Measure &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV2'
        );

Measure &quot;Occupied private dwellings&quot; percent of &quot;Occupied dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV2',
          'denominator'
        );

Measure &quot;Occupied private dwellings&quot; percent of &quot;Occupied dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV2',
          'denominator'
        );

* denominator: [Occupied dwellings](#mx-inegi-columns-viv1)


#### <a name="one-room-private-occupied-dwellings"></a><a name="mx-inegi-columns-viv10"></a>One room private occupied dwellings

[<img alt="../../_images/mx.inegi_columns.VIV10.png" src="../../_images/mx.inegi_columns.VIV10.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV10.png)Measure &quot;One room private occupied dwellings&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV10'
        );

Measure &quot;One room private occupied dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV10'
        );

Measure &quot;One room private occupied dwellings&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV10',
          'denominator'
        );

Measure &quot;One room private occupied dwellings&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV10',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-2-5-people-per-bedroom-or-more"></a><a name="mx-inegi-columns-viv9"></a>Private occupied dwellings with 2.5 people per bedroom or more

[<img alt="../../_images/mx.inegi_columns.VIV9.png" src="../../_images/mx.inegi_columns.VIV9.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV9.png)Measure &quot;Private occupied dwellings with 2.5 people per bedroom or more&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV9'
        );

Measure &quot;Private occupied dwellings with 2.5 people per bedroom or more&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV9'
        );

Measure &quot;Private occupied dwellings with 2.5 people per bedroom or more&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV9',
          'denominator'
        );

Measure &quot;Private occupied dwellings with 2.5 people per bedroom or more&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV9',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-3-people-per-bedroom-or-more"></a><a name="mx-inegi-columns-viv13"></a>Private occupied dwellings with 3 people per bedroom or more

[<img alt="../../_images/mx.inegi_columns.VIV13.png" src="../../_images/mx.inegi_columns.VIV13.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV13.png)Measure &quot;Private occupied dwellings with 3 people per bedroom or more&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV13'
        );

Measure &quot;Private occupied dwellings with 3 people per bedroom or more&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV13'
        );

Measure &quot;Private occupied dwellings with 3 people per bedroom or more&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV13',
          'denominator'
        );

Measure &quot;Private occupied dwellings with 3 people per bedroom or more&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV13',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-a-bathroom-or-toilet"></a><a name="mx-inegi-columns-viv19"></a>Private occupied dwellings with a bathroom or toilet

[<img alt="../../_images/mx.inegi_columns.VIV19.png" src="../../_images/mx.inegi_columns.VIV19.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV19.png)Measure &quot;Private occupied dwellings with a bathroom or toilet&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV19'
        );

Measure &quot;Private occupied dwellings with a bathroom or toilet&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV19'
        );

Measure &quot;Private occupied dwellings with a bathroom or toilet&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV19',
          'denominator'
        );

Measure &quot;Private occupied dwellings with a bathroom or toilet&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV19',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-a-car-or-van"></a><a name="mx-inegi-columns-viv28"></a>Private occupied dwellings with a car or van

[<img alt="../../_images/mx.inegi_columns.VIV28.png" src="../../_images/mx.inegi_columns.VIV28.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV28.png)Measure &quot;Private occupied dwellings with a car or van&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV28'
        );

Measure &quot;Private occupied dwellings with a car or van&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV28'
        );

Measure &quot;Private occupied dwellings with a car or van&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV28',
          'denominator'
        );

Measure &quot;Private occupied dwellings with a car or van&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV28',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-a-cellphone"></a><a name="mx-inegi-columns-viv35"></a>Private occupied dwellings with a cellphone

[<img alt="../../_images/mx.inegi_columns.VIV35.png" src="../../_images/mx.inegi_columns.VIV35.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV35.png)Measure &quot;Private occupied dwellings with a cellphone&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV35'
        );

Measure &quot;Private occupied dwellings with a cellphone&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV35'
        );

Measure &quot;Private occupied dwellings with a cellphone&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV35',
          'denominator'
        );

Measure &quot;Private occupied dwellings with a cellphone&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV35',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-a-computer"></a><a name="mx-inegi-columns-viv33"></a>Private occupied dwellings with a computer

[<img alt="../../_images/mx.inegi_columns.VIV33.png" src="../../_images/mx.inegi_columns.VIV33.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV33.png)Measure &quot;Private occupied dwellings with a computer&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV33'
        );

Measure &quot;Private occupied dwellings with a computer&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV33'
        );

Measure &quot;Private occupied dwellings with a computer&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV33',
          'denominator'
        );

Measure &quot;Private occupied dwellings with a computer&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV33',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-a-landline"></a><a name="mx-inegi-columns-viv34"></a>Private occupied dwellings with a landline

[<img alt="../../_images/mx.inegi_columns.VIV34.png" src="../../_images/mx.inegi_columns.VIV34.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV34.png)Measure &quot;Private occupied dwellings with a landline&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV34'
        );

Measure &quot;Private occupied dwellings with a landline&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV34'
        );

Measure &quot;Private occupied dwellings with a landline&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV34',
          'denominator'
        );

Measure &quot;Private occupied dwellings with a landline&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV34',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-a-radio"></a><a name="mx-inegi-columns-viv31"></a>Private occupied dwellings with a radio

[<img alt="../../_images/mx.inegi_columns.VIV31.png" src="../../_images/mx.inegi_columns.VIV31.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV31.png)Measure &quot;Private occupied dwellings with a radio&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV31'
        );

Measure &quot;Private occupied dwellings with a radio&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV31'
        );

Measure &quot;Private occupied dwellings with a radio&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV31',
          'denominator'
        );

Measure &quot;Private occupied dwellings with a radio&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV31',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-a-refrigator"></a><a name="mx-inegi-columns-viv26"></a>Private occupied dwellings with a refrigator

[<img alt="../../_images/mx.inegi_columns.VIV26.png" src="../../_images/mx.inegi_columns.VIV26.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV26.png)Measure &quot;Private occupied dwellings with a refrigator&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV26'
        );

Measure &quot;Private occupied dwellings with a refrigator&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV26'
        );

Measure &quot;Private occupied dwellings with a refrigator&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV26',
          'denominator'
        );

Measure &quot;Private occupied dwellings with a refrigator&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV26',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-a-television"></a><a name="mx-inegi-columns-viv32"></a>Private occupied dwellings with a television

[<img alt="../../_images/mx.inegi_columns.VIV32.png" src="../../_images/mx.inegi_columns.VIV32.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV32.png)Measure &quot;Private occupied dwellings with a television&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV32'
        );

Measure &quot;Private occupied dwellings with a television&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV32'
        );

Measure &quot;Private occupied dwellings with a television&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV32',
          'denominator'
        );

Measure &quot;Private occupied dwellings with a television&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV32',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-a-washer"></a><a name="mx-inegi-columns-viv27"></a>Private occupied dwellings with a washer

[<img alt="../../_images/mx.inegi_columns.VIV27.png" src="../../_images/mx.inegi_columns.VIV27.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV27.png)Measure &quot;Private occupied dwellings with a washer&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV27'
        );

Measure &quot;Private occupied dwellings with a washer&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV27'
        );

Measure &quot;Private occupied dwellings with a washer&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV27',
          'denominator'
        );

Measure &quot;Private occupied dwellings with a washer&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV27',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-drainage"></a><a name="mx-inegi-columns-viv22"></a>Private occupied dwellings with drainage

[<img alt="../../_images/mx.inegi_columns.VIV22.png" src="../../_images/mx.inegi_columns.VIV22.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV22.png)Measure &quot;Private occupied dwellings with drainage&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV22'
        );

Measure &quot;Private occupied dwellings with drainage&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV22'
        );

Measure &quot;Private occupied dwellings with drainage&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV22',
          'denominator'
        );

Measure &quot;Private occupied dwellings with drainage&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV22',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-earthen-floor"></a><a name="mx-inegi-columns-viv6"></a>Private occupied dwellings with earthen floor

[<img alt="../../_images/mx.inegi_columns.VIV6.png" src="../../_images/mx.inegi_columns.VIV6.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV6.png)Measure &quot;Private occupied dwellings with earthen floor&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV6'
        );

Measure &quot;Private occupied dwellings with earthen floor&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV6'
        );

Measure &quot;Private occupied dwellings with earthen floor&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV6',
          'denominator'
        );

Measure &quot;Private occupied dwellings with earthen floor&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV6',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-electric-light"></a><a name="mx-inegi-columns-viv14"></a>Private occupied dwellings with electric light

[<img alt="../../_images/mx.inegi_columns.VIV14.png" src="../../_images/mx.inegi_columns.VIV14.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV14.png)Measure &quot;Private occupied dwellings with electric light&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV14'
        );

Measure &quot;Private occupied dwellings with electric light&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV14'
        );

Measure &quot;Private occupied dwellings with electric light&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV14',
          'denominator'
        );

Measure &quot;Private occupied dwellings with electric light&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV14',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-electric-light-running-water-and-drainage"></a><a name="mx-inegi-columns-viv24"></a>Private occupied dwellings with electric light, running water and drainage

[<img alt="../../_images/mx.inegi_columns.VIV24.png" src="../../_images/mx.inegi_columns.VIV24.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV24.png)Measure &quot;Private occupied dwellings with electric light, running water and drainage&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV24'
        );

Measure &quot;Private occupied dwellings with electric light, running water and drainage&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV24'
        );

Measure &quot;Private occupied dwellings with electric light, running water and drainage&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV24',
          'denominator'
        );

Measure &quot;Private occupied dwellings with electric light, running water and drainage&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV24',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-internet-access"></a><a name="mx-inegi-columns-viv36"></a>Private occupied dwellings with Internet access

[<img alt="../../_images/mx.inegi_columns.VIV36.png" src="../../_images/mx.inegi_columns.VIV36.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV36.png)Measure &quot;Private occupied dwellings with Internet access&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV36'
        );

Measure &quot;Private occupied dwellings with Internet access&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV36'
        );

Measure &quot;Private occupied dwellings with Internet access&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV36',
          'denominator'
        );

Measure &quot;Private occupied dwellings with Internet access&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV36',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-neither-a-computer-nor-internet-access"></a><a name="mx-inegi-columns-viv39"></a>Private occupied dwellings with neither a computer nor Internet access

[<img alt="../../_images/mx.inegi_columns.VIV39.png" src="../../_images/mx.inegi_columns.VIV39.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV39.png)Measure &quot;Private occupied dwellings with neither a computer nor Internet access&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV39'
        );

Measure &quot;Private occupied dwellings with neither a computer nor Internet access&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV39'
        );

Measure &quot;Private occupied dwellings with neither a computer nor Internet access&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV39',
          'denominator'
        );

Measure &quot;Private occupied dwellings with neither a computer nor Internet access&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV39',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-neither-a-landline-nor-a-cellphone"></a><a name="mx-inegi-columns-viv38"></a>Private occupied dwellings with neither a landline nor a cellphone

[<img alt="../../_images/mx.inegi_columns.VIV38.png" src="../../_images/mx.inegi_columns.VIV38.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV38.png)Measure &quot;Private occupied dwellings with neither a landline nor a cellphone&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV38'
        );

Measure &quot;Private occupied dwellings with neither a landline nor a cellphone&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV38'
        );

Measure &quot;Private occupied dwellings with neither a landline nor a cellphone&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV38',
          'denominator'
        );

Measure &quot;Private occupied dwellings with neither a landline nor a cellphone&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV38',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-neither-electric-light-running-water-nor-drainage"></a><a name="mx-inegi-columns-viv25"></a>Private occupied dwellings with neither electric light, running water nor drainage

[<img alt="../../_images/mx.inegi_columns.VIV25.png" src="../../_images/mx.inegi_columns.VIV25.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV25.png)Measure &quot;Private occupied dwellings with neither electric light, running water nor drainage&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV25'
        );

Measure &quot;Private occupied dwellings with neither electric light, running water nor drainage&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV25'
        );

Measure &quot;Private occupied dwellings with neither electric light, running water nor drainage&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV25',
          'denominator'
        );

Measure &quot;Private occupied dwellings with neither electric light, running water nor drainage&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV25',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-neither-refigerator-nor-washer"></a><a name="mx-inegi-columns-viv29"></a>Private occupied dwellings with neither refigerator nor washer

[<img alt="../../_images/mx.inegi_columns.VIV29.png" src="../../_images/mx.inegi_columns.VIV29.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV29.png)Measure &quot;Private occupied dwellings with neither refigerator nor washer&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV29'
        );

Measure &quot;Private occupied dwellings with neither refigerator nor washer&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV29'
        );

Measure &quot;Private occupied dwellings with neither refigerator nor washer&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV29',
          'denominator'
        );

Measure &quot;Private occupied dwellings with neither refigerator nor washer&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV29',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-neither-refigerator-washer-nor-car-or-van"></a><a name="mx-inegi-columns-viv30"></a>Private occupied dwellings with neither refigerator, washer, nor car or van

[<img alt="../../_images/mx.inegi_columns.VIV30.png" src="../../_images/mx.inegi_columns.VIV30.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV30.png)Measure &quot;Private occupied dwellings with neither refigerator, washer, nor car or van&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV30'
        );

Measure &quot;Private occupied dwellings with neither refigerator, washer, nor car or van&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV30'
        );

Measure &quot;Private occupied dwellings with neither refigerator, washer, nor car or van&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV30',
          'denominator'
        );

Measure &quot;Private occupied dwellings with neither refigerator, washer, nor car or van&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV30',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-neither-television-nor-radio"></a><a name="mx-inegi-columns-viv37"></a>Private occupied dwellings with neither television nor radio

[<img alt="../../_images/mx.inegi_columns.VIV37.png" src="../../_images/mx.inegi_columns.VIV37.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV37.png)Measure &quot;Private occupied dwellings with neither television nor radio&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV37'
        );

Measure &quot;Private occupied dwellings with neither television nor radio&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV37'
        );

Measure &quot;Private occupied dwellings with neither television nor radio&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV37',
          'denominator'
        );

Measure &quot;Private occupied dwellings with neither television nor radio&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV37',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-no-physical-property"></a><a name="mx-inegi-columns-viv41"></a>Private occupied dwellings with no physical property

[<img alt="../../_images/mx.inegi_columns.VIV41.png" src="../../_images/mx.inegi_columns.VIV41.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV41.png)Measure &quot;Private occupied dwellings with no physical property&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV41'
        );

Measure &quot;Private occupied dwellings with no physical property&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV41'
        );

Measure &quot;Private occupied dwellings with no physical property&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV41',
          'denominator'
        );

Measure &quot;Private occupied dwellings with no physical property&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV41',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-one-bedroom"></a><a name="mx-inegi-columns-viv7"></a>Private occupied dwellings with one bedroom

[<img alt="../../_images/mx.inegi_columns.VIV7.png" src="../../_images/mx.inegi_columns.VIV7.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV7.png)Measure &quot;Private occupied dwellings with one bedroom&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV7'
        );

Measure &quot;Private occupied dwellings with one bedroom&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV7'
        );

Measure &quot;Private occupied dwellings with one bedroom&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV7',
          'denominator'
        );

Measure &quot;Private occupied dwellings with one bedroom&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV7',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-running-water"></a><a name="mx-inegi-columns-viv16"></a>Private occupied dwellings with running water

[<img alt="../../_images/mx.inegi_columns.VIV16.png" src="../../_images/mx.inegi_columns.VIV16.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV16.png)Measure &quot;Private occupied dwellings with running water&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV16'
        );

Measure &quot;Private occupied dwellings with running water&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV16'
        );

Measure &quot;Private occupied dwellings with running water&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV16',
          'denominator'
        );

Measure &quot;Private occupied dwellings with running water&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV16',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-with-two-bedrooms-or-more"></a><a name="mx-inegi-columns-viv8"></a>Private occupied dwellings with two bedrooms or more

[<img alt="../../_images/mx.inegi_columns.VIV8.png" src="../../_images/mx.inegi_columns.VIV8.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV8.png)Measure &quot;Private occupied dwellings with two bedrooms or more&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV8'
        );

Measure &quot;Private occupied dwellings with two bedrooms or more&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV8'
        );

Measure &quot;Private occupied dwellings with two bedrooms or more&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV8',
          'denominator'
        );

Measure &quot;Private occupied dwellings with two bedrooms or more&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV8',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-without-a-bathroom-or-toilet"></a><a name="mx-inegi-columns-viv20"></a>Private occupied dwellings without a bathroom or toilet

[<img alt="../../_images/mx.inegi_columns.VIV20.png" src="../../_images/mx.inegi_columns.VIV20.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV20.png)Measure &quot;Private occupied dwellings without a bathroom or toilet&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV20'
        );

Measure &quot;Private occupied dwellings without a bathroom or toilet&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV20'
        );

Measure &quot;Private occupied dwellings without a bathroom or toilet&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV20',
          'denominator'
        );

Measure &quot;Private occupied dwellings without a bathroom or toilet&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV20',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-without-any-information-or-communication-technology"></a><a name="mx-inegi-columns-viv40"></a>Private occupied dwellings without any information or communication technology

[<img alt="../../_images/mx.inegi_columns.VIV40.png" src="../../_images/mx.inegi_columns.VIV40.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV40.png)Measure &quot;Private occupied dwellings without any information or communication technology&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV40'
        );

Measure &quot;Private occupied dwellings without any information or communication technology&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV40'
        );

Measure &quot;Private occupied dwellings without any information or communication technology&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV40',
          'denominator'
        );

Measure &quot;Private occupied dwellings without any information or communication technology&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV40',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-without-drainage"></a><a name="mx-inegi-columns-viv23"></a>Private occupied dwellings without drainage

[<img alt="../../_images/mx.inegi_columns.VIV23.png" src="../../_images/mx.inegi_columns.VIV23.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV23.png)Measure &quot;Private occupied dwellings without drainage&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV23'
        );

Measure &quot;Private occupied dwellings without drainage&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV23'
        );

Measure &quot;Private occupied dwellings without drainage&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV23',
          'denominator'
        );

Measure &quot;Private occupied dwellings without drainage&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV23',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-without-electric-light"></a><a name="mx-inegi-columns-viv15"></a>Private occupied dwellings without electric light

[<img alt="../../_images/mx.inegi_columns.VIV15.png" src="../../_images/mx.inegi_columns.VIV15.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV15.png)Measure &quot;Private occupied dwellings without electric light&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV15'
        );

Measure &quot;Private occupied dwellings without electric light&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV15'
        );

Measure &quot;Private occupied dwellings without electric light&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV15',
          'denominator'
        );

Measure &quot;Private occupied dwellings without electric light&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV15',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="private-occupied-dwellings-without-running-water"></a><a name="mx-inegi-columns-viv17"></a>Private occupied dwellings without running water

[<img alt="../../_images/mx.inegi_columns.VIV17.png" src="../../_images/mx.inegi_columns.VIV17.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV17.png)Measure &quot;Private occupied dwellings without running water&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV17'
        );

Measure &quot;Private occupied dwellings without running water&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV17'
        );

Measure &quot;Private occupied dwellings without running water&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV17',
          'denominator'
        );

Measure &quot;Private occupied dwellings without running water&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV17',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="three-or-more-room-private-occupied-dwellings"></a><a name="mx-inegi-columns-viv12"></a>Three or more room private occupied dwellings

[<img alt="../../_images/mx.inegi_columns.VIV12.png" src="../../_images/mx.inegi_columns.VIV12.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV12.png)Measure &quot;Three or more room private occupied dwellings&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV12'
        );

Measure &quot;Three or more room private occupied dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV12'
        );

Measure &quot;Three or more room private occupied dwellings&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV12',
          'denominator'
        );

Measure &quot;Three or more room private occupied dwellings&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV12',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


#### <a name="two-room-private-occupied-dwellings"></a><a name="mx-inegi-columns-viv11"></a>Two room private occupied dwellings

[<img alt="../../_images/mx.inegi_columns.VIV11.png" src="../../_images/mx.inegi_columns.VIV11.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV11.png)Measure &quot;Two room private occupied dwellings&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV11'
        );

Measure &quot;Two room private occupied dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV11'
        );

Measure &quot;Two room private occupied dwellings&quot; percent of &quot;Occupied private dwellings&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV11',
          'denominator'
        );

Measure &quot;Two room private occupied dwellings&quot; percent of &quot;Occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV11',
          'denominator'
        );

* denominator: [Occupied private dwellings](#mx-inegi-columns-viv2)


## <a name="population-in-occupied-private-dwellings"></a><a name="mx-inegi-columns-viv3"></a>Population in occupied private dwellings

[<img alt="../../_images/mx.inegi_columns.VIV3.png" src="../../_images/mx.inegi_columns.VIV3.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV3.png)Measure &quot;Population in occupied private dwellings&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV3'
        );

Measure &quot;Population in occupied private dwellings&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV3'
        );

Measure &quot;Population in occupied private dwellings&quot; percent of &quot;Total population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV3',
          'denominator'
        );

Measure &quot;Population in occupied private dwellings&quot; percent of &quot;Total population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV3',
          'denominator'
        );

* denominator: [Total population](../age_gender/#mx-inegi-columns-pob1)


## <a name="population-in-private-occupied-dwellings-with-a-bathroom-or-toilet"></a><a name="mx-inegi-columns-viv21"></a>Population in private occupied dwellings with a bathroom or toilet

[<img alt="../../_images/mx.inegi_columns.VIV21.png" src="../../_images/mx.inegi_columns.VIV21.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV21.png)Measure &quot;Population in private occupied dwellings with a bathroom or toilet&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV21'
        );

Measure &quot;Population in private occupied dwellings with a bathroom or toilet&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV21'
        );

Measure &quot;Population in private occupied dwellings with a bathroom or toilet&quot; percent of &quot;Total population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV21',
          'denominator'
        );

Measure &quot;Population in private occupied dwellings with a bathroom or toilet&quot; percent of &quot;Total population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV21',
          'denominator'
        );

* denominator: [Total population](../age_gender/#mx-inegi-columns-pob1)


## <a name="population-in-private-occupied-dwellings-with-running-water"></a><a name="mx-inegi-columns-viv18"></a>Population in private occupied dwellings with running water

[<img alt="../../_images/mx.inegi_columns.VIV18.png" src="../../_images/mx.inegi_columns.VIV18.png" style="width: 100%;" />](../../_images/mx.inegi_columns.VIV18.png)Measure &quot;Population in private occupied dwellings with running water&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV18'
        );

Measure &quot;Population in private occupied dwellings with running water&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV18'
        );

Measure &quot;Population in private occupied dwellings with running water&quot; percent of &quot;Total population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.VIV18',
          'denominator'
        );

Measure &quot;Population in private occupied dwellings with running water&quot; percent of &quot;Total population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.VIV18',
          'denominator'
        );

* denominator: [Total population](../age_gender/#mx-inegi-columns-pob1)


