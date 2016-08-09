

  
# <a name="transportation"></a>Transportation

How do people move from place to place?

- [Car-free households](#car-free-households)

- [Commuters by Car, Truck, or Van](#commuters-by-car-truck-or-van)

    * [Commuters by Carpool](#commuters-by-carpool)

    * [Commuters who drove alone](#commuters-who-drove-alone)

- [Commuters by Public Transportation](#commuters-by-public-transportation)

    * [Commuters by Bus](#commuters-by-bus)

    * [Commuters by Subway or Elevated](#commuters-by-subway-or-elevated)

- [Four car households](#four-car-households)

- [One car households](#one-car-households)

- [Three car households](#three-car-households)

- [Two car households](#two-car-households)

- [Walked to Work](#walked-to-work)

- [Worked at Home](#worked-at-home)

- [Workers age 16 and over who do not work from home](#workers-age-16-and-over-who-do-not-work-from-home)

    * [Number of workers with a commute between 5 and 9 minutes](#number-of-workers-with-a-commute-between-5-and-9-minutes)

    * [Number of workers with a commute between 10 and 14 minutes](#number-of-workers-with-a-commute-between-10-and-14-minutes)

    * [Number of workers with a commute between 15 and 19 minutes](#number-of-workers-with-a-commute-between-15-and-19-minutes)

    * [Number of workers with a commute between 20 and 24 minutes](#number-of-workers-with-a-commute-between-20-and-24-minutes)

    * [Number of workers with a commute between 25 and 29 minutes](#number-of-workers-with-a-commute-between-25-and-29-minutes)

    * [Number of workers with a commute between 30 and 34 minutes](#number-of-workers-with-a-commute-between-30-and-34-minutes)

    * [Number of workers with a commute between 35 and 39 minutes](#number-of-workers-with-a-commute-between-35-and-39-minutes)

    * [Number of workers with a commute between 35 and 44 minutes](#number-of-workers-with-a-commute-between-35-and-44-minutes)

    * [Number of workers with a commute between 40 and 44 minutes](#number-of-workers-with-a-commute-between-40-and-44-minutes)

    * [Number of workers with a commute between 45 and 59 minutes](#number-of-workers-with-a-commute-between-45-and-59-minutes)

    * [Number of workers with a commute between 60 and 89 minutes](#number-of-workers-with-a-commute-between-60-and-89-minutes)

    * [Number of workers with a commute of more than 90 minutes](#number-of-workers-with-a-commute-of-more-than-90-minutes)

    * [Number of workers with a commute of over 60 minutes](#number-of-workers-with-a-commute-of-over-60-minutes)

    * [Number of workers with less than 10 minute commute](#number-of-workers-with-less-than-10-minute-commute)

- [Workers age 16 and over with no vehicle](#workers-age-16-and-over-with-no-vehicle)


## <a name="car-free-households"></a><a name="us-census-acs-b08201002"></a>Car-free households

[<img alt="../../_images/us.census.acs.B08201002.png" src="../../_images/us.census.acs.B08201002.png" style="width: 100%;" />](../../_images/us.census.acs.B08201002.png)The number of households without passenger cars, vans, and pickup or panel trucks of one-ton capacity or less kept at home and available for the use of household members. Vehicles rented or leased for one month or more, company vehicles, and police and government vehicles are included if kept at home and used for non-business purposes. Dismantled or immobile vehicles ware excluded. Vehicles kept at home but used only for business purposes also are excluded.

Measure &quot;Car-free households&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08201002'
        );

Measure &quot;Car-free households&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08201002'
        );

Measure &quot;Car-free households&quot; percent of &quot;Households&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08201002',
          'denominator'
        );

Measure &quot;Car-free households&quot; percent of &quot;Households&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08201002',
          'denominator'
        );

* denominator: [Households](../housing/#us-census-acs-b11001001)


## <a name="commuters-by-car-truck-or-van"></a><a name="us-census-acs-b08006002"></a>Commuters by Car, Truck, or Van

[<img alt="../../_images/us.census.acs.B08006002.png" src="../../_images/us.census.acs.B08006002.png" style="width: 100%;" />](../../_images/us.census.acs.B08006002.png)The number of workers age 16 years and over within  a geographic area who primarily traveled to work by car, truck or  van.  This is the principal mode of travel or type of conveyance,  by distance rather than time, that the worker usually used to get  from home to work.

Measure &quot;Commuters by Car, Truck, or Van&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006002'
        );

Measure &quot;Commuters by Car, Truck, or Van&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006002'
        );

Measure &quot;Commuters by Car, Truck, or Van&quot; percent of &quot;Workers over the Age of 16&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006002',
          'denominator'
        );

Measure &quot;Commuters by Car, Truck, or Van&quot; percent of &quot;Workers over the Age of 16&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006002',
          'denominator'
        );

* denominator: [Workers over the Age of 16](../employment/#us-census-acs-b08006001)

Subcolumns of Commuters by Car, Truck, or Van

- [Commuters by Carpool](#commuters-by-carpool)

- [Commuters who drove alone](#commuters-who-drove-alone)


### <a name="commuters-by-carpool"></a><a name="us-census-acs-b08006004"></a>Commuters by Carpool

[<img alt="../../_images/us.census.acs.B08006004.png" src="../../_images/us.census.acs.B08006004.png" style="width: 100%;" />](../../_images/us.census.acs.B08006004.png)The number of workers age 16 years and over within a geographic area who primarily traveled to work by carpool.  This is the principal mode of travel or type of conveyance, by distance rather than time, that the worker usually used to get from home to work.

Measure &quot;Commuters by Carpool&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006004'
        );

Measure &quot;Commuters by Carpool&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006004'
        );

Measure &quot;Commuters by Carpool&quot; percent of &quot;Commuters by Car, Truck, or Van&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006004',
          'denominator'
        );

Measure &quot;Commuters by Carpool&quot; percent of &quot;Commuters by Car, Truck, or Van&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006004',
          'denominator'
        );

* denominator: [Commuters by Car, Truck, or Van](#us-census-acs-b08006002)


### <a name="commuters-who-drove-alone"></a><a name="us-census-acs-b08006003"></a>Commuters who drove alone

[<img alt="../../_images/us.census.acs.B08006003.png" src="../../_images/us.census.acs.B08006003.png" style="width: 100%;" />](../../_images/us.census.acs.B08006003.png)The number of workers age 16 years and over within a geographic area who primarily traveled by car driving alone. This is the principal mode of travel or type of conveyance, by distance rather than time, that the worker usually used to get from home to work.

Measure &quot;Commuters who drove alone&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006003'
        );

Measure &quot;Commuters who drove alone&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006003'
        );

Measure &quot;Commuters who drove alone&quot; percent of &quot;Commuters by Car, Truck, or Van&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006003',
          'denominator'
        );

Measure &quot;Commuters who drove alone&quot; percent of &quot;Commuters by Car, Truck, or Van&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006003',
          'denominator'
        );

* denominator: [Commuters by Car, Truck, or Van](#us-census-acs-b08006002)


## <a name="commuters-by-public-transportation"></a><a name="us-census-acs-b08301010"></a>Commuters by Public Transportation

[<img alt="../../_images/us.census.acs.B08301010.png" src="../../_images/us.census.acs.B08301010.png" style="width: 100%;" />](../../_images/us.census.acs.B08301010.png)The number of workers age 16 years and over within a geographic area who primarily traveled to work by public transportation.  This is the principal mode of travel or type of conveyance, by distance rather than time, that the worker usually used to get from home to work.

Measure &quot;Commuters by Public Transportation&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08301010'
        );

Measure &quot;Commuters by Public Transportation&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08301010'
        );

Measure &quot;Commuters by Public Transportation&quot; percent of &quot;Workers over the Age of 16&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08301010',
          'denominator'
        );

Measure &quot;Commuters by Public Transportation&quot; percent of &quot;Workers over the Age of 16&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08301010',
          'denominator'
        );

* denominator: [Workers over the Age of 16](../employment/#us-census-acs-b08006001)

Subcolumns of Commuters by Public Transportation

- [Commuters by Bus](#commuters-by-bus)

- [Commuters by Subway or Elevated](#commuters-by-subway-or-elevated)


### <a name="commuters-by-bus"></a><a name="us-census-acs-b08006009"></a>Commuters by Bus

[<img alt="../../_images/us.census.acs.B08006009.png" src="../../_images/us.census.acs.B08006009.png" style="width: 100%;" />](../../_images/us.census.acs.B08006009.png)The number of workers age 16 years and over within a geographic area who primarily traveled to work by bus.  This is the principal mode of travel or type of conveyance, by distance rather than time, that the worker usually used to get from home to work.  This is a subset of workers who commuted by public transport.

Measure &quot;Commuters by Bus&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006009'
        );

Measure &quot;Commuters by Bus&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006009'
        );

Measure &quot;Commuters by Bus&quot; percent of &quot;Commuters by Public Transportation&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006009',
          'denominator'
        );

Measure &quot;Commuters by Bus&quot; percent of &quot;Commuters by Public Transportation&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006009',
          'denominator'
        );

* denominator: [Commuters by Public Transportation](#us-census-acs-b08301010)


### <a name="commuters-by-subway-or-elevated"></a><a name="us-census-acs-b08006011"></a>Commuters by Subway or Elevated

[<img alt="../../_images/us.census.acs.B08006011.png" src="../../_images/us.census.acs.B08006011.png" style="width: 100%;" />](../../_images/us.census.acs.B08006011.png)The number of workers age 16 years and over within a geographic area who primarily traveled to work by subway or elevated train.  This is the principal mode of travel or type of conveyance, by distance rather than time, that the worker usually used to get from home to work.  This is a subset of workers who commuted by public transport.

Measure &quot;Commuters by Subway or Elevated&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006011'
        );

Measure &quot;Commuters by Subway or Elevated&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006011'
        );

Measure &quot;Commuters by Subway or Elevated&quot; percent of &quot;Commuters by Public Transportation&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006011',
          'denominator'
        );

Measure &quot;Commuters by Subway or Elevated&quot; percent of &quot;Commuters by Public Transportation&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006011',
          'denominator'
        );

* denominator: [Commuters by Public Transportation](#us-census-acs-b08301010)


## <a name="four-car-households"></a><a name="us-census-acs-b08201006"></a>Four car households

[<img alt="../../_images/us.census.acs.B08201006.png" src="../../_images/us.census.acs.B08201006.png" style="width: 100%;" />](../../_images/us.census.acs.B08201006.png)The number of households with four or more passenger cars, vans, pickup or panel trucks of one-ton capacity or less kept at home and available for the use of household members. Vehicles rented or leased for one month or more, company vehicles, and police and government vehicles are included if kept at home and used for non-business purposes. Dismantled or immobile vehicles ware excluded. Vehicles kept at home but used only for business purposes also are excluded.

Measure &quot;Four car households&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08201006'
        );

Measure &quot;Four car households&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08201006'
        );

Measure &quot;Four car households&quot; percent of &quot;Households&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08201006',
          'denominator'
        );

Measure &quot;Four car households&quot; percent of &quot;Households&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08201006',
          'denominator'
        );

* denominator: [Households](../housing/#us-census-acs-b11001001)


## <a name="one-car-households"></a><a name="us-census-acs-b08201003"></a>One car households

[<img alt="../../_images/us.census.acs.B08201003.png" src="../../_images/us.census.acs.B08201003.png" style="width: 100%;" />](../../_images/us.census.acs.B08201003.png)The number of households with one passenger car, van, , pickup or panel trucks of one-ton capacity or less kept at home and available for the use of household members. Vehicles rented or leased for one month or more, company vehicles, and police and government vehicles are included if kept at home and used for non-business purposes. Dismantled or immobile vehicles ware excluded. Vehicles kept at home but used only for business purposes also are excluded.

Measure &quot;One car households&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08201003'
        );

Measure &quot;One car households&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08201003'
        );

Measure &quot;One car households&quot; percent of &quot;Households&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08201003',
          'denominator'
        );

Measure &quot;One car households&quot; percent of &quot;Households&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08201003',
          'denominator'
        );

* denominator: [Households](../housing/#us-census-acs-b11001001)


## <a name="three-car-households"></a><a name="us-census-acs-b08201005"></a>Three car households

[<img alt="../../_images/us.census.acs.B08201005.png" src="../../_images/us.census.acs.B08201005.png" style="width: 100%;" />](../../_images/us.census.acs.B08201005.png)The number of households with one passenger cars, vans, pickup or panel trucks of one-ton capacity or less kept at home and available for the use of household members. Vehicles rented or leased for one month or more, company vehicles, and police and government vehicles are included if kept at home and used for non-business purposes. Dismantled or immobile vehicles ware excluded. Vehicles kept at home but used only for business purposes also are excluded.

Measure &quot;Three car households&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08201005'
        );

Measure &quot;Three car households&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08201005'
        );

Measure &quot;Three car households&quot; percent of &quot;Households&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08201005',
          'denominator'
        );

Measure &quot;Three car households&quot; percent of &quot;Households&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08201005',
          'denominator'
        );

* denominator: [Households](../housing/#us-census-acs-b11001001)


## <a name="two-car-households"></a><a name="us-census-acs-b08201004"></a>Two car households

[<img alt="../../_images/us.census.acs.B08201004.png" src="../../_images/us.census.acs.B08201004.png" style="width: 100%;" />](../../_images/us.census.acs.B08201004.png)The number of households with two passenger cars, vans, pickup or panel trucks of one-ton capacity or less kept at home and available for the use of household members. Vehicles rented or leased for one month or more, company vehicles, and police and government vehicles are included if kept at home and used for non-business purposes. Dismantled or immobile vehicles ware excluded. Vehicles kept at home but used only for business purposes also are excluded.

Measure &quot;Two car households&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08201004'
        );

Measure &quot;Two car households&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08201004'
        );

Measure &quot;Two car households&quot; percent of &quot;Households&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08201004',
          'denominator'
        );

Measure &quot;Two car households&quot; percent of &quot;Households&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08201004',
          'denominator'
        );

* denominator: [Households](../housing/#us-census-acs-b11001001)


## <a name="walked-to-work"></a><a name="us-census-acs-b08006015"></a>Walked to Work

[<img alt="../../_images/us.census.acs.B08006015.png" src="../../_images/us.census.acs.B08006015.png" style="width: 100%;" />](../../_images/us.census.acs.B08006015.png)The number of workers age 16 years and over within a geographic area who primarily walked to work.  This would mean that of any way of getting to work, they travelled the most distance walking.

Measure &quot;Walked to Work&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006015'
        );

Measure &quot;Walked to Work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006015'
        );

Measure &quot;Walked to Work&quot; percent of &quot;Workers over the Age of 16&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006015',
          'denominator'
        );

Measure &quot;Walked to Work&quot; percent of &quot;Workers over the Age of 16&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006015',
          'denominator'
        );

* denominator: [Workers over the Age of 16](../employment/#us-census-acs-b08006001)


## <a name="worked-at-home"></a><a name="us-census-acs-b08006017"></a>Worked at Home

[<img alt="../../_images/us.census.acs.B08006017.png" src="../../_images/us.census.acs.B08006017.png" style="width: 100%;" />](../../_images/us.census.acs.B08006017.png)The count within a geographical area of workers over the age of 16 who worked at home.

Measure &quot;Worked at Home&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006017'
        );

Measure &quot;Worked at Home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006017'
        );

Measure &quot;Worked at Home&quot; percent of &quot;Workers over the Age of 16&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08006017',
          'denominator'
        );

Measure &quot;Worked at Home&quot; percent of &quot;Workers over the Age of 16&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08006017',
          'denominator'
        );

* denominator: [Workers over the Age of 16](../employment/#us-census-acs-b08006001)


## <a name="workers-age-16-and-over-who-do-not-work-from-home"></a><a name="us-census-acs-b08134001"></a>Workers age 16 and over who do not work from home

[<img alt="../../_images/us.census.acs.B08134001.png" src="../../_images/us.census.acs.B08134001.png" style="width: 100%;" />](../../_images/us.census.acs.B08134001.png)The number of workers in a geographic area over the age of 16 who do not work from home

Measure &quot;Workers age 16 and over who do not work from home&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08134001'
        );

Measure &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08134001'
        );

Subcolumns of Workers age 16 and over who do not work from home

- [Number of workers with a commute between 5 and 9 minutes](#number-of-workers-with-a-commute-between-5-and-9-minutes)

- [Number of workers with a commute between 10 and 14 minutes](#number-of-workers-with-a-commute-between-10-and-14-minutes)

- [Number of workers with a commute between 15 and 19 minutes](#number-of-workers-with-a-commute-between-15-and-19-minutes)

- [Number of workers with a commute between 20 and 24 minutes](#number-of-workers-with-a-commute-between-20-and-24-minutes)

- [Number of workers with a commute between 25 and 29 minutes](#number-of-workers-with-a-commute-between-25-and-29-minutes)

- [Number of workers with a commute between 30 and 34 minutes](#number-of-workers-with-a-commute-between-30-and-34-minutes)

- [Number of workers with a commute between 35 and 39 minutes](#number-of-workers-with-a-commute-between-35-and-39-minutes)

- [Number of workers with a commute between 35 and 44 minutes](#number-of-workers-with-a-commute-between-35-and-44-minutes)

- [Number of workers with a commute between 40 and 44 minutes](#number-of-workers-with-a-commute-between-40-and-44-minutes)

- [Number of workers with a commute between 45 and 59 minutes](#number-of-workers-with-a-commute-between-45-and-59-minutes)

- [Number of workers with a commute between 60 and 89 minutes](#number-of-workers-with-a-commute-between-60-and-89-minutes)

- [Number of workers with a commute of more than 90 minutes](#number-of-workers-with-a-commute-of-more-than-90-minutes)

- [Number of workers with a commute of over 60 minutes](#number-of-workers-with-a-commute-of-over-60-minutes)

- [Number of workers with less than 10 minute commute](#number-of-workers-with-less-than-10-minute-commute)


### <a name="number-of-workers-with-a-commute-between-5-and-9-minutes"></a><a name="us-census-acs-b08303003"></a>Number of workers with a commute between 5 and 9 minutes

[<img alt="../../_images/us.census.acs.B08303003.png" src="../../_images/us.census.acs.B08303003.png" style="width: 100%;" />](../../_images/us.census.acs.B08303003.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute in between 5 and 9 minutes.

Measure &quot;Number of workers with a commute between 5 and 9 minutes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303003'
        );

Measure &quot;Number of workers with a commute between 5 and 9 minutes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303003'
        );

Measure &quot;Number of workers with a commute between 5 and 9 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303003',
          'denominator'
        );

Measure &quot;Number of workers with a commute between 5 and 9 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303003',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


### <a name="number-of-workers-with-a-commute-between-10-and-14-minutes"></a><a name="us-census-acs-b08303004"></a>Number of workers with a commute between 10 and 14 minutes

[<img alt="../../_images/us.census.acs.B08303004.png" src="../../_images/us.census.acs.B08303004.png" style="width: 100%;" />](../../_images/us.census.acs.B08303004.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute in between 10 and 14 minutes.

Measure &quot;Number of workers with a commute between 10 and 14 minutes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303004'
        );

Measure &quot;Number of workers with a commute between 10 and 14 minutes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303004'
        );

Measure &quot;Number of workers with a commute between 10 and 14 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303004',
          'denominator'
        );

Measure &quot;Number of workers with a commute between 10 and 14 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303004',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


### <a name="number-of-workers-with-a-commute-between-15-and-19-minutes"></a><a name="us-census-acs-b08303005"></a>Number of workers with a commute between 15 and 19 minutes

[<img alt="../../_images/us.census.acs.B08303005.png" src="../../_images/us.census.acs.B08303005.png" style="width: 100%;" />](../../_images/us.census.acs.B08303005.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute in between 15 and 19 minutes.

Measure &quot;Number of workers with a commute between 15 and 19 minutes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303005'
        );

Measure &quot;Number of workers with a commute between 15 and 19 minutes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303005'
        );

Measure &quot;Number of workers with a commute between 15 and 19 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303005',
          'denominator'
        );

Measure &quot;Number of workers with a commute between 15 and 19 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303005',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


### <a name="number-of-workers-with-a-commute-between-20-and-24-minutes"></a><a name="us-census-acs-b08303006"></a>Number of workers with a commute between 20 and 24 minutes

[<img alt="../../_images/us.census.acs.B08303006.png" src="../../_images/us.census.acs.B08303006.png" style="width: 100%;" />](../../_images/us.census.acs.B08303006.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute in between 20 and 24 minutes.

Measure &quot;Number of workers with a commute between 20 and 24 minutes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303006'
        );

Measure &quot;Number of workers with a commute between 20 and 24 minutes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303006'
        );

Measure &quot;Number of workers with a commute between 20 and 24 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303006',
          'denominator'
        );

Measure &quot;Number of workers with a commute between 20 and 24 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303006',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


### <a name="number-of-workers-with-a-commute-between-25-and-29-minutes"></a><a name="us-census-acs-b08303007"></a>Number of workers with a commute between 25 and 29 minutes

[<img alt="../../_images/us.census.acs.B08303007.png" src="../../_images/us.census.acs.B08303007.png" style="width: 100%;" />](../../_images/us.census.acs.B08303007.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute in between 25 and 29 minutes.

Measure &quot;Number of workers with a commute between 25 and 29 minutes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303007'
        );

Measure &quot;Number of workers with a commute between 25 and 29 minutes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303007'
        );

Measure &quot;Number of workers with a commute between 25 and 29 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303007',
          'denominator'
        );

Measure &quot;Number of workers with a commute between 25 and 29 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303007',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


### <a name="number-of-workers-with-a-commute-between-30-and-34-minutes"></a><a name="us-census-acs-b08303008"></a>Number of workers with a commute between 30 and 34 minutes

[<img alt="../../_images/us.census.acs.B08303008.png" src="../../_images/us.census.acs.B08303008.png" style="width: 100%;" />](../../_images/us.census.acs.B08303008.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute in between 30 and 34 minutes.

Measure &quot;Number of workers with a commute between 30 and 34 minutes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303008'
        );

Measure &quot;Number of workers with a commute between 30 and 34 minutes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303008'
        );

Measure &quot;Number of workers with a commute between 30 and 34 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303008',
          'denominator'
        );

Measure &quot;Number of workers with a commute between 30 and 34 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303008',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


### <a name="number-of-workers-with-a-commute-between-35-and-39-minutes"></a><a name="us-census-acs-b08303009"></a>Number of workers with a commute between 35 and 39 minutes

[<img alt="../../_images/us.census.acs.B08303009.png" src="../../_images/us.census.acs.B08303009.png" style="width: 100%;" />](../../_images/us.census.acs.B08303009.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute in between 35 and 39 minutes.

Measure &quot;Number of workers with a commute between 35 and 39 minutes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303009'
        );

Measure &quot;Number of workers with a commute between 35 and 39 minutes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303009'
        );

Measure &quot;Number of workers with a commute between 35 and 39 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303009',
          'denominator'
        );

Measure &quot;Number of workers with a commute between 35 and 39 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303009',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


### <a name="number-of-workers-with-a-commute-between-35-and-44-minutes"></a><a name="us-census-acs-b08134008"></a>Number of workers with a commute between 35 and 44 minutes

[<img alt="../../_images/us.census.acs.B08134008.png" src="../../_images/us.census.acs.B08134008.png" style="width: 100%;" />](../../_images/us.census.acs.B08134008.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute in between 35 and 44 minutes.

Measure &quot;Number of workers with a commute between 35 and 44 minutes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08134008'
        );

Measure &quot;Number of workers with a commute between 35 and 44 minutes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08134008'
        );

Measure &quot;Number of workers with a commute between 35 and 44 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08134008',
          'denominator'
        );

Measure &quot;Number of workers with a commute between 35 and 44 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08134008',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


### <a name="number-of-workers-with-a-commute-between-40-and-44-minutes"></a><a name="us-census-acs-b08303010"></a>Number of workers with a commute between 40 and 44 minutes

[<img alt="../../_images/us.census.acs.B08303010.png" src="../../_images/us.census.acs.B08303010.png" style="width: 100%;" />](../../_images/us.census.acs.B08303010.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute in between 40 and 44 minutes.

Measure &quot;Number of workers with a commute between 40 and 44 minutes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303010'
        );

Measure &quot;Number of workers with a commute between 40 and 44 minutes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303010'
        );

Measure &quot;Number of workers with a commute between 40 and 44 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303010',
          'denominator'
        );

Measure &quot;Number of workers with a commute between 40 and 44 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303010',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


### <a name="number-of-workers-with-a-commute-between-45-and-59-minutes"></a><a name="us-census-acs-b08303011"></a>Number of workers with a commute between 45 and 59 minutes

[<img alt="../../_images/us.census.acs.B08303011.png" src="../../_images/us.census.acs.B08303011.png" style="width: 100%;" />](../../_images/us.census.acs.B08303011.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute in between 45 and 59 minutes.

Measure &quot;Number of workers with a commute between 45 and 59 minutes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303011'
        );

Measure &quot;Number of workers with a commute between 45 and 59 minutes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303011'
        );

Measure &quot;Number of workers with a commute between 45 and 59 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303011',
          'denominator'
        );

Measure &quot;Number of workers with a commute between 45 and 59 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303011',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


### <a name="number-of-workers-with-a-commute-between-60-and-89-minutes"></a><a name="us-census-acs-b08303012"></a>Number of workers with a commute between 60 and 89 minutes

[<img alt="../../_images/us.census.acs.B08303012.png" src="../../_images/us.census.acs.B08303012.png" style="width: 100%;" />](../../_images/us.census.acs.B08303012.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute in between 60 and 89 minutes .

Measure &quot;Number of workers with a commute between 60 and 89 minutes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303012'
        );

Measure &quot;Number of workers with a commute between 60 and 89 minutes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303012'
        );

Measure &quot;Number of workers with a commute between 60 and 89 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303012',
          'denominator'
        );

Measure &quot;Number of workers with a commute between 60 and 89 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303012',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


### <a name="number-of-workers-with-a-commute-of-more-than-90-minutes"></a><a name="us-census-acs-b08303013"></a>Number of workers with a commute of more than 90 minutes

[<img alt="../../_images/us.census.acs.B08303013.png" src="../../_images/us.census.acs.B08303013.png" style="width: 100%;" />](../../_images/us.census.acs.B08303013.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute more than 90 minutes.

Measure &quot;Number of workers with a commute of more than 90 minutes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303013'
        );

Measure &quot;Number of workers with a commute of more than 90 minutes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303013'
        );

Measure &quot;Number of workers with a commute of more than 90 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08303013',
          'denominator'
        );

Measure &quot;Number of workers with a commute of more than 90 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08303013',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


### <a name="number-of-workers-with-a-commute-of-over-60-minutes"></a><a name="us-census-acs-b08134010"></a>Number of workers with a commute of over 60 minutes

[<img alt="../../_images/us.census.acs.B08134010.png" src="../../_images/us.census.acs.B08134010.png" style="width: 100%;" />](../../_images/us.census.acs.B08134010.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute in over 60 minutes.

Measure &quot;Number of workers with a commute of over 60 minutes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08134010'
        );

Measure &quot;Number of workers with a commute of over 60 minutes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08134010'
        );

Measure &quot;Number of workers with a commute of over 60 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08134010',
          'denominator'
        );

Measure &quot;Number of workers with a commute of over 60 minutes&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08134010',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


### <a name="number-of-workers-with-less-than-10-minute-commute"></a><a name="us-census-acs-b08134002"></a>Number of workers with less than 10 minute commute

[<img alt="../../_images/us.census.acs.B08134002.png" src="../../_images/us.census.acs.B08134002.png" style="width: 100%;" />](../../_images/us.census.acs.B08134002.png)The number of workers in a geographic area over the age of 16 who do not work from home and commute in less than 10 minutes.

Measure &quot;Number of workers with less than 10 minute commute&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08134002'
        );

Measure &quot;Number of workers with less than 10 minute commute&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08134002'
        );

Measure &quot;Number of workers with less than 10 minute commute&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08134002',
          'denominator'
        );

Measure &quot;Number of workers with less than 10 minute commute&quot; percent of &quot;Workers age 16 and over who do not work from home&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08134002',
          'denominator'
        );

* denominator: [Workers age 16 and over who do not work from home](#us-census-acs-b08134001)


## <a name="workers-age-16-and-over-with-no-vehicle"></a><a name="us-census-acs-b08014002"></a>Workers age 16 and over with no vehicle

[<img alt="../../_images/us.census.acs.B08014002.png" src="../../_images/us.census.acs.B08014002.png" style="width: 100%;" />](../../_images/us.census.acs.B08014002.png)All people in a geographic area over the age of 16 who do not own a car.

Measure &quot;Workers age 16 and over with no vehicle&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08014002'
        );

Measure &quot;Workers age 16 and over with no vehicle&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08014002'
        );

Measure &quot;Workers age 16 and over with no vehicle&quot; percent of &quot;Workers over the Age of 16&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B08014002',
          'denominator'
        );

Measure &quot;Workers age 16 and over with no vehicle&quot; percent of &quot;Workers over the Age of 16&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B08014002',
          'denominator'
        );

* denominator: [Workers over the Age of 16](../employment/#us-census-acs-b08006001)


