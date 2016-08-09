

  
# <a name="housing"></a>Housing

What type of housing exists and how do people live in it?

- [Households](#households)

- [Housing Units](#housing-units)

    * [Housing units built before 1939](#housing-units-built-before-1939)

    * [Housing units built between 2000 and 2004](#housing-units-built-between-2000-and-2004)

    * [Housing units built in 2005 or later](#housing-units-built-in-2005-or-later)

    * [Mobile homes](#mobile-homes)

    * [Owner-occupied Housing Units](#owner-occupied-housing-units)

        - [Owner-occupied Housing Units valued at $1,000,000 or more.](#owner-occupied-housing-units-valued-at-1-000-000-or-more)

        - [Owner-occupied Housing Units with a Mortgage](#owner-occupied-housing-units-with-a-mortgage)

    * [Renter occupied housing units](#renter-occupied-housing-units)

        - [Renter occupied housing units](#us-census-acs-b25003003)

    * [Vacant Housing Units](#vacant-housing-units)

        - [Vacant Housing Units for Rent](#vacant-housing-units-for-rent)

        - [Vacant Housing Units for Sale](#vacant-housing-units-for-sale)

- [Lived in a different house one year ago in a different city](#lived-in-a-different-house-one-year-ago-in-a-different-city)

- [Lived in a different house one year ago in the same city](#lived-in-a-different-house-one-year-ago-in-the-same-city)

- [Median Rent](#median-rent)

- [Median rental price per square foot for All homes](#median-rental-price-per-square-foot-for-all-homes)

- [Median rental price per square foot for Single Family residence rental](#median-rental-price-per-square-foot-for-single-family-residence-rental)

- [Median value per square foot for All homes](#median-value-per-square-foot-for-all-homes)

- [Owner-Occupied Housing Units Median Value](#owner-occupied-housing-units-median-value)

- [Percent of Household Income Spent on Rent](#percent-of-household-income-spent-on-rent)

- [Zillow Home Value Index for All homes](#zillow-home-value-index-for-all-homes)

- [Zillow Home Value Index for Single Family Homes](#zillow-home-value-index-for-single-family-homes)

- [Zillow Rental Index for All homes plus multifamily](#zillow-rental-index-for-all-homes-plus-multifamily)

- [Zillow Rental Index for Single Family residence rental](#zillow-rental-index-for-single-family-residence-rental)


## <a name="households"></a><a name="us-census-acs-b11001001"></a>Households

[<img alt="../../_images/us.census.acs.B11001001.png" src="../../_images/us.census.acs.B11001001.png" style="width: 100%;" />](../../_images/us.census.acs.B11001001.png)A count of the number of households in each geography. A household consists of one or more people who live in the same dwelling and also share at meals or living accommodation, and may consist of a single family or some other grouping of people.

Measure &quot;Households&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B11001001'
        );

Measure &quot;Households&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B11001001'
        );


## <a name="housing-units"></a><a name="us-census-acs-b25001001"></a>Housing Units

[<img alt="../../_images/us.census.acs.B25001001.png" src="../../_images/us.census.acs.B25001001.png" style="width: 100%;" />](../../_images/us.census.acs.B25001001.png)A count of housing units in each geography.  A housing unit is a house, an apartment, a mobile home or trailer, a group of rooms, or a single room occupied as separate living quarters, or if vacant, intended for occupancy as separate living quarters.

Measure &quot;Housing Units&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25001001'
        );

Measure &quot;Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25001001'
        );

Subcolumns of Housing Units

- [Housing units built before 1939](#housing-units-built-before-1939)

- [Housing units built between 2000 and 2004](#housing-units-built-between-2000-and-2004)

- [Housing units built in 2005 or later](#housing-units-built-in-2005-or-later)

- [Mobile homes](#mobile-homes)

- [Owner-occupied Housing Units](#owner-occupied-housing-units)

- [Renter occupied housing units](#renter-occupied-housing-units)

- [Vacant Housing Units](#vacant-housing-units)


### <a name="housing-units-built-before-1939"></a><a name="us-census-acs-b25034010"></a>Housing units built before 1939

[<img alt="../../_images/us.census.acs.B25034010.png" src="../../_images/us.census.acs.B25034010.png" style="width: 100%;" />](../../_images/us.census.acs.B25034010.png)A house, an apartment, a mobile home or trailer, a group of rooms, or a single room occupied as separate living quarters, or if vacant, intended for occupancy as separate living quarters built in 1939 or earlier.

Measure &quot;Housing units built before 1939&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25034010'
        );

Measure &quot;Housing units built before 1939&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25034010'
        );

Measure &quot;Housing units built before 1939&quot; percent of &quot;Housing Units&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25034010',
          'denominator'
        );

Measure &quot;Housing units built before 1939&quot; percent of &quot;Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25034010',
          'denominator'
        );

* denominator: [Housing Units](#us-census-acs-b25001001)


### <a name="housing-units-built-between-2000-and-2004"></a><a name="us-census-acs-b25034003"></a>Housing units built between 2000 and 2004

[<img alt="../../_images/us.census.acs.B25034003.png" src="../../_images/us.census.acs.B25034003.png" style="width: 100%;" />](../../_images/us.census.acs.B25034003.png)A house, an apartment, a mobile home or trailer, a group of rooms, or a single room occupied as separate living quarters, or if vacant, intended for occupancy as separate living quarters built from 2000 to 2004.

Measure &quot;Housing units built between 2000 and 2004&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25034003'
        );

Measure &quot;Housing units built between 2000 and 2004&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25034003'
        );

Measure &quot;Housing units built between 2000 and 2004&quot; percent of &quot;Housing Units&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25034003',
          'denominator'
        );

Measure &quot;Housing units built between 2000 and 2004&quot; percent of &quot;Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25034003',
          'denominator'
        );

* denominator: [Housing Units](#us-census-acs-b25001001)


### <a name="housing-units-built-in-2005-or-later"></a><a name="us-census-acs-b25034002"></a>Housing units built in 2005 or later

[<img alt="../../_images/us.census.acs.B25034002.png" src="../../_images/us.census.acs.B25034002.png" style="width: 100%;" />](../../_images/us.census.acs.B25034002.png)A house, an apartment, a mobile home or trailer, a group of rooms, or a single room occupied as separate living quarters, or if vacant, intended for occupancy as separate living quarters built in 2005 or later.

Measure &quot;Housing units built in 2005 or later&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25034002'
        );

Measure &quot;Housing units built in 2005 or later&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25034002'
        );

Measure &quot;Housing units built in 2005 or later&quot; percent of &quot;Housing Units&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25034002',
          'denominator'
        );

Measure &quot;Housing units built in 2005 or later&quot; percent of &quot;Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25034002',
          'denominator'
        );

* denominator: [Housing Units](#us-census-acs-b25001001)


### <a name="mobile-homes"></a><a name="us-census-acs-b25024010"></a>Mobile homes

[<img alt="../../_images/us.census.acs.B25024010.png" src="../../_images/us.census.acs.B25024010.png" style="width: 100%;" />](../../_images/us.census.acs.B25024010.png)A manufactured home is defined as a movable dwelling, 8 feet or more wide and 40 feet or more long, designed to be towed on its own chassis, with transportation gear integral to the unit when it leaves the factory, and without need of a permanent foundation. These homes are built in accordance with the U.S. Department of Housing and Urban Development (HUD) building code.

Measure &quot;Mobile homes&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25024010'
        );

Measure &quot;Mobile homes&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25024010'
        );

Measure &quot;Mobile homes&quot; percent of &quot;Housing Units&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25024010',
          'denominator'
        );

Measure &quot;Mobile homes&quot; percent of &quot;Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25024010',
          'denominator'
        );

* denominator: [Housing Units](#us-census-acs-b25001001)


### <a name="owner-occupied-housing-units"></a><a name="us-census-acs-b25075001"></a>Owner-occupied Housing Units

[<img alt="../../_images/us.census.acs.B25075001.png" src="../../_images/us.census.acs.B25075001.png" style="width: 100%;" />](../../_images/us.census.acs.B25075001.png)Measure &quot;Owner-occupied Housing Units&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25075001'
        );

Measure &quot;Owner-occupied Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25075001'
        );

Measure &quot;Owner-occupied Housing Units&quot; percent of &quot;Housing Units&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25075001',
          'denominator'
        );

Measure &quot;Owner-occupied Housing Units&quot; percent of &quot;Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25075001',
          'denominator'
        );

* denominator: [Housing Units](#us-census-acs-b25001001)

Subcolumns of Owner-occupied Housing Units

- [Owner-occupied Housing Units valued at $1,000,000 or more.](#owner-occupied-housing-units-valued-at-1-000-000-or-more)

- [Owner-occupied Housing Units with a Mortgage](#owner-occupied-housing-units-with-a-mortgage)


#### <a name="owner-occupied-housing-units-valued-at-1-000-000-or-more"></a><a name="us-census-acs-b25075025"></a>Owner-occupied Housing Units valued at $1,000,000 or more.

[<img alt="../../_images/us.census.acs.B25075025.png" src="../../_images/us.census.acs.B25075025.png" style="width: 100%;" />](../../_images/us.census.acs.B25075025.png)The count of owner occupied housing units in a geographic area that are valued at $1,000,000 or more.  Value is the respondent's estimate of how much the property (house and lot, mobile home and lot, or condominium unit) would sell for if it were for sale.

Measure &quot;Owner-occupied Housing Units valued at $1,000,000 or more.&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25075025'
        );

Measure &quot;Owner-occupied Housing Units valued at $1,000,000 or more.&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25075025'
        );

Measure &quot;Owner-occupied Housing Units valued at $1,000,000 or more.&quot; percent of &quot;Owner-occupied Housing Units&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25075025',
          'denominator'
        );

Measure &quot;Owner-occupied Housing Units valued at $1,000,000 or more.&quot; percent of &quot;Owner-occupied Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25075025',
          'denominator'
        );

* denominator: [Owner-occupied Housing Units](#us-census-acs-b25075001)


#### <a name="owner-occupied-housing-units-with-a-mortgage"></a><a name="us-census-acs-b25081002"></a>Owner-occupied Housing Units with a Mortgage

[<img alt="../../_images/us.census.acs.B25081002.png" src="../../_images/us.census.acs.B25081002.png" style="width: 100%;" />](../../_images/us.census.acs.B25081002.png)The count of housing units within a geographic area that are mortagaged. &quot;Mortgage&quot; refers to all forms of debt where the property is pledged as security for repayment of the debt, including deeds of trust, trust deed, contracts to purchase, land contracts, junior mortgages, and home equity loans.

Measure &quot;Owner-occupied Housing Units with a Mortgage&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25081002'
        );

Measure &quot;Owner-occupied Housing Units with a Mortgage&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25081002'
        );

Measure &quot;Owner-occupied Housing Units with a Mortgage&quot; percent of &quot;Owner-occupied Housing Units&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25081002',
          'denominator'
        );

Measure &quot;Owner-occupied Housing Units with a Mortgage&quot; percent of &quot;Owner-occupied Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25081002',
          'denominator'
        );

* denominator: [Owner-occupied Housing Units](#us-census-acs-b25075001)


### <a name="renter-occupied-housing-units"></a><a name="us-census-acs-b25003001"></a>Renter occupied housing units

[<img alt="../../_images/us.census.acs.B25003001.png" src="../../_images/us.census.acs.B25003001.png" style="width: 100%;" />](../../_images/us.census.acs.B25003001.png)A housing unit is classified as occupied if it is the usual place of residence of the person or group of people living in it at the time of enumeration.

Measure &quot;Renter occupied housing units&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25003001'
        );

Measure &quot;Renter occupied housing units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25003001'
        );

Measure &quot;Renter occupied housing units&quot; percent of &quot;Housing Units&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25003001',
          'denominator'
        );

Measure &quot;Renter occupied housing units&quot; percent of &quot;Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25003001',
          'denominator'
        );

* denominator: [Housing Units](#us-census-acs-b25001001)

Subcolumns of Renter occupied housing units

- [Renter occupied housing units](#us-census-acs-b25003003)


#### <a name="us-census-acs-b25003003"></a><a name="id1"></a>Renter occupied housing units

[<img alt="../../_images/us.census.acs.B25003003.png" src="../../_images/us.census.acs.B25003003.png" style="width: 100%;" />](../../_images/us.census.acs.B25003003.png)All occupied units which are not owner occupied, whether they are rented for cash rent or occupied without payment of cash rent, are classified as renter-occupied.

Measure &quot;Renter occupied housing units&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25003003'
        );

Measure &quot;Renter occupied housing units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25003003'
        );

Measure &quot;Renter occupied housing units&quot; percent of &quot;Renter occupied housing units&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25003003',
          'denominator'
        );

Measure &quot;Renter occupied housing units&quot; percent of &quot;Renter occupied housing units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25003003',
          'denominator'
        );

* denominator: [Renter occupied housing units](#us-census-acs-b25003001)


### <a name="vacant-housing-units"></a><a name="us-census-acs-b25002003"></a>Vacant Housing Units

[<img alt="../../_images/us.census.acs.B25002003.png" src="../../_images/us.census.acs.B25002003.png" style="width: 100%;" />](../../_images/us.census.acs.B25002003.png)The count of vacant housing units in a geographic area. A housing unit is vacant if no one is living in it at the time of enumeration, unless its occupants are only temporarily absent. Units temporarily occupied at the time of enumeration entirely by people who have a usual residence elsewhere are also classified as vacant.

Measure &quot;Vacant Housing Units&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25002003'
        );

Measure &quot;Vacant Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25002003'
        );

Measure &quot;Vacant Housing Units&quot; percent of &quot;Housing Units&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25002003',
          'denominator'
        );

Measure &quot;Vacant Housing Units&quot; percent of &quot;Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25002003',
          'denominator'
        );

* denominator: [Housing Units](#us-census-acs-b25001001)

Subcolumns of Vacant Housing Units

- [Vacant Housing Units for Rent](#vacant-housing-units-for-rent)

- [Vacant Housing Units for Sale](#vacant-housing-units-for-sale)


#### <a name="vacant-housing-units-for-rent"></a><a name="us-census-acs-b25004002"></a>Vacant Housing Units for Rent

[<img alt="../../_images/us.census.acs.B25004002.png" src="../../_images/us.census.acs.B25004002.png" style="width: 100%;" />](../../_images/us.census.acs.B25004002.png)The count of vacant housing units in a geographic area that are for rent. A housing unit is vacant if no one is living in it at the time of enumeration, unless its occupants are only temporarily absent. Units temporarily occupied at the time of enumeration entirely by people who have a usual residence elsewhere are also classified as vacant.

Measure &quot;Vacant Housing Units for Rent&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25004002'
        );

Measure &quot;Vacant Housing Units for Rent&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25004002'
        );

Measure &quot;Vacant Housing Units for Rent&quot; percent of &quot;Vacant Housing Units&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25004002',
          'denominator'
        );

Measure &quot;Vacant Housing Units for Rent&quot; percent of &quot;Vacant Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25004002',
          'denominator'
        );

* denominator: [Vacant Housing Units](#us-census-acs-b25002003)


#### <a name="vacant-housing-units-for-sale"></a><a name="us-census-acs-b25004004"></a>Vacant Housing Units for Sale

[<img alt="../../_images/us.census.acs.B25004004.png" src="../../_images/us.census.acs.B25004004.png" style="width: 100%;" />](../../_images/us.census.acs.B25004004.png)The count of vacant housing units in a geographic area that are for sale. A housing unit is vacant if no one is living in it at the time of enumeration, unless its occupants are only temporarily absent. Units temporarily occupied at the time of enumeration entirely by people who have a usual residence elsewhere are also classified as vacant.

Measure &quot;Vacant Housing Units for Sale&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25004004'
        );

Measure &quot;Vacant Housing Units for Sale&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25004004'
        );

Measure &quot;Vacant Housing Units for Sale&quot; percent of &quot;Vacant Housing Units&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25004004',
          'denominator'
        );

Measure &quot;Vacant Housing Units for Sale&quot; percent of &quot;Vacant Housing Units&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B25004004',
          'denominator'
        );

* denominator: [Vacant Housing Units](#us-census-acs-b25002003)


## <a name="lived-in-a-different-house-one-year-ago-in-a-different-city"></a><a name="us-census-acs-b07204007"></a>Lived in a different house one year ago in a different city

[<img alt="../../_images/us.census.acs.B07204007.png" src="../../_images/us.census.acs.B07204007.png" style="width: 100%;" />](../../_images/us.census.acs.B07204007.png)All people in a geographic area who lived in a different city within the year prior to the survey.

Measure &quot;Lived in a different house one year ago in a different city&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B07204007'
        );

Measure &quot;Lived in a different house one year ago in a different city&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B07204007'
        );

Measure &quot;Lived in a different house one year ago in a different city&quot; percent of &quot;Population 1 year and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B07204007',
          'denominator'
        );

Measure &quot;Lived in a different house one year ago in a different city&quot; percent of &quot;Population 1 year and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B07204007',
          'denominator'
        );

* denominator: [Population 1 year and over](../age_gender/#us-census-acs-b07204001)


## <a name="lived-in-a-different-house-one-year-ago-in-the-same-city"></a><a name="us-census-acs-b07204004"></a>Lived in a different house one year ago in the same city

[<img alt="../../_images/us.census.acs.B07204004.png" src="../../_images/us.census.acs.B07204004.png" style="width: 100%;" />](../../_images/us.census.acs.B07204004.png)All people in a geographic area who lived in the same city but moved to a different unit within the year prior to the survey.

Measure &quot;Lived in a different house one year ago in the same city&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B07204004'
        );

Measure &quot;Lived in a different house one year ago in the same city&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B07204004'
        );

Measure &quot;Lived in a different house one year ago in the same city&quot; percent of &quot;Population 1 year and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B07204004',
          'denominator'
        );

Measure &quot;Lived in a different house one year ago in the same city&quot; percent of &quot;Population 1 year and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'us.census.acs.B07204004',
          'denominator'
        );

* denominator: [Population 1 year and over](../age_gender/#us-census-acs-b07204001)


## <a name="median-rent"></a><a name="us-census-acs-b25058001"></a>Median Rent

[<img alt="../../_images/us.census.acs.B25058001.png" src="../../_images/us.census.acs.B25058001.png" style="width: 100%;" />](../../_images/us.census.acs.B25058001.png)The median contract rent within a geographic area. The contract rent is the monthly rent agreed to or contracted for, regardless of any furnishings, utilities, fees, meals, or services that may be included. For vacant units, it is the monthly rent asked for the rental unit at the time of interview.

Measure &quot;Median Rent&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25058001'
        );

Median Rent is only available for point lookups.


## <a name="median-rental-price-per-square-foot-for-all-homes"></a><a name="us-zillow-allhomes-medianrentalpricepersqft"></a>Median rental price per square foot for All homes

[<img alt="../../_images/us.zillow.AllHomes_MedianRentalPricePerSqft.png" src="../../_images/us.zillow.AllHomes_MedianRentalPricePerSqft.png" style="width: 100%;" />](../../_images/us.zillow.AllHomes_MedianRentalPricePerSqft.png)Median of the value (US Dollars) of all homes per square foot. This number is calculated by taking the estimated home value for each home in a given region and dividing it by the home's square footage. Zillow defines all homes as single-family, condominium and co-operative homes with a county record. Unless specified, all series cover this segment of the housing stock.

Measure &quot;Median rental price per square foot for All homes&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(28.3305906291771, -81.3544048197256),
          'us.zillow.AllHomes_MedianRentalPricePerSqft'
        );

Median rental price per square foot for All homes is only available for point lookups.


## <a name="median-rental-price-per-square-foot-for-single-family-residence-rental"></a><a name="us-zillow-sfr-medianrentalpricepersqft"></a>Median rental price per square foot for Single Family residence rental

[<img alt="../../_images/us.zillow.Sfr_MedianRentalPricePerSqft.png" src="../../_images/us.zillow.Sfr_MedianRentalPricePerSqft.png" style="width: 100%;" />](../../_images/us.zillow.Sfr_MedianRentalPricePerSqft.png)Median of the value (US Dollars) of all homes per square foot. This number is calculated by taking the estimated home value for each home in a given region and dividing it by the home's square footage. Single family residences are detached, free-standing residential buildings.

Measure &quot;Median rental price per square foot for Single Family residence rental&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(28.3305906291771, -81.3544048197256),
          'us.zillow.Sfr_MedianRentalPricePerSqft'
        );

Median rental price per square foot for Single Family residence rental is only available for point lookups.


## <a name="median-value-per-square-foot-for-all-homes"></a><a name="us-zillow-allhomes-medianvaluepersqft"></a>Median value per square foot for All homes

[<img alt="../../_images/us.zillow.AllHomes_MedianValuePerSqft.png" src="../../_images/us.zillow.AllHomes_MedianValuePerSqft.png" style="width: 100%;" />](../../_images/us.zillow.AllHomes_MedianValuePerSqft.png)Median of the estimated monthly rent price (US Dollars) of all homes, per square foot. This is calculated by taking the estimated rent price for a home and dividing it by the homes square footage. Zillow defines all homes as single-family, condominium and co-operative homes with a county record. Unless specified, all series cover this segment of the housing stock.

Measure &quot;Median value per square foot for All homes&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(28.3305906291771, -81.3544048197256),
          'us.zillow.AllHomes_MedianValuePerSqft'
        );

Median value per square foot for All homes is only available for point lookups.


## <a name="owner-occupied-housing-units-median-value"></a><a name="us-census-acs-b25077001"></a>Owner-Occupied Housing Units Median Value

[<img alt="../../_images/us.census.acs.B25077001.png" src="../../_images/us.census.acs.B25077001.png" style="width: 100%;" />](../../_images/us.census.acs.B25077001.png)The middle value (median) in a geographic area owner occupied housing units.

Measure &quot;Owner-Occupied Housing Units Median Value&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25077001'
        );

Owner-Occupied Housing Units Median Value is only available for point lookups.


## <a name="percent-of-household-income-spent-on-rent"></a><a name="us-census-acs-b25071001"></a>Percent of Household Income Spent on Rent

[<img alt="../../_images/us.census.acs.B25071001.png" src="../../_images/us.census.acs.B25071001.png" style="width: 100%;" />](../../_images/us.census.acs.B25071001.png)Within a geographic area, the median percentage of household income which was spent on gross rent.  Gross rent is the amount of the contract rent plus the estimated average monthly cost of utilities (electricity, gas, water, sewer etc.) and fuels (oil, coal, wood, etc.) if these are paid by the renter.  Household income is the sum of the income of all people 15 years and older living in the household.

Measure &quot;Percent of Household Income Spent on Rent&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.census.acs.B25071001'
        );

Percent of Household Income Spent on Rent is only available for point lookups.


## <a name="zillow-home-value-index-for-all-homes"></a><a name="us-zillow-allhomes-zhvi"></a>Zillow Home Value Index for All homes

[<img alt="../../_images/us.zillow.AllHomes_Zhvi.png" src="../../_images/us.zillow.AllHomes_Zhvi.png" style="width: 100%;" />](../../_images/us.zillow.AllHomes_Zhvi.png)The Zillow Home Value Index (ZHVI) is a time series tracking the monthly median home value (in US Dollars) in a particular geographical region. In general, each ZHVI time series begins in April 1996. See [Zillow's methodology](http://www.zillow.com/research/zhvi-methodology-6032/) for more information. Zillow defines all homes as single-family, condominium and co-operative homes with a county record. Unless specified, all series cover this segment of the housing stock.

Measure &quot;Zillow Home Value Index for All homes&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(28.3305906291771, -81.3544048197256),
          'us.zillow.AllHomes_Zhvi'
        );

Zillow Home Value Index for All homes is only available for point lookups.


## <a name="zillow-home-value-index-for-single-family-homes"></a><a name="us-zillow-singlefamilyresidence-zhvi"></a>Zillow Home Value Index for Single Family Homes

[<img alt="../../_images/us.zillow.SingleFamilyResidence_Zhvi.png" src="../../_images/us.zillow.SingleFamilyResidence_Zhvi.png" style="width: 100%;" />](../../_images/us.zillow.SingleFamilyResidence_Zhvi.png)The Zillow Home Value Index (ZHVI) is a time series tracking the monthly median home value (in US Dollars) in a particular geographical region. In general, each ZHVI time series begins in April 1996. See [Zillow's methodology](http://www.zillow.com/research/zhvi-methodology-6032/) for more information. Single family residences are detached, free-standing residential buildings.

Measure &quot;Zillow Home Value Index for Single Family Homes&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(28.3305906291771, -81.3544048197256),
          'us.zillow.SingleFamilyResidence_Zhvi'
        );

Zillow Home Value Index for Single Family Homes is only available for point lookups.


## <a name="zillow-rental-index-for-all-homes-plus-multifamily"></a><a name="us-zillow-allhomesplusmultifamily-zri"></a>Zillow Rental Index for All homes plus multifamily

[<img alt="../../_images/us.zillow.AllHomesPlusMultifamily_Zri.png" src="../../_images/us.zillow.AllHomesPlusMultifamily_Zri.png" style="width: 100%;" />](../../_images/us.zillow.AllHomesPlusMultifamily_Zri.png)Similar to Zillow's ZHVI, the Zillow Rent Index (ZRI) tracks the monthly median rent (in US Dollars) in different geographical regions. In general, each ZRI time series beginds in November, 2010.  See [Zillow's methodology](http://www.zillow.com/research/zillow-rent-index-methodology-2393/) for more information. In addition to &quot;All homes&quot;, which Zillow defines as single-family, condominium and co-operative homes with a county record, this group includes units in buildings with 5 or more housing units that are not a condominiums or co-ops.

Measure &quot;Zillow Rental Index for All homes plus multifamily&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(28.3305906291771, -81.3544048197256),
          'us.zillow.AllHomesPlusMultifamily_Zri'
        );

Zillow Rental Index for All homes plus multifamily is only available for point lookups.


## <a name="zillow-rental-index-for-single-family-residence-rental"></a><a name="us-zillow-singlefamilyresidencerental-zri"></a>Zillow Rental Index for Single Family residence rental

[<img alt="../../_images/us.zillow.SingleFamilyResidenceRental_Zri.png" src="../../_images/us.zillow.SingleFamilyResidenceRental_Zri.png" style="width: 100%;" />](../../_images/us.zillow.SingleFamilyResidenceRental_Zri.png)Similar to Zillow's ZHVI, the Zillow Rent Index (ZRI) tracks the monthly median rent (in US Dollars) in different geographical regions. In general, each ZRI time series beginds in November, 2010.  See [Zillow's methodology](http://www.zillow.com/research/zillow-rent-index-methodology-2393/) for more information. Single Family residence rental is defined as detached, free-standing residential buildings which are rented out.

Measure &quot;Zillow Rental Index for Single Family residence rental&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(28.3305906291771, -81.3544048197256),
          'us.zillow.SingleFamilyResidenceRental_Zri'
        );

Zillow Rental Index for Single Family residence rental is only available for point lookups.


