

  
# <a name="zillow-home-value-and-rental-indexes"></a>Zillow Home Value and Rental Indexes

Zillow home value and rental indexes.

- [Median rental price per square foot for All homes](#median-rental-price-per-square-foot-for-all-homes)

- [Median rental price per square foot for Single Family residence rental](#median-rental-price-per-square-foot-for-single-family-residence-rental)

- [Median value per square foot for All homes](#median-value-per-square-foot-for-all-homes)

- [Zillow Home Value Index for All homes](#zillow-home-value-index-for-all-homes)

- [Zillow Home Value Index for Single Family Homes](#zillow-home-value-index-for-single-family-homes)

- [Zillow Rental Index for All homes plus multifamily](#zillow-rental-index-for-all-homes-plus-multifamily)

- [Zillow Rental Index for Single Family residence rental](#zillow-rental-index-for-single-family-residence-rental)


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


