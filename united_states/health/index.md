

  
# <a name="health"></a>Health

Breakdowns of the population by health

- [Female life expectancy](#female-life-expectancy)

- [Female obesity prevalence](#female-obesity-prevalence)

- [Female sufficient physical activity prevalence](#female-sufficient-physical-activity-prevalence)

- [Male life expectancy](#male-life-expectancy)

- [Male obesity prevalence](#male-obesity-prevalence)

- [Male sufficient physical activity prevalence](#male-sufficient-physical-activity-prevalence)


## <a name="female-life-expectancy"></a><a name="us-ihme-female-life-expectancy"></a>Female life expectancy

[<img alt="../../_images/us.ihme.female_life_expectancy.png" src="../../_images/us.ihme.female_life_expectancy.png" style="width: 100%;" />](../../_images/us.ihme.female_life_expectancy.png)Measure &quot;Female life expectancy&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.ihme.female_life_expectancy'
        );

Female life expectancy is only available for point lookups.


## <a name="female-obesity-prevalence"></a><a name="us-ihme-female-obesity"></a>Female obesity prevalence

[<img alt="../../_images/us.ihme.female_obesity.png" src="../../_images/us.ihme.female_obesity.png" style="width: 100%;" />](../../_images/us.ihme.female_obesity.png)Measure &quot;Female obesity prevalence&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.ihme.female_obesity'
        );

Female obesity prevalence is only available for point lookups.


## <a name="female-sufficient-physical-activity-prevalence"></a><a name="us-ihme-female-physical-activity"></a>Female sufficient physical activity prevalence

[<img alt="../../_images/us.ihme.female_physical_activity.png" src="../../_images/us.ihme.female_physical_activity.png" style="width: 100%;" />](../../_images/us.ihme.female_physical_activity.png)Measure &quot;Female sufficient physical activity prevalence&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.ihme.female_physical_activity'
        );

Female sufficient physical activity prevalence is only available for point lookups.


## <a name="male-life-expectancy"></a><a name="us-ihme-male-life-expectancy"></a>Male life expectancy

[<img alt="../../_images/us.ihme.male_life_expectancy.png" src="../../_images/us.ihme.male_life_expectancy.png" style="width: 100%;" />](../../_images/us.ihme.male_life_expectancy.png)Measure &quot;Male life expectancy&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.ihme.male_life_expectancy'
        );

Male life expectancy is only available for point lookups.


## <a name="male-obesity-prevalence"></a><a name="us-ihme-male-obesity"></a>Male obesity prevalence

[<img alt="../../_images/us.ihme.male_obesity.png" src="../../_images/us.ihme.male_obesity.png" style="width: 100%;" />](../../_images/us.ihme.male_obesity.png)Measure &quot;Male obesity prevalence&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.ihme.male_obesity'
        );

Male obesity prevalence is only available for point lookups.


## <a name="male-sufficient-physical-activity-prevalence"></a><a name="us-ihme-male-physical-activity"></a>Male sufficient physical activity prevalence

[<img alt="../../_images/us.ihme.male_physical_activity.png" src="../../_images/us.ihme.male_physical_activity.png" style="width: 100%;" />](../../_images/us.ihme.male_physical_activity.png)Measure &quot;Male sufficient physical activity prevalence&quot;  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'us.ihme.male_physical_activity'
        );

Male sufficient physical activity prevalence is only available for point lookups.


