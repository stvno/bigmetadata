

  
# <a name="health"></a>Health

Breakdowns of the population by health

- [Bad or very bad health](#bad-or-very-bad-health)

- [Day-to-day activities not limited](#day-to-day-activities-not-limited)

- [Day-to-day activities somewhat limited](#day-to-day-activities-somewhat-limited)

- [Day-to-day activities very limited](#day-to-day-activities-very-limited)

- [Fair health](#fair-health)

- [Very good or good health](#very-good-or-good-health)


## <a name="bad-or-very-bad-health"></a><a name="uk-ons-lc3202wa0010"></a>Bad or very bad health

Measure &quot;Bad or very bad health&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC3202WA0010'
        );

Measure &quot;Bad or very bad health&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC3202WA0010'
        );

Measure &quot;Bad or very bad health&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC3202WA0010',
          'denominator'
        );

Measure &quot;Bad or very bad health&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC3202WA0010',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="day-to-day-activities-not-limited"></a><a name="uk-ons-lc3204wa0010"></a>Day-to-day activities not limited

Measure &quot;Day-to-day activities not limited&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC3204WA0010'
        );

Measure &quot;Day-to-day activities not limited&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC3204WA0010'
        );

Measure &quot;Day-to-day activities not limited&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC3204WA0010',
          'denominator'
        );

Measure &quot;Day-to-day activities not limited&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC3204WA0010',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="day-to-day-activities-somewhat-limited"></a><a name="uk-ons-lc3204wa0007"></a>Day-to-day activities somewhat limited

Measure &quot;Day-to-day activities somewhat limited&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC3204WA0007'
        );

Measure &quot;Day-to-day activities somewhat limited&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC3204WA0007'
        );

Measure &quot;Day-to-day activities somewhat limited&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC3204WA0007',
          'denominator'
        );

Measure &quot;Day-to-day activities somewhat limited&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC3204WA0007',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="day-to-day-activities-very-limited"></a><a name="uk-ons-lc3204wa0004"></a>Day-to-day activities very limited

Measure &quot;Day-to-day activities very limited&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC3204WA0004'
        );

Measure &quot;Day-to-day activities very limited&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC3204WA0004'
        );

Measure &quot;Day-to-day activities very limited&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC3204WA0004',
          'denominator'
        );

Measure &quot;Day-to-day activities very limited&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC3204WA0004',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="fair-health"></a><a name="uk-ons-lc3202wa0007"></a>Fair health

Measure &quot;Fair health&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC3202WA0007'
        );

Measure &quot;Fair health&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC3202WA0007'
        );

Measure &quot;Fair health&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC3202WA0007',
          'denominator'
        );

Measure &quot;Fair health&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC3202WA0007',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="very-good-or-good-health"></a><a name="uk-ons-lc3202wa0004"></a>Very good or good health

Measure &quot;Very good or good health&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC3202WA0004'
        );

Measure &quot;Very good or good health&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC3202WA0004'
        );

Measure &quot;Very good or good health&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC3202WA0004',
          'denominator'
        );

Measure &quot;Very good or good health&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC3202WA0004',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


