

  
# <a name="religion"></a>Religion

Breakdowns of the population by religion.

- [Catholic population](#catholic-population)

- [Population with no religion](#population-with-no-religion)

- [Population with other religion](#population-with-other-religion)

- [Protestant and evangelical population](#protestant-and-evangelical-population)


## <a name="catholic-population"></a><a name="mx-inegi-columns-relig1"></a>Catholic population

[<img alt="../../_images/mx.inegi_columns.RELIG1.png" src="../../_images/mx.inegi_columns.RELIG1.png" style="width: 100%;" />](../../_images/mx.inegi_columns.RELIG1.png)Measure &quot;Catholic population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.RELIG1'
        );

Measure &quot;Catholic population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.RELIG1'
        );

Measure &quot;Catholic population&quot; percent of &quot;Total population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.RELIG1',
          'denominator'
        );

Measure &quot;Catholic population&quot; percent of &quot;Total population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.RELIG1',
          'denominator'
        );

* denominator: [Total population](../age_gender/#mx-inegi-columns-pob1)


## <a name="population-with-no-religion"></a><a name="mx-inegi-columns-relig4"></a>Population with no religion

[<img alt="../../_images/mx.inegi_columns.RELIG4.png" src="../../_images/mx.inegi_columns.RELIG4.png" style="width: 100%;" />](../../_images/mx.inegi_columns.RELIG4.png)Measure &quot;Population with no religion&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.RELIG4'
        );

Measure &quot;Population with no religion&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.RELIG4'
        );

Measure &quot;Population with no religion&quot; percent of &quot;Total population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.RELIG4',
          'denominator'
        );

Measure &quot;Population with no religion&quot; percent of &quot;Total population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.RELIG4',
          'denominator'
        );

* denominator: [Total population](../age_gender/#mx-inegi-columns-pob1)


## <a name="population-with-other-religion"></a><a name="mx-inegi-columns-relig3"></a>Population with other religion

[<img alt="../../_images/mx.inegi_columns.RELIG3.png" src="../../_images/mx.inegi_columns.RELIG3.png" style="width: 100%;" />](../../_images/mx.inegi_columns.RELIG3.png)Measure &quot;Population with other religion&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.RELIG3'
        );

Measure &quot;Population with other religion&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.RELIG3'
        );

Measure &quot;Population with other religion&quot; percent of &quot;Total population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.RELIG3',
          'denominator'
        );

Measure &quot;Population with other religion&quot; percent of &quot;Total population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.RELIG3',
          'denominator'
        );

* denominator: [Total population](../age_gender/#mx-inegi-columns-pob1)


## <a name="protestant-and-evangelical-population"></a><a name="mx-inegi-columns-relig2"></a>Protestant and evangelical population

[<img alt="../../_images/mx.inegi_columns.RELIG2.png" src="../../_images/mx.inegi_columns.RELIG2.png" style="width: 100%;" />](../../_images/mx.inegi_columns.RELIG2.png)Measure &quot;Protestant and evangelical population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.RELIG2'
        );

Measure &quot;Protestant and evangelical population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.RELIG2'
        );

Measure &quot;Protestant and evangelical population&quot; percent of &quot;Total population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.RELIG2',
          'denominator'
        );

Measure &quot;Protestant and evangelical population&quot; percent of &quot;Total population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.RELIG2',
          'denominator'
        );

* denominator: [Total population](../age_gender/#mx-inegi-columns-pob1)


