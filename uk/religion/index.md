

  
# <a name="religion"></a>Religion

Breakdowns of the population by religion.

- [Buddhist Population](#buddhist-population)

- [Christian Population](#christian-population)

- [Hindu Population](#hindu-population)

- [Jewish Population](#jewish-population)

- [Muslim Population](#muslim-population)

- [Population who are not religious](#population-who-are-not-religious)

- [Sikh Population](#sikh-population)


## <a name="buddhist-population"></a><a name="uk-ons-lc2107ew0003"></a>Buddhist Population

Measure &quot;Buddhist Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0003'
        );

Measure &quot;Buddhist Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0003'
        );

Measure &quot;Buddhist Population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0003',
          'denominator'
        );

Measure &quot;Buddhist Population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0003',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


## <a name="christian-population"></a><a name="uk-ons-lc2107ew0002"></a>Christian Population

Measure &quot;Christian Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0002'
        );

Measure &quot;Christian Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0002'
        );

Measure &quot;Christian Population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0002',
          'denominator'
        );

Measure &quot;Christian Population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0002',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


## <a name="hindu-population"></a><a name="uk-ons-lc2107ew0004"></a>Hindu Population

Measure &quot;Hindu Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0004'
        );

Measure &quot;Hindu Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0004'
        );

Measure &quot;Hindu Population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0004',
          'denominator'
        );

Measure &quot;Hindu Population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0004',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


## <a name="jewish-population"></a><a name="uk-ons-lc2107ew0005"></a>Jewish Population

Measure &quot;Jewish Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0005'
        );

Measure &quot;Jewish Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0005'
        );

Measure &quot;Jewish Population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0005',
          'denominator'
        );

Measure &quot;Jewish Population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0005',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


## <a name="muslim-population"></a><a name="uk-ons-lc2107ew0006"></a>Muslim Population

Measure &quot;Muslim Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0006'
        );

Measure &quot;Muslim Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0006'
        );

Measure &quot;Muslim Population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0006',
          'denominator'
        );

Measure &quot;Muslim Population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0006',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


## <a name="population-who-are-not-religious"></a><a name="uk-ons-lc2107ew0009"></a>Population who are not religious

Measure &quot;Population who are not religious&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0009'
        );

Measure &quot;Population who are not religious&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0009'
        );

Measure &quot;Population who are not religious&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0009',
          'denominator'
        );

Measure &quot;Population who are not religious&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0009',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


## <a name="sikh-population"></a><a name="uk-ons-lc2107ew0007"></a>Sikh Population

Measure &quot;Sikh Population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0007'
        );

Measure &quot;Sikh Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0007'
        );

Measure &quot;Sikh Population&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2107EW0007',
          'denominator'
        );

Measure &quot;Sikh Population&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2107EW0007',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


