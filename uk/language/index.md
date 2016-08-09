

  
# <a name="language"></a>Language

What languages people speak.

- [Population whose main language is Arabic](#population-whose-main-language-is-arabic)

- [Population whose main language is Bengali (with Sylheti and Chatgaya)](#population-whose-main-language-is-bengali-with-sylheti-and-chatgaya)

- [Population whose main language is Chinese](#population-whose-main-language-is-chinese)

- [Population whose main language is English (English or Welsh if in Wales)](#population-whose-main-language-is-english-english-or-welsh-if-in-wales)

- [Population whose main language is French](#population-whose-main-language-is-french)

- [Population whose main language is Gujarati](#population-whose-main-language-is-gujarati)

- [Population whose main language is Panjabi](#population-whose-main-language-is-panjabi)

- [Population whose main language is Polish](#population-whose-main-language-is-polish)

- [Population whose main language is Portuguese](#population-whose-main-language-is-portuguese)

- [Population whose main language is Spanish](#population-whose-main-language-is-spanish)

- [Population whose main language is Tamil](#population-whose-main-language-is-tamil)

- [Population whose main language is Urdu](#population-whose-main-language-is-urdu)


## <a name="population-whose-main-language-is-arabic"></a><a name="uk-ons-lc2104ew0010"></a>Population whose main language is Arabic

Measure &quot;Population whose main language is Arabic&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0010'
        );

Measure &quot;Population whose main language is Arabic&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0010'
        );

Measure &quot;Population whose main language is Arabic&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0010',
          'denominator'
        );

Measure &quot;Population whose main language is Arabic&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0010',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="population-whose-main-language-is-bengali-with-sylheti-and-chatgaya"></a><a name="uk-ons-lc2104ew0015"></a>Population whose main language is Bengali (with Sylheti and Chatgaya)

Measure &quot;Population whose main language is Bengali (with Sylheti and Chatgaya)&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0015'
        );

Measure &quot;Population whose main language is Bengali (with Sylheti and Chatgaya)&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0015'
        );

Measure &quot;Population whose main language is Bengali (with Sylheti and Chatgaya)&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0015',
          'denominator'
        );

Measure &quot;Population whose main language is Bengali (with Sylheti and Chatgaya)&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0015',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="population-whose-main-language-is-chinese"></a><a name="uk-ons-lc2104ew0020"></a>Population whose main language is Chinese

Measure &quot;Population whose main language is Chinese&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0020'
        );

Measure &quot;Population whose main language is Chinese&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0020'
        );

Measure &quot;Population whose main language is Chinese&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0020',
          'denominator'
        );

Measure &quot;Population whose main language is Chinese&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0020',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="population-whose-main-language-is-english-english-or-welsh-if-in-wales"></a><a name="uk-ons-lc2104ew0002"></a>Population whose main language is English (English or Welsh if in Wales)

Measure &quot;Population whose main language is English (English or Welsh if in Wales)&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0002'
        );

Measure &quot;Population whose main language is English (English or Welsh if in Wales)&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0002'
        );

Measure &quot;Population whose main language is English (English or Welsh if in Wales)&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0002',
          'denominator'
        );

Measure &quot;Population whose main language is English (English or Welsh if in Wales)&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0002',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="population-whose-main-language-is-french"></a><a name="uk-ons-lc2104ew0003"></a>Population whose main language is French

Measure &quot;Population whose main language is French&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0003'
        );

Measure &quot;Population whose main language is French&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0003'
        );

Measure &quot;Population whose main language is French&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0003',
          'denominator'
        );

Measure &quot;Population whose main language is French&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0003',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="population-whose-main-language-is-gujarati"></a><a name="uk-ons-lc2104ew0016"></a>Population whose main language is Gujarati

Measure &quot;Population whose main language is Gujarati&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0016'
        );

Measure &quot;Population whose main language is Gujarati&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0016'
        );

Measure &quot;Population whose main language is Gujarati&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0016',
          'denominator'
        );

Measure &quot;Population whose main language is Gujarati&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0016',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="population-whose-main-language-is-panjabi"></a><a name="uk-ons-lc2104ew0013"></a>Population whose main language is Panjabi

Measure &quot;Population whose main language is Panjabi&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0013'
        );

Measure &quot;Population whose main language is Panjabi&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0013'
        );

Measure &quot;Population whose main language is Panjabi&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0013',
          'denominator'
        );

Measure &quot;Population whose main language is Panjabi&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0013',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="population-whose-main-language-is-polish"></a><a name="uk-ons-lc2104ew0007"></a>Population whose main language is Polish

Measure &quot;Population whose main language is Polish&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0007'
        );

Measure &quot;Population whose main language is Polish&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0007'
        );

Measure &quot;Population whose main language is Polish&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0007',
          'denominator'
        );

Measure &quot;Population whose main language is Polish&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0007',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="population-whose-main-language-is-portuguese"></a><a name="uk-ons-lc2104ew0004"></a>Population whose main language is Portuguese

Measure &quot;Population whose main language is Portuguese&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0004'
        );

Measure &quot;Population whose main language is Portuguese&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0004'
        );

Measure &quot;Population whose main language is Portuguese&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0004',
          'denominator'
        );

Measure &quot;Population whose main language is Portuguese&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0004',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="population-whose-main-language-is-spanish"></a><a name="uk-ons-lc2104ew0005"></a>Population whose main language is Spanish

Measure &quot;Population whose main language is Spanish&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0005'
        );

Measure &quot;Population whose main language is Spanish&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0005'
        );

Measure &quot;Population whose main language is Spanish&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0005',
          'denominator'
        );

Measure &quot;Population whose main language is Spanish&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0005',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="population-whose-main-language-is-tamil"></a><a name="uk-ons-lc2104ew0017"></a>Population whose main language is Tamil

Measure &quot;Population whose main language is Tamil&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0017'
        );

Measure &quot;Population whose main language is Tamil&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0017'
        );

Measure &quot;Population whose main language is Tamil&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0017',
          'denominator'
        );

Measure &quot;Population whose main language is Tamil&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0017',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


## <a name="population-whose-main-language-is-urdu"></a><a name="uk-ons-lc2104ew0014"></a>Population whose main language is Urdu

Measure &quot;Population whose main language is Urdu&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0014'
        );

Measure &quot;Population whose main language is Urdu&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0014'
        );

Measure &quot;Population whose main language is Urdu&quot; percent of &quot;Population age 3 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2104EW0014',
          'denominator'
        );

Measure &quot;Population whose main language is Urdu&quot; percent of &quot;Population age 3 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2104EW0014',
          'denominator'
        );

* denominator: [Population age 3 and over](../age_gender/#uk-ons-lc2104ew0001)


