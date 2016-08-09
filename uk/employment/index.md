

  
# <a name="employment"></a>Employment

How people are employed.

- [Population age 16 and over](#population-age-16-and-over)

    * [Economically active](#economically-active)

        - [Employed population](#employed-population)

        - [Employed as an employee](#employed-as-an-employee)

        - [Full-time employees](#full-time-employees)

        - [Part-time employees](#part-time-employees)

        - [Full-time students](#full-time-students)

        - [Self-employed](#self-employed)

        - [Self-employed full-time](#self-employed-full-time)

        - [Self-employed part-time](#self-employed-part-time)

        - [Unemployed](#unemployed)

        - [Full-time students, unemployed](#full-time-students-unemployed)

        - [Unemployed (excluding full-time students)](#unemployed-excluding-full-time-students)

    * [Economically inactive](#economically-inactive)

        - [Economically inactive students](#economically-inactive-students)

        - [Long-term sick or disabled](#long-term-sick-or-disabled)

        - [Looking after home or family](#looking-after-home-or-family)

        - [Retired population](#retired-population)

    * [Higher managerial, administrative and professional occupations](#higher-managerial-administrative-and-professional-occupations)

        - [Higher professional occupations](#higher-professional-occupations)

        - [Large employers and higher managerial and administrative occupations](#large-employers-and-higher-managerial-and-administrative-occupations)

    * [Intermediate occupations](#intermediate-occupations)

    * [Lower managerial, administrative and professional occupations](#lower-managerial-administrative-and-professional-occupations)

    * [Lower supervisory and technical occupations](#lower-supervisory-and-technical-occupations)

    * [Never worked and long-term unemployed](#never-worked-and-long-term-unemployed)

        - [Long-term unemployed](#long-term-unemployed)

        - [Never worked](#never-worked)

    * [Routine occupations](#routine-occupations)

    * [Semi-routine occupations](#semi-routine-occupations)

    * [Small employers and account workers](#small-employers-and-account-workers)


## <a name="population-age-16-and-over"></a><a name="uk-ons-lc6201ew0001"></a>Population age 16 and over

Measure &quot;Population age 16 and over&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0001'
        );

Measure &quot;Population age 16 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0001'
        );

Subcolumns of Population age 16 and over

- [Economically active](#economically-active)

- [Economically inactive](#economically-inactive)

- [Higher managerial, administrative and professional occupations](#higher-managerial-administrative-and-professional-occupations)

- [Intermediate occupations](#intermediate-occupations)

- [Lower managerial, administrative and professional occupations](#lower-managerial-administrative-and-professional-occupations)

- [Lower supervisory and technical occupations](#lower-supervisory-and-technical-occupations)

- [Never worked and long-term unemployed](#never-worked-and-long-term-unemployed)

- [Routine occupations](#routine-occupations)

- [Semi-routine occupations](#semi-routine-occupations)

- [Small employers and account workers](#small-employers-and-account-workers)


### <a name="economically-active"></a><a name="uk-ons-lc6201ew0010"></a>Economically active

Measure &quot;Economically active&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0010'
        );

Measure &quot;Economically active&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0010'
        );

Measure &quot;Economically active&quot; percent of &quot;Population age 16 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0010',
          'denominator'
        );

Measure &quot;Economically active&quot; percent of &quot;Population age 16 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0010',
          'denominator'
        );

* denominator: [Population age 16 and over](#uk-ons-lc6201ew0001)

Subcolumns of Economically active

- [Employed population](#employed-population)

- [Employed as an employee](#employed-as-an-employee)

- [Full-time employees](#full-time-employees)

- [Part-time employees](#part-time-employees)

- [Full-time students](#full-time-students)

- [Self-employed](#self-employed)

- [Self-employed full-time](#self-employed-full-time)

- [Self-employed part-time](#self-employed-part-time)

- [Unemployed](#unemployed)

- [Full-time students, unemployed](#full-time-students-unemployed)

- [Unemployed (excluding full-time students)](#unemployed-excluding-full-time-students)


#### <a name="employed-population"></a><a name="uk-ons-lc6201ew0019"></a>Employed population

Measure &quot;Employed population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0019'
        );

Measure &quot;Employed population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0019'
        );

Measure &quot;Employed population&quot; percent of &quot;Economically active&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0019',
          'denominator'
        );

Measure &quot;Employed population&quot; percent of &quot;Economically active&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0019',
          'denominator'
        );

* denominator: [Economically active](#uk-ons-lc6201ew0010)


#### <a name="employed-as-an-employee"></a><a name="uk-ons-lc6201ew0028"></a>Employed as an employee

Measure &quot;Employed as an employee&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0028'
        );

Measure &quot;Employed as an employee&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0028'
        );

Measure &quot;Employed as an employee&quot; percent of &quot;Employed population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0028',
          'denominator'
        );

Measure &quot;Employed as an employee&quot; percent of &quot;Employed population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0028',
          'denominator'
        );

* denominator: [Employed population](#uk-ons-lc6201ew0019)


#### <a name="full-time-employees"></a><a name="uk-ons-lc6201ew0046"></a>Full-time employees

Measure &quot;Full-time employees&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0046'
        );

Measure &quot;Full-time employees&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0046'
        );

Measure &quot;Full-time employees&quot; percent of &quot;Employed as an employee&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0046',
          'denominator'
        );

Measure &quot;Full-time employees&quot; percent of &quot;Employed as an employee&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0046',
          'denominator'
        );

* denominator: [Employed as an employee](#uk-ons-lc6201ew0028)


#### <a name="part-time-employees"></a><a name="uk-ons-lc6201ew0037"></a>Part-time employees

Measure &quot;Part-time employees&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0037'
        );

Measure &quot;Part-time employees&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0037'
        );

Measure &quot;Part-time employees&quot; percent of &quot;Employed as an employee&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0037',
          'denominator'
        );

Measure &quot;Part-time employees&quot; percent of &quot;Employed as an employee&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0037',
          'denominator'
        );

* denominator: [Employed as an employee](#uk-ons-lc6201ew0028)


#### <a name="full-time-students"></a><a name="uk-ons-lc6201ew0082"></a>Full-time students

Measure &quot;Full-time students&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0082'
        );

Measure &quot;Full-time students&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0082'
        );

Measure &quot;Full-time students&quot; percent of &quot;Employed population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0082',
          'denominator'
        );

Measure &quot;Full-time students&quot; percent of &quot;Employed population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0082',
          'denominator'
        );

* denominator: [Employed population](#uk-ons-lc6201ew0019)


#### <a name="self-employed"></a><a name="uk-ons-lc6201ew0055"></a>Self-employed

Measure &quot;Self-employed&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0055'
        );

Measure &quot;Self-employed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0055'
        );

Measure &quot;Self-employed&quot; percent of &quot;Employed population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0055',
          'denominator'
        );

Measure &quot;Self-employed&quot; percent of &quot;Employed population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0055',
          'denominator'
        );

* denominator: [Employed population](#uk-ons-lc6201ew0019)


#### <a name="self-employed-full-time"></a><a name="uk-ons-lc6201ew0073"></a>Self-employed full-time

Measure &quot;Self-employed full-time&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0073'
        );

Measure &quot;Self-employed full-time&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0073'
        );

Measure &quot;Self-employed full-time&quot; percent of &quot;Self-employed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0073',
          'denominator'
        );

Measure &quot;Self-employed full-time&quot; percent of &quot;Self-employed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0073',
          'denominator'
        );

* denominator: [Self-employed](#uk-ons-lc6201ew0055)


#### <a name="self-employed-part-time"></a><a name="uk-ons-lc6201ew0064"></a>Self-employed part-time

Measure &quot;Self-employed part-time&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0064'
        );

Measure &quot;Self-employed part-time&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0064'
        );

Measure &quot;Self-employed part-time&quot; percent of &quot;Self-employed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0064',
          'denominator'
        );

Measure &quot;Self-employed part-time&quot; percent of &quot;Self-employed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0064',
          'denominator'
        );

* denominator: [Self-employed](#uk-ons-lc6201ew0055)


#### <a name="unemployed"></a><a name="uk-ons-lc6201ew0091"></a>Unemployed

Measure &quot;Unemployed&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0091'
        );

Measure &quot;Unemployed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0091'
        );

Measure &quot;Unemployed&quot; percent of &quot;Economically active&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0091',
          'denominator'
        );

Measure &quot;Unemployed&quot; percent of &quot;Economically active&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0091',
          'denominator'
        );

* denominator: [Economically active](#uk-ons-lc6201ew0010)


#### <a name="full-time-students-unemployed"></a><a name="uk-ons-lc6201ew0109"></a>Full-time students, unemployed

Measure &quot;Full-time students, unemployed&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0109'
        );

Measure &quot;Full-time students, unemployed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0109'
        );

Measure &quot;Full-time students, unemployed&quot; percent of &quot;Unemployed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0109',
          'denominator'
        );

Measure &quot;Full-time students, unemployed&quot; percent of &quot;Unemployed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0109',
          'denominator'
        );

* denominator: [Unemployed](#uk-ons-lc6201ew0091)


#### <a name="unemployed-excluding-full-time-students"></a><a name="uk-ons-lc6201ew0100"></a>Unemployed (excluding full-time students)

Measure &quot;Unemployed (excluding full-time students)&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0100'
        );

Measure &quot;Unemployed (excluding full-time students)&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0100'
        );

Measure &quot;Unemployed (excluding full-time students)&quot; percent of &quot;Unemployed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0100',
          'denominator'
        );

Measure &quot;Unemployed (excluding full-time students)&quot; percent of &quot;Unemployed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0100',
          'denominator'
        );

* denominator: [Unemployed](#uk-ons-lc6201ew0091)


### <a name="economically-inactive"></a><a name="uk-ons-lc6201ew0118"></a>Economically inactive

Measure &quot;Economically inactive&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0118'
        );

Measure &quot;Economically inactive&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0118'
        );

Measure &quot;Economically inactive&quot; percent of &quot;Population age 16 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0118',
          'denominator'
        );

Measure &quot;Economically inactive&quot; percent of &quot;Population age 16 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0118',
          'denominator'
        );

* denominator: [Population age 16 and over](#uk-ons-lc6201ew0001)

Subcolumns of Economically inactive

- [Economically inactive students](#economically-inactive-students)

- [Long-term sick or disabled](#long-term-sick-or-disabled)

- [Looking after home or family](#looking-after-home-or-family)

- [Retired population](#retired-population)


#### <a name="economically-inactive-students"></a><a name="uk-ons-lc6201ew0136"></a>Economically inactive students

Measure &quot;Economically inactive students&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0136'
        );

Measure &quot;Economically inactive students&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0136'
        );

Measure &quot;Economically inactive students&quot; percent of &quot;Economically inactive&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0136',
          'denominator'
        );

Measure &quot;Economically inactive students&quot; percent of &quot;Economically inactive&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0136',
          'denominator'
        );

* denominator: [Economically inactive](#uk-ons-lc6201ew0118)


#### <a name="long-term-sick-or-disabled"></a><a name="uk-ons-lc6201ew0154"></a>Long-term sick or disabled

Measure &quot;Long-term sick or disabled&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0154'
        );

Measure &quot;Long-term sick or disabled&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0154'
        );

Measure &quot;Long-term sick or disabled&quot; percent of &quot;Economically inactive&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0154',
          'denominator'
        );

Measure &quot;Long-term sick or disabled&quot; percent of &quot;Economically inactive&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0154',
          'denominator'
        );

* denominator: [Economically inactive](#uk-ons-lc6201ew0118)


#### <a name="looking-after-home-or-family"></a><a name="uk-ons-lc6201ew0145"></a>Looking after home or family

Measure &quot;Looking after home or family&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0145'
        );

Measure &quot;Looking after home or family&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0145'
        );

Measure &quot;Looking after home or family&quot; percent of &quot;Economically inactive&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0145',
          'denominator'
        );

Measure &quot;Looking after home or family&quot; percent of &quot;Economically inactive&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0145',
          'denominator'
        );

* denominator: [Economically inactive](#uk-ons-lc6201ew0118)


#### <a name="retired-population"></a><a name="uk-ons-lc6201ew0127"></a>Retired population

Measure &quot;Retired population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0127'
        );

Measure &quot;Retired population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0127'
        );

Measure &quot;Retired population&quot; percent of &quot;Economically inactive&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6201EW0127',
          'denominator'
        );

Measure &quot;Retired population&quot; percent of &quot;Economically inactive&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6201EW0127',
          'denominator'
        );

* denominator: [Economically inactive](#uk-ons-lc6201ew0118)


### <a name="higher-managerial-administrative-and-professional-occupations"></a><a name="uk-ons-lc6206ew0010"></a>Higher managerial, administrative and professional occupations

Measure &quot;Higher managerial, administrative and professional occupations&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0010'
        );

Measure &quot;Higher managerial, administrative and professional occupations&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0010'
        );

Measure &quot;Higher managerial, administrative and professional occupations&quot; percent of &quot;Population age 16 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0010',
          'denominator'
        );

Measure &quot;Higher managerial, administrative and professional occupations&quot; percent of &quot;Population age 16 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0010',
          'denominator'
        );

* denominator: [Population age 16 and over](#uk-ons-lc6201ew0001)

Subcolumns of Higher managerial, administrative and professional occupations

- [Higher professional occupations](#higher-professional-occupations)

- [Large employers and higher managerial and administrative occupations](#large-employers-and-higher-managerial-and-administrative-occupations)


#### <a name="higher-professional-occupations"></a><a name="uk-ons-lc6206ew0028"></a>Higher professional occupations

Measure &quot;Higher professional occupations&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0028'
        );

Measure &quot;Higher professional occupations&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0028'
        );

Measure &quot;Higher professional occupations&quot; percent of &quot;Higher managerial, administrative and professional occupations&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0028',
          'denominator'
        );

Measure &quot;Higher professional occupations&quot; percent of &quot;Higher managerial, administrative and professional occupations&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0028',
          'denominator'
        );

* denominator: [Higher managerial, administrative and professional occupations](#uk-ons-lc6206ew0010)


#### <a name="large-employers-and-higher-managerial-and-administrative-occupations"></a><a name="uk-ons-lc6206ew0019"></a>Large employers and higher managerial and administrative occupations

Measure &quot;Large employers and higher managerial and administrative occupations&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0019'
        );

Measure &quot;Large employers and higher managerial and administrative occupations&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0019'
        );

Measure &quot;Large employers and higher managerial and administrative occupations&quot; percent of &quot;Higher managerial, administrative and professional occupations&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0019',
          'denominator'
        );

Measure &quot;Large employers and higher managerial and administrative occupations&quot; percent of &quot;Higher managerial, administrative and professional occupations&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0019',
          'denominator'
        );

* denominator: [Higher managerial, administrative and professional occupations](#uk-ons-lc6206ew0010)


### <a name="intermediate-occupations"></a><a name="uk-ons-lc6206ew0046"></a>Intermediate occupations

Measure &quot;Intermediate occupations&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0046'
        );

Measure &quot;Intermediate occupations&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0046'
        );

Measure &quot;Intermediate occupations&quot; percent of &quot;Population age 16 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0046',
          'denominator'
        );

Measure &quot;Intermediate occupations&quot; percent of &quot;Population age 16 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0046',
          'denominator'
        );

* denominator: [Population age 16 and over](#uk-ons-lc6201ew0001)


### <a name="lower-managerial-administrative-and-professional-occupations"></a><a name="uk-ons-lc6206ew0037"></a>Lower managerial, administrative and professional occupations

Measure &quot;Lower managerial, administrative and professional occupations&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0037'
        );

Measure &quot;Lower managerial, administrative and professional occupations&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0037'
        );

Measure &quot;Lower managerial, administrative and professional occupations&quot; percent of &quot;Population age 16 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0037',
          'denominator'
        );

Measure &quot;Lower managerial, administrative and professional occupations&quot; percent of &quot;Population age 16 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0037',
          'denominator'
        );

* denominator: [Population age 16 and over](#uk-ons-lc6201ew0001)


### <a name="lower-supervisory-and-technical-occupations"></a><a name="uk-ons-lc6206ew0064"></a>Lower supervisory and technical occupations

Measure &quot;Lower supervisory and technical occupations&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0064'
        );

Measure &quot;Lower supervisory and technical occupations&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0064'
        );

Measure &quot;Lower supervisory and technical occupations&quot; percent of &quot;Population age 16 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0064',
          'denominator'
        );

Measure &quot;Lower supervisory and technical occupations&quot; percent of &quot;Population age 16 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0064',
          'denominator'
        );

* denominator: [Population age 16 and over](#uk-ons-lc6201ew0001)


### <a name="never-worked-and-long-term-unemployed"></a><a name="uk-ons-lc6206ew0091"></a>Never worked and long-term unemployed

Measure &quot;Never worked and long-term unemployed&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0091'
        );

Measure &quot;Never worked and long-term unemployed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0091'
        );

Measure &quot;Never worked and long-term unemployed&quot; percent of &quot;Population age 16 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0091',
          'denominator'
        );

Measure &quot;Never worked and long-term unemployed&quot; percent of &quot;Population age 16 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0091',
          'denominator'
        );

* denominator: [Population age 16 and over](#uk-ons-lc6201ew0001)

Subcolumns of Never worked and long-term unemployed

- [Long-term unemployed](#long-term-unemployed)

- [Never worked](#never-worked)


#### <a name="long-term-unemployed"></a><a name="uk-ons-lc6206ew0109"></a>Long-term unemployed

Measure &quot;Long-term unemployed&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0109'
        );

Measure &quot;Long-term unemployed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0109'
        );

Measure &quot;Long-term unemployed&quot; percent of &quot;Never worked and long-term unemployed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0109',
          'denominator'
        );

Measure &quot;Long-term unemployed&quot; percent of &quot;Never worked and long-term unemployed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0109',
          'denominator'
        );

* denominator: [Never worked and long-term unemployed](#uk-ons-lc6206ew0091)


#### <a name="never-worked"></a><a name="uk-ons-lc6206ew0100"></a>Never worked

Measure &quot;Never worked&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0100'
        );

Measure &quot;Never worked&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0100'
        );

Measure &quot;Never worked&quot; percent of &quot;Never worked and long-term unemployed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0100',
          'denominator'
        );

Measure &quot;Never worked&quot; percent of &quot;Never worked and long-term unemployed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0100',
          'denominator'
        );

* denominator: [Never worked and long-term unemployed](#uk-ons-lc6206ew0091)


### <a name="routine-occupations"></a><a name="uk-ons-lc6206ew0082"></a>Routine occupations

Measure &quot;Routine occupations&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0082'
        );

Measure &quot;Routine occupations&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0082'
        );

Measure &quot;Routine occupations&quot; percent of &quot;Population age 16 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0082',
          'denominator'
        );

Measure &quot;Routine occupations&quot; percent of &quot;Population age 16 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0082',
          'denominator'
        );

* denominator: [Population age 16 and over](#uk-ons-lc6201ew0001)


### <a name="semi-routine-occupations"></a><a name="uk-ons-lc6206ew0073"></a>Semi-routine occupations

Measure &quot;Semi-routine occupations&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0073'
        );

Measure &quot;Semi-routine occupations&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0073'
        );

Measure &quot;Semi-routine occupations&quot; percent of &quot;Population age 16 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0073',
          'denominator'
        );

Measure &quot;Semi-routine occupations&quot; percent of &quot;Population age 16 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0073',
          'denominator'
        );

* denominator: [Population age 16 and over](#uk-ons-lc6201ew0001)


### <a name="small-employers-and-account-workers"></a><a name="uk-ons-lc6206ew0055"></a>Small employers and account workers

Measure &quot;Small employers and account workers&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0055'
        );

Measure &quot;Small employers and account workers&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0055'
        );

Measure &quot;Small employers and account workers&quot; percent of &quot;Population age 16 and over&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC6206EW0055',
          'denominator'
        );

Measure &quot;Small employers and account workers&quot; percent of &quot;Population age 16 and over&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC6206EW0055',
          'denominator'
        );

* denominator: [Population age 16 and over](#uk-ons-lc6201ew0001)


