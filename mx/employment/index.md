

  
# <a name="employment"></a>Employment

How people are employed.

- [Economically active population](#economically-active-population)

    * [Economically active female population](#economically-active-female-population)

        - [Employed female](#employed-female)

        - [Employed female population who completed basic education](#employed-female-population-who-completed-basic-education)

        - [Employed female population with a high school degree](#employed-female-population-with-a-high-school-degree)

        - [Employed female population with a university degree or more](#employed-female-population-with-a-university-degree-or-more)

        - [Employed female population with incomplete secondary education](#employed-female-population-with-incomplete-secondary-education)

        - [Employed female population with primary education](#employed-female-population-with-primary-education)

        - [Employed female population without schooling](#employed-female-population-without-schooling)

        - [Unemployed female](#unemployed-female)

    * [Economically active male population](#economically-active-male-population)

        - [Employed male](#employed-male)

        - [Employed male population who completed basic education](#employed-male-population-who-completed-basic-education)

        - [Employed male population with a high school degree](#employed-male-population-with-a-high-school-degree)

        - [Employed male population with a university degree or more](#employed-male-population-with-a-university-degree-or-more)

        - [Employed male population with incomplete secondary education](#employed-male-population-with-incomplete-secondary-education)

        - [Employed male population with primary education](#employed-male-population-with-primary-education)

        - [Employed male population without schooling](#employed-male-population-without-schooling)

        - [Unemployed male](#unemployed-male)

    * [Employed](#employed)

        - [Employed female](#id1)

        - [Employed female population who completed basic education](#id4)

        - [Employed female population with a high school degree](#id6)

        - [Employed female population with a university degree or more](#id8)

        - [Employed female population with incomplete secondary education](#id10)

        - [Employed female population with primary education](#id12)

        - [Employed female population without schooling](#id14)

        - [Employed male](#id16)

        - [Employed male population who completed basic education](#id19)

        - [Employed male population with a high school degree](#id21)

        - [Employed male population with a university degree or more](#id23)

        - [Employed male population with incomplete secondary education](#id25)

        - [Employed male population with primary education](#id27)

        - [Employed male population without schooling](#id29)

        - [Employed population who completed basic education](#employed-population-who-completed-basic-education)

        - [Employed female population who completed basic education](#id31)

        - [Employed male population who completed basic education](#id33)

        - [Employed population with a high school degree](#employed-population-with-a-high-school-degree)

        - [Employed female population with a high school degree](#id35)

        - [Employed male population with a high school degree](#id37)

        - [Employed population with a university degree or more](#employed-population-with-a-university-degree-or-more)

        - [Employed female population with a university degree or more](#id39)

        - [Employed male population with a university degree or more](#id41)

        - [Employed population with incomplete secondary education](#employed-population-with-incomplete-secondary-education)

        - [Employed female population with incomplete secondary education](#id43)

        - [Employed male population with incomplete secondary education](#id45)

        - [Employed population with primary education](#employed-population-with-primary-education)

        - [Employed female population with primary education](#id47)

        - [Employed male population with primary education](#id49)

        - [Employed population without schooling](#employed-population-without-schooling)

        - [Employed female population without schooling](#id51)

        - [Employed male population without schooling](#id53)

    * [Unemployed](#unemployed)

        - [Unemployed female](#id55)

        - [Unemployed male](#id57)

- [Economically inactive](#economically-inactive)

    * [Dedicated to housework](#dedicated-to-housework)

        - [Female dedicated to housework](#female-dedicated-to-housework)

        - [Male dedicated to housework](#male-dedicated-to-housework)

    * [Economically inactive with a physical or mental limitation impeding work](#economically-inactive-with-a-physical-or-mental-limitation-impeding-work)

        - [Female economically inactive with a physical or mental limitation impeding work](#female-economically-inactive-with-a-physical-or-mental-limitation-impeding-work)

        - [Male economically inactive with a physical or mental limitation impeding work](#male-economically-inactive-with-a-physical-or-mental-limitation-impeding-work)

    * [Economically inactive with some other reason that impedes work](#economically-inactive-with-some-other-reason-that-impedes-work)

        - [Female economically inactive with some other reason that impedes work](#female-economically-inactive-with-some-other-reason-that-impedes-work)

        - [Male economically inactive with some other reason that impedes work](#male-economically-inactive-with-some-other-reason-that-impedes-work)

    * [Female economically inactive population](#female-economically-inactive-population)

        - [Female dedicated to housework](#id59)

        - [Female economically inactive with a physical or mental limitation impeding work](#id61)

        - [Female economically inactive with some other reason that impedes work](#id63)

        - [Female full time student](#female-full-time-student)

        - [Female retiree or receiving pension](#female-retiree-or-receiving-pension)

    * [Full time student](#full-time-student)

        - [Female full time student](#id65)

        - [Male full time student](#male-full-time-student)

    * [Male economically inactive population](#male-economically-inactive-population)

        - [Male dedicated to housework](#id67)

        - [Male economically inactive with a physical or mental limitation impeding work](#id69)

        - [Male economically inactive with some other reason that impedes work](#id71)

        - [Male full time student](#id73)

        - [Male retiree or receiving pension](#male-retiree-or-receiving-pension)

    * [Retiree or receiving pension](#retiree-or-receiving-pension)

        - [Female retiree or receiving pension](#id75)

        - [Male retiree or receiving pension](#id77)


## <a name="economically-active-population"></a><a name="mx-inegi-columns-eco1"></a>Economically active population

[<img alt="../../_images/mx.inegi_columns.ECO1.png" src="../../_images/mx.inegi_columns.ECO1.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO1.png)Measure &quot;Economically active population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO1'
        );

Measure &quot;Economically active population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO1'
        );

Measure &quot;Economically active population&quot; percent of &quot;Total population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO1',
          'denominator'
        );

Measure &quot;Economically active population&quot; percent of &quot;Total population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO1',
          'denominator'
        );

* denominator: [Total population](../age_gender/#mx-inegi-columns-pob1)

Subcolumns of Economically active population

- [Economically active female population](#economically-active-female-population)

- [Economically active male population](#economically-active-male-population)

- [Employed](#employed)

- [Unemployed](#unemployed)


### <a name="economically-active-female-population"></a><a name="mx-inegi-columns-eco2"></a>Economically active female population

[<img alt="../../_images/mx.inegi_columns.ECO2.png" src="../../_images/mx.inegi_columns.ECO2.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO2.png)Measure &quot;Economically active female population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO2'
        );

Measure &quot;Economically active female population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO2'
        );

Measure &quot;Economically active female population&quot; percent of &quot;Economically active population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO2',
          'denominator'
        );

Measure &quot;Economically active female population&quot; percent of &quot;Economically active population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO2',
          'denominator'
        );

* denominator: [Economically active population](#mx-inegi-columns-eco1)

* denominator: [Female population](../age_gender/#mx-inegi-columns-pob31)

Subcolumns of Economically active female population

- [Employed female](#employed-female)

- [Employed female population who completed basic education](#employed-female-population-who-completed-basic-education)

- [Employed female population with a high school degree](#employed-female-population-with-a-high-school-degree)

- [Employed female population with a university degree or more](#employed-female-population-with-a-university-degree-or-more)

- [Employed female population with incomplete secondary education](#employed-female-population-with-incomplete-secondary-education)

- [Employed female population with primary education](#employed-female-population-with-primary-education)

- [Employed female population without schooling](#employed-female-population-without-schooling)

- [Unemployed female](#unemployed-female)


#### <a name="employed-female"></a><a name="mx-inegi-columns-eco5"></a>Employed female

[<img alt="../../_images/mx.inegi_columns.ECO5.png" src="../../_images/mx.inegi_columns.ECO5.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO5.png)Measure &quot;Employed female&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO5'
        );

Measure &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO5'
        );

Measure &quot;Employed female&quot; percent of &quot;Economically active female population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO5',
          'denominator'
        );

Measure &quot;Employed female&quot; percent of &quot;Economically active female population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO5',
          'denominator'
        );

* denominator: [Economically active female population](#mx-inegi-columns-eco2)

* denominator: [Employed](#mx-inegi-columns-eco4)


#### <a name="employed-female-population-who-completed-basic-education"></a><a name="mx-inegi-columns-eco17"></a>Employed female population who completed basic education

[<img alt="../../_images/mx.inegi_columns.ECO17.png" src="../../_images/mx.inegi_columns.ECO17.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO17.png)Measure &quot;Employed female population who completed basic education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO17'
        );

Measure &quot;Employed female population who completed basic education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO17'
        );

Measure &quot;Employed female population who completed basic education&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO17',
          'denominator'
        );

Measure &quot;Employed female population who completed basic education&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO17',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population who completed basic education](#mx-inegi-columns-eco16)


#### <a name="employed-female-population-with-a-high-school-degree"></a><a name="mx-inegi-columns-eco20"></a>Employed female population with a high school degree

[<img alt="../../_images/mx.inegi_columns.ECO20.png" src="../../_images/mx.inegi_columns.ECO20.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO20.png)Measure &quot;Employed female population with a high school degree&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO20'
        );

Measure &quot;Employed female population with a high school degree&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO20'
        );

Measure &quot;Employed female population with a high school degree&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO20',
          'denominator'
        );

Measure &quot;Employed female population with a high school degree&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO20',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population with a high school degree](#mx-inegi-columns-eco19)


#### <a name="employed-female-population-with-a-university-degree-or-more"></a><a name="mx-inegi-columns-eco23"></a>Employed female population with a university degree or more

[<img alt="../../_images/mx.inegi_columns.ECO23.png" src="../../_images/mx.inegi_columns.ECO23.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO23.png)Measure &quot;Employed female population with a university degree or more&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO23'
        );

Measure &quot;Employed female population with a university degree or more&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO23'
        );

Measure &quot;Employed female population with a university degree or more&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO23',
          'denominator'
        );

Measure &quot;Employed female population with a university degree or more&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO23',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population with a university degree or more](#mx-inegi-columns-eco22)


#### <a name="employed-female-population-with-incomplete-secondary-education"></a><a name="mx-inegi-columns-eco14"></a>Employed female population with incomplete secondary education

[<img alt="../../_images/mx.inegi_columns.ECO14.png" src="../../_images/mx.inegi_columns.ECO14.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO14.png)Measure &quot;Employed female population with incomplete secondary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO14'
        );

Measure &quot;Employed female population with incomplete secondary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO14'
        );

Measure &quot;Employed female population with incomplete secondary education&quot; percent of &quot;Employed population with incomplete secondary education&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO14',
          'denominator'
        );

Measure &quot;Employed female population with incomplete secondary education&quot; percent of &quot;Employed population with incomplete secondary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO14',
          'denominator'
        );

* denominator: [Employed population with incomplete secondary education](#mx-inegi-columns-eco13)

* denominator: mx.inegi_columns.ECO5


#### <a name="employed-female-population-with-primary-education"></a><a name="mx-inegi-columns-eco11"></a>Employed female population with primary education

[<img alt="../../_images/mx.inegi_columns.ECO11.png" src="../../_images/mx.inegi_columns.ECO11.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO11.png)Measure &quot;Employed female population with primary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO11'
        );

Measure &quot;Employed female population with primary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO11'
        );

Measure &quot;Employed female population with primary education&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO11',
          'denominator'
        );

Measure &quot;Employed female population with primary education&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO11',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population with primary education](#mx-inegi-columns-eco10)


#### <a name="employed-female-population-without-schooling"></a><a name="mx-inegi-columns-eco8"></a>Employed female population without schooling

[<img alt="../../_images/mx.inegi_columns.ECO8.png" src="../../_images/mx.inegi_columns.ECO8.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO8.png)Measure &quot;Employed female population without schooling&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO8'
        );

Measure &quot;Employed female population without schooling&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO8'
        );

Measure &quot;Employed female population without schooling&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO8',
          'denominator'
        );

Measure &quot;Employed female population without schooling&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO8',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population without schooling](#mx-inegi-columns-eco7)


#### <a name="unemployed-female"></a><a name="mx-inegi-columns-eco26"></a>Unemployed female

[<img alt="../../_images/mx.inegi_columns.ECO26.png" src="../../_images/mx.inegi_columns.ECO26.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO26.png)Measure &quot;Unemployed female&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO26'
        );

Measure &quot;Unemployed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO26'
        );

Measure &quot;Unemployed female&quot; percent of &quot;Unemployed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO26',
          'denominator'
        );

Measure &quot;Unemployed female&quot; percent of &quot;Unemployed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO26',
          'denominator'
        );

* denominator: [Unemployed](#mx-inegi-columns-eco25)

* denominator: [Economically active female population](#mx-inegi-columns-eco2)


### <a name="economically-active-male-population"></a><a name="mx-inegi-columns-eco3"></a>Economically active male population

[<img alt="../../_images/mx.inegi_columns.ECO3.png" src="../../_images/mx.inegi_columns.ECO3.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO3.png)Measure &quot;Economically active male population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO3'
        );

Measure &quot;Economically active male population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO3'
        );

Measure &quot;Economically active male population&quot; percent of &quot;Economically active population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO3',
          'denominator'
        );

Measure &quot;Economically active male population&quot; percent of &quot;Economically active population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO3',
          'denominator'
        );

* denominator: [Economically active population](#mx-inegi-columns-eco1)

* denominator: [Male population](../age_gender/#mx-inegi-columns-pob57)

Subcolumns of Economically active male population

- [Employed male](#employed-male)

- [Employed male population who completed basic education](#employed-male-population-who-completed-basic-education)

- [Employed male population with a high school degree](#employed-male-population-with-a-high-school-degree)

- [Employed male population with a university degree or more](#employed-male-population-with-a-university-degree-or-more)

- [Employed male population with incomplete secondary education](#employed-male-population-with-incomplete-secondary-education)

- [Employed male population with primary education](#employed-male-population-with-primary-education)

- [Employed male population without schooling](#employed-male-population-without-schooling)

- [Unemployed male](#unemployed-male)


#### <a name="employed-male"></a><a name="mx-inegi-columns-eco6"></a>Employed male

[<img alt="../../_images/mx.inegi_columns.ECO6.png" src="../../_images/mx.inegi_columns.ECO6.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO6.png)Measure &quot;Employed male&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO6'
        );

Measure &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO6'
        );

Measure &quot;Employed male&quot; percent of &quot;Economically active male population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO6',
          'denominator'
        );

Measure &quot;Employed male&quot; percent of &quot;Economically active male population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO6',
          'denominator'
        );

* denominator: [Economically active male population](#mx-inegi-columns-eco3)

* denominator: [Employed](#mx-inegi-columns-eco4)


#### <a name="employed-male-population-who-completed-basic-education"></a><a name="mx-inegi-columns-eco18"></a>Employed male population who completed basic education

[<img alt="../../_images/mx.inegi_columns.ECO18.png" src="../../_images/mx.inegi_columns.ECO18.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO18.png)Measure &quot;Employed male population who completed basic education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO18'
        );

Measure &quot;Employed male population who completed basic education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO18'
        );

Measure &quot;Employed male population who completed basic education&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO18',
          'denominator'
        );

Measure &quot;Employed male population who completed basic education&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO18',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population who completed basic education](#mx-inegi-columns-eco16)


#### <a name="employed-male-population-with-a-high-school-degree"></a><a name="mx-inegi-columns-eco21"></a>Employed male population with a high school degree

[<img alt="../../_images/mx.inegi_columns.ECO21.png" src="../../_images/mx.inegi_columns.ECO21.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO21.png)Measure &quot;Employed male population with a high school degree&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO21'
        );

Measure &quot;Employed male population with a high school degree&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO21'
        );

Measure &quot;Employed male population with a high school degree&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO21',
          'denominator'
        );

Measure &quot;Employed male population with a high school degree&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO21',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population with a high school degree](#mx-inegi-columns-eco19)


#### <a name="employed-male-population-with-a-university-degree-or-more"></a><a name="mx-inegi-columns-eco24"></a>Employed male population with a university degree or more

[<img alt="../../_images/mx.inegi_columns.ECO24.png" src="../../_images/mx.inegi_columns.ECO24.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO24.png)Measure &quot;Employed male population with a university degree or more&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO24'
        );

Measure &quot;Employed male population with a university degree or more&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO24'
        );

Measure &quot;Employed male population with a university degree or more&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO24',
          'denominator'
        );

Measure &quot;Employed male population with a university degree or more&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO24',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population with a university degree or more](#mx-inegi-columns-eco22)


#### <a name="employed-male-population-with-incomplete-secondary-education"></a><a name="mx-inegi-columns-eco15"></a>Employed male population with incomplete secondary education

[<img alt="../../_images/mx.inegi_columns.ECO15.png" src="../../_images/mx.inegi_columns.ECO15.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO15.png)Measure &quot;Employed male population with incomplete secondary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO15'
        );

Measure &quot;Employed male population with incomplete secondary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO15'
        );

Measure &quot;Employed male population with incomplete secondary education&quot; percent of &quot;Employed population with incomplete secondary education&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO15',
          'denominator'
        );

Measure &quot;Employed male population with incomplete secondary education&quot; percent of &quot;Employed population with incomplete secondary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO15',
          'denominator'
        );

* denominator: [Employed population with incomplete secondary education](#mx-inegi-columns-eco13)

* denominator: mx.inegi_columns.ECO6


#### <a name="employed-male-population-with-primary-education"></a><a name="mx-inegi-columns-eco12"></a>Employed male population with primary education

[<img alt="../../_images/mx.inegi_columns.ECO12.png" src="../../_images/mx.inegi_columns.ECO12.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO12.png)Measure &quot;Employed male population with primary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO12'
        );

Measure &quot;Employed male population with primary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO12'
        );

Measure &quot;Employed male population with primary education&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO12',
          'denominator'
        );

Measure &quot;Employed male population with primary education&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO12',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population with primary education](#mx-inegi-columns-eco10)


#### <a name="employed-male-population-without-schooling"></a><a name="mx-inegi-columns-eco9"></a>Employed male population without schooling

[<img alt="../../_images/mx.inegi_columns.ECO9.png" src="../../_images/mx.inegi_columns.ECO9.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO9.png)Measure &quot;Employed male population without schooling&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO9'
        );

Measure &quot;Employed male population without schooling&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO9'
        );

Measure &quot;Employed male population without schooling&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO9',
          'denominator'
        );

Measure &quot;Employed male population without schooling&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO9',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population without schooling](#mx-inegi-columns-eco7)


#### <a name="unemployed-male"></a><a name="mx-inegi-columns-eco27"></a>Unemployed male

[<img alt="../../_images/mx.inegi_columns.ECO27.png" src="../../_images/mx.inegi_columns.ECO27.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO27.png)Measure &quot;Unemployed male&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO27'
        );

Measure &quot;Unemployed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO27'
        );

Measure &quot;Unemployed male&quot; percent of &quot;Unemployed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO27',
          'denominator'
        );

Measure &quot;Unemployed male&quot; percent of &quot;Unemployed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO27',
          'denominator'
        );

* denominator: [Unemployed](#mx-inegi-columns-eco25)

* denominator: [Economically active male population](#mx-inegi-columns-eco3)


### <a name="employed"></a><a name="mx-inegi-columns-eco4"></a>Employed

[<img alt="../../_images/mx.inegi_columns.ECO4.png" src="../../_images/mx.inegi_columns.ECO4.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO4.png)Measure &quot;Employed&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO4'
        );

Measure &quot;Employed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO4'
        );

Measure &quot;Employed&quot; percent of &quot;Economically active population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO4',
          'denominator'
        );

Measure &quot;Employed&quot; percent of &quot;Economically active population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO4',
          'denominator'
        );

* denominator: [Economically active population](#mx-inegi-columns-eco1)

Subcolumns of Employed

- [Employed female](#id1)

- [Employed female population who completed basic education](#id4)

- [Employed female population with a high school degree](#id6)

- [Employed female population with a university degree or more](#id8)

- [Employed female population with incomplete secondary education](#id10)

- [Employed female population with primary education](#id12)

- [Employed female population without schooling](#id14)

- [Employed male](#id16)

- [Employed male population who completed basic education](#id19)

- [Employed male population with a high school degree](#id21)

- [Employed male population with a university degree or more](#id23)

- [Employed male population with incomplete secondary education](#id25)

- [Employed male population with primary education](#id27)

- [Employed male population without schooling](#id29)

- [Employed population who completed basic education](#employed-population-who-completed-basic-education)

- [Employed female population who completed basic education](#id31)

- [Employed male population who completed basic education](#id33)

- [Employed population with a high school degree](#employed-population-with-a-high-school-degree)

- [Employed female population with a high school degree](#id35)

- [Employed male population with a high school degree](#id37)

- [Employed population with a university degree or more](#employed-population-with-a-university-degree-or-more)

- [Employed female population with a university degree or more](#id39)

- [Employed male population with a university degree or more](#id41)

- [Employed population with incomplete secondary education](#employed-population-with-incomplete-secondary-education)

- [Employed female population with incomplete secondary education](#id43)

- [Employed male population with incomplete secondary education](#id45)

- [Employed population with primary education](#employed-population-with-primary-education)

- [Employed female population with primary education](#id47)

- [Employed male population with primary education](#id49)

- [Employed population without schooling](#employed-population-without-schooling)

- [Employed female population without schooling](#id51)

- [Employed male population without schooling](#id53)


#### <a name="id1"></a><a name="id2"></a>Employed female

[<img alt="../../_images/mx.inegi_columns.ECO5.png" src="../../_images/mx.inegi_columns.ECO5.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO5.png)Measure &quot;Employed female&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO5'
        );

Measure &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO5'
        );

Measure &quot;Employed female&quot; percent of &quot;Economically active female population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO5',
          'denominator'
        );

Measure &quot;Employed female&quot; percent of &quot;Economically active female population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO5',
          'denominator'
        );

* denominator: [Economically active female population](#mx-inegi-columns-eco2)

* denominator: [Employed](#mx-inegi-columns-eco4)


#### <a name="id4"></a><a name="id5"></a>Employed female population who completed basic education

[<img alt="../../_images/mx.inegi_columns.ECO17.png" src="../../_images/mx.inegi_columns.ECO17.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO17.png)Measure &quot;Employed female population who completed basic education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO17'
        );

Measure &quot;Employed female population who completed basic education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO17'
        );

Measure &quot;Employed female population who completed basic education&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO17',
          'denominator'
        );

Measure &quot;Employed female population who completed basic education&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO17',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population who completed basic education](#mx-inegi-columns-eco16)


#### <a name="id6"></a><a name="id7"></a>Employed female population with a high school degree

[<img alt="../../_images/mx.inegi_columns.ECO20.png" src="../../_images/mx.inegi_columns.ECO20.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO20.png)Measure &quot;Employed female population with a high school degree&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO20'
        );

Measure &quot;Employed female population with a high school degree&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO20'
        );

Measure &quot;Employed female population with a high school degree&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO20',
          'denominator'
        );

Measure &quot;Employed female population with a high school degree&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO20',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population with a high school degree](#mx-inegi-columns-eco19)


#### <a name="id8"></a><a name="id9"></a>Employed female population with a university degree or more

[<img alt="../../_images/mx.inegi_columns.ECO23.png" src="../../_images/mx.inegi_columns.ECO23.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO23.png)Measure &quot;Employed female population with a university degree or more&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO23'
        );

Measure &quot;Employed female population with a university degree or more&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO23'
        );

Measure &quot;Employed female population with a university degree or more&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO23',
          'denominator'
        );

Measure &quot;Employed female population with a university degree or more&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO23',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population with a university degree or more](#mx-inegi-columns-eco22)


#### <a name="id10"></a><a name="id11"></a>Employed female population with incomplete secondary education

[<img alt="../../_images/mx.inegi_columns.ECO14.png" src="../../_images/mx.inegi_columns.ECO14.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO14.png)Measure &quot;Employed female population with incomplete secondary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO14'
        );

Measure &quot;Employed female population with incomplete secondary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO14'
        );

Measure &quot;Employed female population with incomplete secondary education&quot; percent of &quot;Employed population with incomplete secondary education&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO14',
          'denominator'
        );

Measure &quot;Employed female population with incomplete secondary education&quot; percent of &quot;Employed population with incomplete secondary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO14',
          'denominator'
        );

* denominator: [Employed population with incomplete secondary education](#mx-inegi-columns-eco13)

* denominator: mx.inegi_columns.ECO5


#### <a name="id12"></a><a name="id13"></a>Employed female population with primary education

[<img alt="../../_images/mx.inegi_columns.ECO11.png" src="../../_images/mx.inegi_columns.ECO11.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO11.png)Measure &quot;Employed female population with primary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO11'
        );

Measure &quot;Employed female population with primary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO11'
        );

Measure &quot;Employed female population with primary education&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO11',
          'denominator'
        );

Measure &quot;Employed female population with primary education&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO11',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population with primary education](#mx-inegi-columns-eco10)


#### <a name="id14"></a><a name="id15"></a>Employed female population without schooling

[<img alt="../../_images/mx.inegi_columns.ECO8.png" src="../../_images/mx.inegi_columns.ECO8.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO8.png)Measure &quot;Employed female population without schooling&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO8'
        );

Measure &quot;Employed female population without schooling&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO8'
        );

Measure &quot;Employed female population without schooling&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO8',
          'denominator'
        );

Measure &quot;Employed female population without schooling&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO8',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population without schooling](#mx-inegi-columns-eco7)


#### <a name="id16"></a><a name="id17"></a>Employed male

[<img alt="../../_images/mx.inegi_columns.ECO6.png" src="../../_images/mx.inegi_columns.ECO6.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO6.png)Measure &quot;Employed male&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO6'
        );

Measure &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO6'
        );

Measure &quot;Employed male&quot; percent of &quot;Economically active male population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO6',
          'denominator'
        );

Measure &quot;Employed male&quot; percent of &quot;Economically active male population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO6',
          'denominator'
        );

* denominator: [Economically active male population](#mx-inegi-columns-eco3)

* denominator: [Employed](#mx-inegi-columns-eco4)


#### <a name="id19"></a><a name="id20"></a>Employed male population who completed basic education

[<img alt="../../_images/mx.inegi_columns.ECO18.png" src="../../_images/mx.inegi_columns.ECO18.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO18.png)Measure &quot;Employed male population who completed basic education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO18'
        );

Measure &quot;Employed male population who completed basic education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO18'
        );

Measure &quot;Employed male population who completed basic education&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO18',
          'denominator'
        );

Measure &quot;Employed male population who completed basic education&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO18',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population who completed basic education](#mx-inegi-columns-eco16)


#### <a name="id21"></a><a name="id22"></a>Employed male population with a high school degree

[<img alt="../../_images/mx.inegi_columns.ECO21.png" src="../../_images/mx.inegi_columns.ECO21.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO21.png)Measure &quot;Employed male population with a high school degree&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO21'
        );

Measure &quot;Employed male population with a high school degree&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO21'
        );

Measure &quot;Employed male population with a high school degree&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO21',
          'denominator'
        );

Measure &quot;Employed male population with a high school degree&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO21',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population with a high school degree](#mx-inegi-columns-eco19)


#### <a name="id23"></a><a name="id24"></a>Employed male population with a university degree or more

[<img alt="../../_images/mx.inegi_columns.ECO24.png" src="../../_images/mx.inegi_columns.ECO24.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO24.png)Measure &quot;Employed male population with a university degree or more&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO24'
        );

Measure &quot;Employed male population with a university degree or more&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO24'
        );

Measure &quot;Employed male population with a university degree or more&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO24',
          'denominator'
        );

Measure &quot;Employed male population with a university degree or more&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO24',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population with a university degree or more](#mx-inegi-columns-eco22)


#### <a name="id25"></a><a name="id26"></a>Employed male population with incomplete secondary education

[<img alt="../../_images/mx.inegi_columns.ECO15.png" src="../../_images/mx.inegi_columns.ECO15.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO15.png)Measure &quot;Employed male population with incomplete secondary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO15'
        );

Measure &quot;Employed male population with incomplete secondary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO15'
        );

Measure &quot;Employed male population with incomplete secondary education&quot; percent of &quot;Employed population with incomplete secondary education&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO15',
          'denominator'
        );

Measure &quot;Employed male population with incomplete secondary education&quot; percent of &quot;Employed population with incomplete secondary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO15',
          'denominator'
        );

* denominator: [Employed population with incomplete secondary education](#mx-inegi-columns-eco13)

* denominator: mx.inegi_columns.ECO6


#### <a name="id27"></a><a name="id28"></a>Employed male population with primary education

[<img alt="../../_images/mx.inegi_columns.ECO12.png" src="../../_images/mx.inegi_columns.ECO12.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO12.png)Measure &quot;Employed male population with primary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO12'
        );

Measure &quot;Employed male population with primary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO12'
        );

Measure &quot;Employed male population with primary education&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO12',
          'denominator'
        );

Measure &quot;Employed male population with primary education&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO12',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population with primary education](#mx-inegi-columns-eco10)


#### <a name="id29"></a><a name="id30"></a>Employed male population without schooling

[<img alt="../../_images/mx.inegi_columns.ECO9.png" src="../../_images/mx.inegi_columns.ECO9.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO9.png)Measure &quot;Employed male population without schooling&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO9'
        );

Measure &quot;Employed male population without schooling&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO9'
        );

Measure &quot;Employed male population without schooling&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO9',
          'denominator'
        );

Measure &quot;Employed male population without schooling&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO9',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population without schooling](#mx-inegi-columns-eco7)


#### <a name="employed-population-who-completed-basic-education"></a><a name="mx-inegi-columns-eco16"></a>Employed population who completed basic education

[<img alt="../../_images/mx.inegi_columns.ECO16.png" src="../../_images/mx.inegi_columns.ECO16.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO16.png)Measure &quot;Employed population who completed basic education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO16'
        );

Measure &quot;Employed population who completed basic education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO16'
        );

Measure &quot;Employed population who completed basic education&quot; percent of &quot;Employed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO16',
          'denominator'
        );

Measure &quot;Employed population who completed basic education&quot; percent of &quot;Employed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO16',
          'denominator'
        );

* denominator: [Employed](#mx-inegi-columns-eco4)


#### <a name="id31"></a><a name="id32"></a>Employed female population who completed basic education

[<img alt="../../_images/mx.inegi_columns.ECO17.png" src="../../_images/mx.inegi_columns.ECO17.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO17.png)Measure &quot;Employed female population who completed basic education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO17'
        );

Measure &quot;Employed female population who completed basic education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO17'
        );

Measure &quot;Employed female population who completed basic education&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO17',
          'denominator'
        );

Measure &quot;Employed female population who completed basic education&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO17',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population who completed basic education](#mx-inegi-columns-eco16)


#### <a name="id33"></a><a name="id34"></a>Employed male population who completed basic education

[<img alt="../../_images/mx.inegi_columns.ECO18.png" src="../../_images/mx.inegi_columns.ECO18.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO18.png)Measure &quot;Employed male population who completed basic education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO18'
        );

Measure &quot;Employed male population who completed basic education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO18'
        );

Measure &quot;Employed male population who completed basic education&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO18',
          'denominator'
        );

Measure &quot;Employed male population who completed basic education&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO18',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population who completed basic education](#mx-inegi-columns-eco16)


#### <a name="employed-population-with-a-high-school-degree"></a><a name="mx-inegi-columns-eco19"></a>Employed population with a high school degree

[<img alt="../../_images/mx.inegi_columns.ECO19.png" src="../../_images/mx.inegi_columns.ECO19.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO19.png)Measure &quot;Employed population with a high school degree&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO19'
        );

Measure &quot;Employed population with a high school degree&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO19'
        );

Measure &quot;Employed population with a high school degree&quot; percent of &quot;Employed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO19',
          'denominator'
        );

Measure &quot;Employed population with a high school degree&quot; percent of &quot;Employed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO19',
          'denominator'
        );

* denominator: [Employed](#mx-inegi-columns-eco4)


#### <a name="id35"></a><a name="id36"></a>Employed female population with a high school degree

[<img alt="../../_images/mx.inegi_columns.ECO20.png" src="../../_images/mx.inegi_columns.ECO20.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO20.png)Measure &quot;Employed female population with a high school degree&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO20'
        );

Measure &quot;Employed female population with a high school degree&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO20'
        );

Measure &quot;Employed female population with a high school degree&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO20',
          'denominator'
        );

Measure &quot;Employed female population with a high school degree&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO20',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population with a high school degree](#mx-inegi-columns-eco19)


#### <a name="id37"></a><a name="id38"></a>Employed male population with a high school degree

[<img alt="../../_images/mx.inegi_columns.ECO21.png" src="../../_images/mx.inegi_columns.ECO21.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO21.png)Measure &quot;Employed male population with a high school degree&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO21'
        );

Measure &quot;Employed male population with a high school degree&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO21'
        );

Measure &quot;Employed male population with a high school degree&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO21',
          'denominator'
        );

Measure &quot;Employed male population with a high school degree&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO21',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population with a high school degree](#mx-inegi-columns-eco19)


#### <a name="employed-population-with-a-university-degree-or-more"></a><a name="mx-inegi-columns-eco22"></a>Employed population with a university degree or more

[<img alt="../../_images/mx.inegi_columns.ECO22.png" src="../../_images/mx.inegi_columns.ECO22.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO22.png)Measure &quot;Employed population with a university degree or more&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO22'
        );

Measure &quot;Employed population with a university degree or more&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO22'
        );

Measure &quot;Employed population with a university degree or more&quot; percent of &quot;Employed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO22',
          'denominator'
        );

Measure &quot;Employed population with a university degree or more&quot; percent of &quot;Employed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO22',
          'denominator'
        );

* denominator: [Employed](#mx-inegi-columns-eco4)


#### <a name="id39"></a><a name="id40"></a>Employed female population with a university degree or more

[<img alt="../../_images/mx.inegi_columns.ECO23.png" src="../../_images/mx.inegi_columns.ECO23.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO23.png)Measure &quot;Employed female population with a university degree or more&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO23'
        );

Measure &quot;Employed female population with a university degree or more&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO23'
        );

Measure &quot;Employed female population with a university degree or more&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO23',
          'denominator'
        );

Measure &quot;Employed female population with a university degree or more&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO23',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population with a university degree or more](#mx-inegi-columns-eco22)


#### <a name="id41"></a><a name="id42"></a>Employed male population with a university degree or more

[<img alt="../../_images/mx.inegi_columns.ECO24.png" src="../../_images/mx.inegi_columns.ECO24.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO24.png)Measure &quot;Employed male population with a university degree or more&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO24'
        );

Measure &quot;Employed male population with a university degree or more&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO24'
        );

Measure &quot;Employed male population with a university degree or more&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO24',
          'denominator'
        );

Measure &quot;Employed male population with a university degree or more&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO24',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population with a university degree or more](#mx-inegi-columns-eco22)


#### <a name="employed-population-with-incomplete-secondary-education"></a><a name="mx-inegi-columns-eco13"></a>Employed population with incomplete secondary education

[<img alt="../../_images/mx.inegi_columns.ECO13.png" src="../../_images/mx.inegi_columns.ECO13.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO13.png)Measure &quot;Employed population with incomplete secondary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO13'
        );

Measure &quot;Employed population with incomplete secondary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO13'
        );

Measure &quot;Employed population with incomplete secondary education&quot; percent of &quot;Employed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO13',
          'denominator'
        );

Measure &quot;Employed population with incomplete secondary education&quot; percent of &quot;Employed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO13',
          'denominator'
        );

* denominator: [Employed](#mx-inegi-columns-eco4)


#### <a name="id43"></a><a name="id44"></a>Employed female population with incomplete secondary education

[<img alt="../../_images/mx.inegi_columns.ECO14.png" src="../../_images/mx.inegi_columns.ECO14.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO14.png)Measure &quot;Employed female population with incomplete secondary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO14'
        );

Measure &quot;Employed female population with incomplete secondary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO14'
        );

Measure &quot;Employed female population with incomplete secondary education&quot; percent of &quot;Employed population with incomplete secondary education&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO14',
          'denominator'
        );

Measure &quot;Employed female population with incomplete secondary education&quot; percent of &quot;Employed population with incomplete secondary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO14',
          'denominator'
        );

* denominator: [Employed population with incomplete secondary education](#mx-inegi-columns-eco13)

* denominator: mx.inegi_columns.ECO5


#### <a name="id45"></a><a name="id46"></a>Employed male population with incomplete secondary education

[<img alt="../../_images/mx.inegi_columns.ECO15.png" src="../../_images/mx.inegi_columns.ECO15.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO15.png)Measure &quot;Employed male population with incomplete secondary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO15'
        );

Measure &quot;Employed male population with incomplete secondary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO15'
        );

Measure &quot;Employed male population with incomplete secondary education&quot; percent of &quot;Employed population with incomplete secondary education&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO15',
          'denominator'
        );

Measure &quot;Employed male population with incomplete secondary education&quot; percent of &quot;Employed population with incomplete secondary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO15',
          'denominator'
        );

* denominator: [Employed population with incomplete secondary education](#mx-inegi-columns-eco13)

* denominator: mx.inegi_columns.ECO6


#### <a name="employed-population-with-primary-education"></a><a name="mx-inegi-columns-eco10"></a>Employed population with primary education

[<img alt="../../_images/mx.inegi_columns.ECO10.png" src="../../_images/mx.inegi_columns.ECO10.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO10.png)Measure &quot;Employed population with primary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO10'
        );

Measure &quot;Employed population with primary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO10'
        );

Measure &quot;Employed population with primary education&quot; percent of &quot;Employed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO10',
          'denominator'
        );

Measure &quot;Employed population with primary education&quot; percent of &quot;Employed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO10',
          'denominator'
        );

* denominator: [Employed](#mx-inegi-columns-eco4)


#### <a name="id47"></a><a name="id48"></a>Employed female population with primary education

[<img alt="../../_images/mx.inegi_columns.ECO11.png" src="../../_images/mx.inegi_columns.ECO11.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO11.png)Measure &quot;Employed female population with primary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO11'
        );

Measure &quot;Employed female population with primary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO11'
        );

Measure &quot;Employed female population with primary education&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO11',
          'denominator'
        );

Measure &quot;Employed female population with primary education&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO11',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population with primary education](#mx-inegi-columns-eco10)


#### <a name="id49"></a><a name="id50"></a>Employed male population with primary education

[<img alt="../../_images/mx.inegi_columns.ECO12.png" src="../../_images/mx.inegi_columns.ECO12.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO12.png)Measure &quot;Employed male population with primary education&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO12'
        );

Measure &quot;Employed male population with primary education&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO12'
        );

Measure &quot;Employed male population with primary education&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO12',
          'denominator'
        );

Measure &quot;Employed male population with primary education&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO12',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population with primary education](#mx-inegi-columns-eco10)


#### <a name="employed-population-without-schooling"></a><a name="mx-inegi-columns-eco7"></a>Employed population without schooling

[<img alt="../../_images/mx.inegi_columns.ECO7.png" src="../../_images/mx.inegi_columns.ECO7.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO7.png)Measure &quot;Employed population without schooling&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO7'
        );

Measure &quot;Employed population without schooling&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO7'
        );

Measure &quot;Employed population without schooling&quot; percent of &quot;Employed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO7',
          'denominator'
        );

Measure &quot;Employed population without schooling&quot; percent of &quot;Employed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO7',
          'denominator'
        );

* denominator: [Employed](#mx-inegi-columns-eco4)


#### <a name="id51"></a><a name="id52"></a>Employed female population without schooling

[<img alt="../../_images/mx.inegi_columns.ECO8.png" src="../../_images/mx.inegi_columns.ECO8.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO8.png)Measure &quot;Employed female population without schooling&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO8'
        );

Measure &quot;Employed female population without schooling&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO8'
        );

Measure &quot;Employed female population without schooling&quot; percent of &quot;Employed female&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO8',
          'denominator'
        );

Measure &quot;Employed female population without schooling&quot; percent of &quot;Employed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO8',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO5

* denominator: [Employed population without schooling](#mx-inegi-columns-eco7)


#### <a name="id53"></a><a name="id54"></a>Employed male population without schooling

[<img alt="../../_images/mx.inegi_columns.ECO9.png" src="../../_images/mx.inegi_columns.ECO9.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO9.png)Measure &quot;Employed male population without schooling&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO9'
        );

Measure &quot;Employed male population without schooling&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO9'
        );

Measure &quot;Employed male population without schooling&quot; percent of &quot;Employed male&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO9',
          'denominator'
        );

Measure &quot;Employed male population without schooling&quot; percent of &quot;Employed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO9',
          'denominator'
        );

* denominator: mx.inegi_columns.ECO6

* denominator: [Employed population without schooling](#mx-inegi-columns-eco7)


### <a name="unemployed"></a><a name="mx-inegi-columns-eco25"></a>Unemployed

[<img alt="../../_images/mx.inegi_columns.ECO25.png" src="../../_images/mx.inegi_columns.ECO25.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO25.png)Measure &quot;Unemployed&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO25'
        );

Measure &quot;Unemployed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO25'
        );

Measure &quot;Unemployed&quot; percent of &quot;Economically active population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO25',
          'denominator'
        );

Measure &quot;Unemployed&quot; percent of &quot;Economically active population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO25',
          'denominator'
        );

* denominator: [Economically active population](#mx-inegi-columns-eco1)

Subcolumns of Unemployed

- [Unemployed female](#id55)

- [Unemployed male](#id57)


#### <a name="id55"></a><a name="id56"></a>Unemployed female

[<img alt="../../_images/mx.inegi_columns.ECO26.png" src="../../_images/mx.inegi_columns.ECO26.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO26.png)Measure &quot;Unemployed female&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO26'
        );

Measure &quot;Unemployed female&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO26'
        );

Measure &quot;Unemployed female&quot; percent of &quot;Unemployed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO26',
          'denominator'
        );

Measure &quot;Unemployed female&quot; percent of &quot;Unemployed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO26',
          'denominator'
        );

* denominator: [Unemployed](#mx-inegi-columns-eco25)

* denominator: [Economically active female population](#mx-inegi-columns-eco2)


#### <a name="id57"></a><a name="id58"></a>Unemployed male

[<img alt="../../_images/mx.inegi_columns.ECO27.png" src="../../_images/mx.inegi_columns.ECO27.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO27.png)Measure &quot;Unemployed male&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO27'
        );

Measure &quot;Unemployed male&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO27'
        );

Measure &quot;Unemployed male&quot; percent of &quot;Unemployed&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO27',
          'denominator'
        );

Measure &quot;Unemployed male&quot; percent of &quot;Unemployed&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO27',
          'denominator'
        );

* denominator: [Unemployed](#mx-inegi-columns-eco25)

* denominator: [Economically active male population](#mx-inegi-columns-eco3)


## <a name="economically-inactive"></a><a name="mx-inegi-columns-eco28"></a>Economically inactive

[<img alt="../../_images/mx.inegi_columns.ECO28.png" src="../../_images/mx.inegi_columns.ECO28.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO28.png)Measure &quot;Economically inactive&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO28'
        );

Measure &quot;Economically inactive&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO28'
        );

Measure &quot;Economically inactive&quot; percent of &quot;Total population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO28',
          'denominator'
        );

Measure &quot;Economically inactive&quot; percent of &quot;Total population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO28',
          'denominator'
        );

* denominator: [Total population](../age_gender/#mx-inegi-columns-pob1)

Subcolumns of Economically inactive

- [Dedicated to housework](#dedicated-to-housework)

- [Economically inactive with a physical or mental limitation impeding work](#economically-inactive-with-a-physical-or-mental-limitation-impeding-work)

- [Economically inactive with some other reason that impedes work](#economically-inactive-with-some-other-reason-that-impedes-work)

- [Female economically inactive population](#female-economically-inactive-population)

- [Full time student](#full-time-student)

- [Male economically inactive population](#male-economically-inactive-population)

- [Retiree or receiving pension](#retiree-or-receiving-pension)


### <a name="dedicated-to-housework"></a><a name="mx-inegi-columns-eco37"></a>Dedicated to housework

[<img alt="../../_images/mx.inegi_columns.ECO37.png" src="../../_images/mx.inegi_columns.ECO37.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO37.png)Measure &quot;Dedicated to housework&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO37'
        );

Measure &quot;Dedicated to housework&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO37'
        );

Measure &quot;Dedicated to housework&quot; percent of &quot;Economically inactive&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO37',
          'denominator'
        );

Measure &quot;Dedicated to housework&quot; percent of &quot;Economically inactive&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO37',
          'denominator'
        );

* denominator: [Economically inactive](#mx-inegi-columns-eco28)

Subcolumns of Dedicated to housework

- [Female dedicated to housework](#female-dedicated-to-housework)

- [Male dedicated to housework](#male-dedicated-to-housework)


#### <a name="female-dedicated-to-housework"></a><a name="mx-inegi-columns-eco38"></a>Female dedicated to housework

[<img alt="../../_images/mx.inegi_columns.ECO38.png" src="../../_images/mx.inegi_columns.ECO38.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO38.png)Measure &quot;Female dedicated to housework&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO38'
        );

Measure &quot;Female dedicated to housework&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO38'
        );

Measure &quot;Female dedicated to housework&quot; percent of &quot;Female economically inactive population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO38',
          'denominator'
        );

Measure &quot;Female dedicated to housework&quot; percent of &quot;Female economically inactive population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO38',
          'denominator'
        );

* denominator: [Female economically inactive population](#mx-inegi-columns-eco29)

* denominator: [Dedicated to housework](#mx-inegi-columns-eco37)


#### <a name="male-dedicated-to-housework"></a><a name="mx-inegi-columns-eco39"></a>Male dedicated to housework

[<img alt="../../_images/mx.inegi_columns.ECO39.png" src="../../_images/mx.inegi_columns.ECO39.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO39.png)Measure &quot;Male dedicated to housework&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO39'
        );

Measure &quot;Male dedicated to housework&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO39'
        );

Measure &quot;Male dedicated to housework&quot; percent of &quot;Male economically inactive population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO39',
          'denominator'
        );

Measure &quot;Male dedicated to housework&quot; percent of &quot;Male economically inactive population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO39',
          'denominator'
        );

* denominator: [Male economically inactive population](#mx-inegi-columns-eco30)

* denominator: [Dedicated to housework](#mx-inegi-columns-eco37)


### <a name="economically-inactive-with-a-physical-or-mental-limitation-impeding-work"></a><a name="mx-inegi-columns-eco40"></a>Economically inactive with a physical or mental limitation impeding work

[<img alt="../../_images/mx.inegi_columns.ECO40.png" src="../../_images/mx.inegi_columns.ECO40.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO40.png)Measure &quot;Economically inactive with a physical or mental limitation impeding work&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO40'
        );

Measure &quot;Economically inactive with a physical or mental limitation impeding work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO40'
        );

Measure &quot;Economically inactive with a physical or mental limitation impeding work&quot; percent of &quot;Economically inactive&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO40',
          'denominator'
        );

Measure &quot;Economically inactive with a physical or mental limitation impeding work&quot; percent of &quot;Economically inactive&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO40',
          'denominator'
        );

* denominator: [Economically inactive](#mx-inegi-columns-eco28)

Subcolumns of Economically inactive with a physical or mental limitation impeding work

- [Female economically inactive with a physical or mental limitation impeding work](#female-economically-inactive-with-a-physical-or-mental-limitation-impeding-work)

- [Male economically inactive with a physical or mental limitation impeding work](#male-economically-inactive-with-a-physical-or-mental-limitation-impeding-work)


#### <a name="female-economically-inactive-with-a-physical-or-mental-limitation-impeding-work"></a><a name="mx-inegi-columns-eco41"></a>Female economically inactive with a physical or mental limitation impeding work

[<img alt="../../_images/mx.inegi_columns.ECO41.png" src="../../_images/mx.inegi_columns.ECO41.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO41.png)Measure &quot;Female economically inactive with a physical or mental limitation impeding work&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO41'
        );

Measure &quot;Female economically inactive with a physical or mental limitation impeding work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO41'
        );

Measure &quot;Female economically inactive with a physical or mental limitation impeding work&quot; percent of &quot;Female economically inactive population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO41',
          'denominator'
        );

Measure &quot;Female economically inactive with a physical or mental limitation impeding work&quot; percent of &quot;Female economically inactive population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO41',
          'denominator'
        );

* denominator: [Female economically inactive population](#mx-inegi-columns-eco29)

* denominator: [Economically inactive with a physical or mental limitation impeding work](#mx-inegi-columns-eco40)


#### <a name="male-economically-inactive-with-a-physical-or-mental-limitation-impeding-work"></a><a name="mx-inegi-columns-eco42"></a>Male economically inactive with a physical or mental limitation impeding work

[<img alt="../../_images/mx.inegi_columns.ECO42.png" src="../../_images/mx.inegi_columns.ECO42.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO42.png)Measure &quot;Male economically inactive with a physical or mental limitation impeding work&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO42'
        );

Measure &quot;Male economically inactive with a physical or mental limitation impeding work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO42'
        );

Measure &quot;Male economically inactive with a physical or mental limitation impeding work&quot; percent of &quot;Male economically inactive population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO42',
          'denominator'
        );

Measure &quot;Male economically inactive with a physical or mental limitation impeding work&quot; percent of &quot;Male economically inactive population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO42',
          'denominator'
        );

* denominator: [Male economically inactive population](#mx-inegi-columns-eco30)

* denominator: [Economically inactive with a physical or mental limitation impeding work](#mx-inegi-columns-eco40)


### <a name="economically-inactive-with-some-other-reason-that-impedes-work"></a><a name="mx-inegi-columns-eco43"></a>Economically inactive with some other reason that impedes work

[<img alt="../../_images/mx.inegi_columns.ECO43.png" src="../../_images/mx.inegi_columns.ECO43.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO43.png)Measure &quot;Economically inactive with some other reason that impedes work&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO43'
        );

Measure &quot;Economically inactive with some other reason that impedes work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO43'
        );

Measure &quot;Economically inactive with some other reason that impedes work&quot; percent of &quot;Economically inactive&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO43',
          'denominator'
        );

Measure &quot;Economically inactive with some other reason that impedes work&quot; percent of &quot;Economically inactive&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO43',
          'denominator'
        );

* denominator: [Economically inactive](#mx-inegi-columns-eco28)

Subcolumns of Economically inactive with some other reason that impedes work

- [Female economically inactive with some other reason that impedes work](#female-economically-inactive-with-some-other-reason-that-impedes-work)

- [Male economically inactive with some other reason that impedes work](#male-economically-inactive-with-some-other-reason-that-impedes-work)


#### <a name="female-economically-inactive-with-some-other-reason-that-impedes-work"></a><a name="mx-inegi-columns-eco44"></a>Female economically inactive with some other reason that impedes work

[<img alt="../../_images/mx.inegi_columns.ECO44.png" src="../../_images/mx.inegi_columns.ECO44.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO44.png)Measure &quot;Female economically inactive with some other reason that impedes work&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO44'
        );

Measure &quot;Female economically inactive with some other reason that impedes work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO44'
        );

Measure &quot;Female economically inactive with some other reason that impedes work&quot; percent of &quot;Economically inactive with some other reason that impedes work&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO44',
          'denominator'
        );

Measure &quot;Female economically inactive with some other reason that impedes work&quot; percent of &quot;Economically inactive with some other reason that impedes work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO44',
          'denominator'
        );

* denominator: [Economically inactive with some other reason that impedes work](#mx-inegi-columns-eco43)

* denominator: [Female economically inactive population](#mx-inegi-columns-eco29)


#### <a name="male-economically-inactive-with-some-other-reason-that-impedes-work"></a><a name="mx-inegi-columns-eco45"></a>Male economically inactive with some other reason that impedes work

[<img alt="../../_images/mx.inegi_columns.ECO45.png" src="../../_images/mx.inegi_columns.ECO45.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO45.png)Measure &quot;Male economically inactive with some other reason that impedes work&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO45'
        );

Measure &quot;Male economically inactive with some other reason that impedes work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO45'
        );

Measure &quot;Male economically inactive with some other reason that impedes work&quot; percent of &quot;Economically inactive with some other reason that impedes work&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO45',
          'denominator'
        );

Measure &quot;Male economically inactive with some other reason that impedes work&quot; percent of &quot;Economically inactive with some other reason that impedes work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO45',
          'denominator'
        );

* denominator: [Economically inactive with some other reason that impedes work](#mx-inegi-columns-eco43)

* denominator: [Male economically inactive population](#mx-inegi-columns-eco30)


### <a name="female-economically-inactive-population"></a><a name="mx-inegi-columns-eco29"></a>Female economically inactive population

[<img alt="../../_images/mx.inegi_columns.ECO29.png" src="../../_images/mx.inegi_columns.ECO29.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO29.png)Measure &quot;Female economically inactive population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO29'
        );

Measure &quot;Female economically inactive population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO29'
        );

Measure &quot;Female economically inactive population&quot; percent of &quot;Female population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO29',
          'denominator'
        );

Measure &quot;Female economically inactive population&quot; percent of &quot;Female population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO29',
          'denominator'
        );

* denominator: [Female population](../age_gender/#mx-inegi-columns-pob31)

* denominator: [Economically inactive](#mx-inegi-columns-eco28)

Subcolumns of Female economically inactive population

- [Female dedicated to housework](#id59)

- [Female economically inactive with a physical or mental limitation impeding work](#id61)

- [Female economically inactive with some other reason that impedes work](#id63)

- [Female full time student](#female-full-time-student)

- [Female retiree or receiving pension](#female-retiree-or-receiving-pension)


#### <a name="id59"></a><a name="id60"></a>Female dedicated to housework

[<img alt="../../_images/mx.inegi_columns.ECO38.png" src="../../_images/mx.inegi_columns.ECO38.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO38.png)Measure &quot;Female dedicated to housework&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO38'
        );

Measure &quot;Female dedicated to housework&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO38'
        );

Measure &quot;Female dedicated to housework&quot; percent of &quot;Female economically inactive population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO38',
          'denominator'
        );

Measure &quot;Female dedicated to housework&quot; percent of &quot;Female economically inactive population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO38',
          'denominator'
        );

* denominator: [Female economically inactive population](#mx-inegi-columns-eco29)

* denominator: [Dedicated to housework](#mx-inegi-columns-eco37)


#### <a name="id61"></a><a name="id62"></a>Female economically inactive with a physical or mental limitation impeding work

[<img alt="../../_images/mx.inegi_columns.ECO41.png" src="../../_images/mx.inegi_columns.ECO41.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO41.png)Measure &quot;Female economically inactive with a physical or mental limitation impeding work&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO41'
        );

Measure &quot;Female economically inactive with a physical or mental limitation impeding work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO41'
        );

Measure &quot;Female economically inactive with a physical or mental limitation impeding work&quot; percent of &quot;Female economically inactive population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO41',
          'denominator'
        );

Measure &quot;Female economically inactive with a physical or mental limitation impeding work&quot; percent of &quot;Female economically inactive population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO41',
          'denominator'
        );

* denominator: [Female economically inactive population](#mx-inegi-columns-eco29)

* denominator: [Economically inactive with a physical or mental limitation impeding work](#mx-inegi-columns-eco40)


#### <a name="id63"></a><a name="id64"></a>Female economically inactive with some other reason that impedes work

[<img alt="../../_images/mx.inegi_columns.ECO44.png" src="../../_images/mx.inegi_columns.ECO44.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO44.png)Measure &quot;Female economically inactive with some other reason that impedes work&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO44'
        );

Measure &quot;Female economically inactive with some other reason that impedes work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO44'
        );

Measure &quot;Female economically inactive with some other reason that impedes work&quot; percent of &quot;Economically inactive with some other reason that impedes work&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO44',
          'denominator'
        );

Measure &quot;Female economically inactive with some other reason that impedes work&quot; percent of &quot;Economically inactive with some other reason that impedes work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO44',
          'denominator'
        );

* denominator: [Economically inactive with some other reason that impedes work](#mx-inegi-columns-eco43)

* denominator: [Female economically inactive population](#mx-inegi-columns-eco29)


#### <a name="female-full-time-student"></a><a name="mx-inegi-columns-eco35"></a>Female full time student

[<img alt="../../_images/mx.inegi_columns.ECO35.png" src="../../_images/mx.inegi_columns.ECO35.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO35.png)Measure &quot;Female full time student&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO35'
        );

Measure &quot;Female full time student&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO35'
        );

Measure &quot;Female full time student&quot; percent of &quot;Full time student&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO35',
          'denominator'
        );

Measure &quot;Female full time student&quot; percent of &quot;Full time student&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO35',
          'denominator'
        );

* denominator: [Full time student](#mx-inegi-columns-eco34)

* denominator: [Female economically inactive population](#mx-inegi-columns-eco29)


#### <a name="female-retiree-or-receiving-pension"></a><a name="mx-inegi-columns-eco32"></a>Female retiree or receiving pension

[<img alt="../../_images/mx.inegi_columns.ECO32.png" src="../../_images/mx.inegi_columns.ECO32.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO32.png)Measure &quot;Female retiree or receiving pension&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO32'
        );

Measure &quot;Female retiree or receiving pension&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO32'
        );

Measure &quot;Female retiree or receiving pension&quot; percent of &quot;Retiree or receiving pension&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO32',
          'denominator'
        );

Measure &quot;Female retiree or receiving pension&quot; percent of &quot;Retiree or receiving pension&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO32',
          'denominator'
        );

* denominator: [Retiree or receiving pension](#mx-inegi-columns-eco31)

* denominator: [Female economically inactive population](#mx-inegi-columns-eco29)


### <a name="full-time-student"></a><a name="mx-inegi-columns-eco34"></a>Full time student

[<img alt="../../_images/mx.inegi_columns.ECO34.png" src="../../_images/mx.inegi_columns.ECO34.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO34.png)Measure &quot;Full time student&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO34'
        );

Measure &quot;Full time student&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO34'
        );

Measure &quot;Full time student&quot; percent of &quot;Economically inactive&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO34',
          'denominator'
        );

Measure &quot;Full time student&quot; percent of &quot;Economically inactive&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO34',
          'denominator'
        );

* denominator: [Economically inactive](#mx-inegi-columns-eco28)

Subcolumns of Full time student

- [Female full time student](#id65)

- [Male full time student](#male-full-time-student)


#### <a name="id65"></a><a name="id66"></a>Female full time student

[<img alt="../../_images/mx.inegi_columns.ECO35.png" src="../../_images/mx.inegi_columns.ECO35.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO35.png)Measure &quot;Female full time student&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO35'
        );

Measure &quot;Female full time student&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO35'
        );

Measure &quot;Female full time student&quot; percent of &quot;Full time student&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO35',
          'denominator'
        );

Measure &quot;Female full time student&quot; percent of &quot;Full time student&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO35',
          'denominator'
        );

* denominator: [Full time student](#mx-inegi-columns-eco34)

* denominator: [Female economically inactive population](#mx-inegi-columns-eco29)


#### <a name="male-full-time-student"></a><a name="mx-inegi-columns-eco36"></a>Male full time student

[<img alt="../../_images/mx.inegi_columns.ECO36.png" src="../../_images/mx.inegi_columns.ECO36.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO36.png)Measure &quot;Male full time student&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO36'
        );

Measure &quot;Male full time student&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO36'
        );

Measure &quot;Male full time student&quot; percent of &quot;Full time student&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO36',
          'denominator'
        );

Measure &quot;Male full time student&quot; percent of &quot;Full time student&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO36',
          'denominator'
        );

* denominator: [Full time student](#mx-inegi-columns-eco34)

* denominator: [Male economically inactive population](#mx-inegi-columns-eco30)


### <a name="male-economically-inactive-population"></a><a name="mx-inegi-columns-eco30"></a>Male economically inactive population

[<img alt="../../_images/mx.inegi_columns.ECO30.png" src="../../_images/mx.inegi_columns.ECO30.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO30.png)Measure &quot;Male economically inactive population&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO30'
        );

Measure &quot;Male economically inactive population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO30'
        );

Measure &quot;Male economically inactive population&quot; percent of &quot;Economically inactive&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO30',
          'denominator'
        );

Measure &quot;Male economically inactive population&quot; percent of &quot;Economically inactive&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO30',
          'denominator'
        );

* denominator: [Economically inactive](#mx-inegi-columns-eco28)

* denominator: [Male population](../age_gender/#mx-inegi-columns-pob57)

Subcolumns of Male economically inactive population

- [Male dedicated to housework](#id67)

- [Male economically inactive with a physical or mental limitation impeding work](#id69)

- [Male economically inactive with some other reason that impedes work](#id71)

- [Male full time student](#id73)

- [Male retiree or receiving pension](#male-retiree-or-receiving-pension)


#### <a name="id67"></a><a name="id68"></a>Male dedicated to housework

[<img alt="../../_images/mx.inegi_columns.ECO39.png" src="../../_images/mx.inegi_columns.ECO39.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO39.png)Measure &quot;Male dedicated to housework&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO39'
        );

Measure &quot;Male dedicated to housework&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO39'
        );

Measure &quot;Male dedicated to housework&quot; percent of &quot;Male economically inactive population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO39',
          'denominator'
        );

Measure &quot;Male dedicated to housework&quot; percent of &quot;Male economically inactive population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO39',
          'denominator'
        );

* denominator: [Male economically inactive population](#mx-inegi-columns-eco30)

* denominator: [Dedicated to housework](#mx-inegi-columns-eco37)


#### <a name="id69"></a><a name="id70"></a>Male economically inactive with a physical or mental limitation impeding work

[<img alt="../../_images/mx.inegi_columns.ECO42.png" src="../../_images/mx.inegi_columns.ECO42.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO42.png)Measure &quot;Male economically inactive with a physical or mental limitation impeding work&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO42'
        );

Measure &quot;Male economically inactive with a physical or mental limitation impeding work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO42'
        );

Measure &quot;Male economically inactive with a physical or mental limitation impeding work&quot; percent of &quot;Male economically inactive population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO42',
          'denominator'
        );

Measure &quot;Male economically inactive with a physical or mental limitation impeding work&quot; percent of &quot;Male economically inactive population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO42',
          'denominator'
        );

* denominator: [Male economically inactive population](#mx-inegi-columns-eco30)

* denominator: [Economically inactive with a physical or mental limitation impeding work](#mx-inegi-columns-eco40)


#### <a name="id71"></a><a name="id72"></a>Male economically inactive with some other reason that impedes work

[<img alt="../../_images/mx.inegi_columns.ECO45.png" src="../../_images/mx.inegi_columns.ECO45.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO45.png)Measure &quot;Male economically inactive with some other reason that impedes work&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO45'
        );

Measure &quot;Male economically inactive with some other reason that impedes work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO45'
        );

Measure &quot;Male economically inactive with some other reason that impedes work&quot; percent of &quot;Economically inactive with some other reason that impedes work&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO45',
          'denominator'
        );

Measure &quot;Male economically inactive with some other reason that impedes work&quot; percent of &quot;Economically inactive with some other reason that impedes work&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO45',
          'denominator'
        );

* denominator: [Economically inactive with some other reason that impedes work](#mx-inegi-columns-eco43)

* denominator: [Male economically inactive population](#mx-inegi-columns-eco30)


#### <a name="id73"></a><a name="id74"></a>Male full time student

[<img alt="../../_images/mx.inegi_columns.ECO36.png" src="../../_images/mx.inegi_columns.ECO36.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO36.png)Measure &quot;Male full time student&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO36'
        );

Measure &quot;Male full time student&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO36'
        );

Measure &quot;Male full time student&quot; percent of &quot;Full time student&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO36',
          'denominator'
        );

Measure &quot;Male full time student&quot; percent of &quot;Full time student&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO36',
          'denominator'
        );

* denominator: [Full time student](#mx-inegi-columns-eco34)

* denominator: [Male economically inactive population](#mx-inegi-columns-eco30)


#### <a name="male-retiree-or-receiving-pension"></a><a name="mx-inegi-columns-eco33"></a>Male retiree or receiving pension

[<img alt="../../_images/mx.inegi_columns.ECO33.png" src="../../_images/mx.inegi_columns.ECO33.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO33.png)Measure &quot;Male retiree or receiving pension&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO33'
        );

Measure &quot;Male retiree or receiving pension&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO33'
        );

Measure &quot;Male retiree or receiving pension&quot; percent of &quot;Retiree or receiving pension&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO33',
          'denominator'
        );

Measure &quot;Male retiree or receiving pension&quot; percent of &quot;Retiree or receiving pension&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO33',
          'denominator'
        );

* denominator: [Retiree or receiving pension](#mx-inegi-columns-eco31)

* denominator: [Male economically inactive population](#mx-inegi-columns-eco30)


### <a name="retiree-or-receiving-pension"></a><a name="mx-inegi-columns-eco31"></a>Retiree or receiving pension

[<img alt="../../_images/mx.inegi_columns.ECO31.png" src="../../_images/mx.inegi_columns.ECO31.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO31.png)Measure &quot;Retiree or receiving pension&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO31'
        );

Measure &quot;Retiree or receiving pension&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO31'
        );

Measure &quot;Retiree or receiving pension&quot; percent of &quot;Economically inactive&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO31',
          'denominator'
        );

Measure &quot;Retiree or receiving pension&quot; percent of &quot;Economically inactive&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO31',
          'denominator'
        );

* denominator: [Economically inactive](#mx-inegi-columns-eco28)

Subcolumns of Retiree or receiving pension

- [Female retiree or receiving pension](#id75)

- [Male retiree or receiving pension](#id77)


#### <a name="id75"></a><a name="id76"></a>Female retiree or receiving pension

[<img alt="../../_images/mx.inegi_columns.ECO32.png" src="../../_images/mx.inegi_columns.ECO32.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO32.png)Measure &quot;Female retiree or receiving pension&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO32'
        );

Measure &quot;Female retiree or receiving pension&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO32'
        );

Measure &quot;Female retiree or receiving pension&quot; percent of &quot;Retiree or receiving pension&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO32',
          'denominator'
        );

Measure &quot;Female retiree or receiving pension&quot; percent of &quot;Retiree or receiving pension&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO32',
          'denominator'
        );

* denominator: [Retiree or receiving pension](#mx-inegi-columns-eco31)

* denominator: [Female economically inactive population](#mx-inegi-columns-eco29)


#### <a name="id77"></a><a name="id78"></a>Male retiree or receiving pension

[<img alt="../../_images/mx.inegi_columns.ECO33.png" src="../../_images/mx.inegi_columns.ECO33.png" style="width: 100%;" />](../../_images/mx.inegi_columns.ECO33.png)Measure &quot;Male retiree or receiving pension&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO33'
        );

Measure &quot;Male retiree or receiving pension&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO33'
        );

Measure &quot;Male retiree or receiving pension&quot; percent of &quot;Retiree or receiving pension&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'mx.inegi_columns.ECO33',
          'denominator'
        );

Measure &quot;Male retiree or receiving pension&quot; percent of &quot;Retiree or receiving pension&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'mx.inegi_columns.ECO33',
          'denominator'
        );

* denominator: [Retiree or receiving pension](#mx-inegi-columns-eco31)

* denominator: [Male economically inactive population](#mx-inegi-columns-eco30)


