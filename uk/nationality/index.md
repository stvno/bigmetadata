

  
# <a name="nationality"></a>Nationality

Population breakdowns by nationality and place of birth.

- [Born in Africa](#born-in-africa)

    * [Born in Central and Western Africa](#born-in-central-and-western-africa)

    * [Born in North Africa](#born-in-north-africa)

    * [Born in South and Eastern Africa](#born-in-south-and-eastern-africa)

- [Born in Australia, Australaisa, Oceania, and Antarctica](#born-in-australia-australaisa-oceania-and-antarctica)

- [Born in Europe](#born-in-europe)

    * [Born in the UK](#born-in-the-uk)

        - [Born in England](#born-in-england)

        - [Born in Northern Ireland](#born-in-northern-ireland)

        - [Born in Scotland](#born-in-scotland)

        - [Born in Wales](#born-in-wales)

- [Born in Europe outside the UK and Ireland](#born-in-europe-outside-the-uk-and-ireland)

- [Born in Ireland](#born-in-ireland)

- [Born in the Americas and Caribbean](#born-in-the-americas-and-caribbean)

    * [Born in Central and South America](#born-in-central-and-south-america)

    * [Born in North America](#born-in-north-america)

- [Born in the Middle East and Asia](#born-in-the-middle-east-and-asia)

    * [Born in Central Asia](#born-in-central-asia)

    * [Born in Eastern Asia](#born-in-eastern-asia)

    * [Born in Southeast Asia](#born-in-southeast-asia)

    * [Born in Southern Asia](#born-in-southern-asia)

    * [Born in the Middle East](#born-in-the-middle-east)


## <a name="born-in-africa"></a><a name="uk-ons-lc2205ew0136"></a>Born in Africa

Measure &quot;Born in Africa&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0136'
        );

Measure &quot;Born in Africa&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0136'
        );

Measure &quot;Born in Africa&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0136',
          'denominator'
        );

Measure &quot;Born in Africa&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0136',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)

Subcolumns of Born in Africa

- [Born in Central and Western Africa](#born-in-central-and-western-africa)

- [Born in North Africa](#born-in-north-africa)

- [Born in South and Eastern Africa](#born-in-south-and-eastern-africa)


### <a name="born-in-central-and-western-africa"></a><a name="uk-ons-lc2205ew0154"></a>Born in Central and Western Africa

Measure &quot;Born in Central and Western Africa&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0154'
        );

Measure &quot;Born in Central and Western Africa&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0154'
        );

Measure &quot;Born in Central and Western Africa&quot; percent of &quot;Born in Africa&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0154',
          'denominator'
        );

Measure &quot;Born in Central and Western Africa&quot; percent of &quot;Born in Africa&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0154',
          'denominator'
        );

* denominator: [Born in Africa](#uk-ons-lc2205ew0136)


### <a name="born-in-north-africa"></a><a name="uk-ons-lc2205ew0145"></a>Born in North Africa

Measure &quot;Born in North Africa&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0145'
        );

Measure &quot;Born in North Africa&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0145'
        );

Measure &quot;Born in North Africa&quot; percent of &quot;Born in Africa&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0145',
          'denominator'
        );

Measure &quot;Born in North Africa&quot; percent of &quot;Born in Africa&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0145',
          'denominator'
        );

* denominator: [Born in Africa](#uk-ons-lc2205ew0136)


### <a name="born-in-south-and-eastern-africa"></a><a name="uk-ons-lc2205ew0163"></a>Born in South and Eastern Africa

Measure &quot;Born in South and Eastern Africa&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0163'
        );

Measure &quot;Born in South and Eastern Africa&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0163'
        );

Measure &quot;Born in South and Eastern Africa&quot; percent of &quot;Born in Africa&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0163',
          'denominator'
        );

Measure &quot;Born in South and Eastern Africa&quot; percent of &quot;Born in Africa&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0163',
          'denominator'
        );

* denominator: [Born in Africa](#uk-ons-lc2205ew0136)


## <a name="born-in-australia-australaisa-oceania-and-antarctica"></a><a name="uk-ons-lc2205ew0262"></a>Born in Australia, Australaisa, Oceania, and Antarctica

Measure &quot;Born in Australia, Australaisa, Oceania, and Antarctica&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0262'
        );

Measure &quot;Born in Australia, Australaisa, Oceania, and Antarctica&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0262'
        );

Measure &quot;Born in Australia, Australaisa, Oceania, and Antarctica&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0262',
          'denominator'
        );

Measure &quot;Born in Australia, Australaisa, Oceania, and Antarctica&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0262',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


## <a name="born-in-europe"></a><a name="uk-ons-lc2205ew0010"></a>Born in Europe

Measure &quot;Born in Europe&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0010'
        );

Measure &quot;Born in Europe&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0010'
        );

Measure &quot;Born in Europe&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0010',
          'denominator'
        );

Measure &quot;Born in Europe&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0010',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)

Subcolumns of Born in Europe

- [Born in the UK](#born-in-the-uk)


### <a name="born-in-the-uk"></a><a name="uk-ons-lc2205ew0019"></a>Born in the UK

Measure &quot;Born in the UK&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0019'
        );

Measure &quot;Born in the UK&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0019'
        );

Measure &quot;Born in the UK&quot; percent of &quot;Born in Europe&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0019',
          'denominator'
        );

Measure &quot;Born in the UK&quot; percent of &quot;Born in Europe&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0019',
          'denominator'
        );

* denominator: [Born in Europe](#uk-ons-lc2205ew0010)

Subcolumns of Born in the UK

- [Born in England](#born-in-england)

- [Born in Northern Ireland](#born-in-northern-ireland)

- [Born in Scotland](#born-in-scotland)

- [Born in Wales](#born-in-wales)


#### <a name="born-in-england"></a><a name="uk-ons-lc2205ew0028"></a>Born in England

Measure &quot;Born in England&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0028'
        );

Measure &quot;Born in England&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0028'
        );

Measure &quot;Born in England&quot; percent of &quot;Born in the UK&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0028',
          'denominator'
        );

Measure &quot;Born in England&quot; percent of &quot;Born in the UK&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0028',
          'denominator'
        );

* denominator: [Born in the UK](#uk-ons-lc2205ew0019)


#### <a name="born-in-northern-ireland"></a><a name="uk-ons-lc2205ew0037"></a>Born in Northern Ireland

Measure &quot;Born in Northern Ireland&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0037'
        );

Measure &quot;Born in Northern Ireland&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0037'
        );

Measure &quot;Born in Northern Ireland&quot; percent of &quot;Born in the UK&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0037',
          'denominator'
        );

Measure &quot;Born in Northern Ireland&quot; percent of &quot;Born in the UK&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0037',
          'denominator'
        );

* denominator: [Born in the UK](#uk-ons-lc2205ew0019)


#### <a name="born-in-scotland"></a><a name="uk-ons-lc2205ew0046"></a>Born in Scotland

Measure &quot;Born in Scotland&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0046'
        );

Measure &quot;Born in Scotland&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0046'
        );

Measure &quot;Born in Scotland&quot; percent of &quot;Born in the UK&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0046',
          'denominator'
        );

Measure &quot;Born in Scotland&quot; percent of &quot;Born in the UK&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0046',
          'denominator'
        );

* denominator: [Born in the UK](#uk-ons-lc2205ew0019)


#### <a name="born-in-wales"></a><a name="uk-ons-lc2205ew0055"></a>Born in Wales

Measure &quot;Born in Wales&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0055'
        );

Measure &quot;Born in Wales&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0055'
        );

Measure &quot;Born in Wales&quot; percent of &quot;Born in the UK&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0055',
          'denominator'
        );

Measure &quot;Born in Wales&quot; percent of &quot;Born in the UK&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0055',
          'denominator'
        );

* denominator: [Born in the UK](#uk-ons-lc2205ew0019)


## <a name="born-in-europe-outside-the-uk-and-ireland"></a><a name="uk-ons-lc2205ew0091"></a>Born in Europe outside the UK and Ireland

Measure &quot;Born in Europe outside the UK and Ireland&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0091'
        );

Measure &quot;Born in Europe outside the UK and Ireland&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0091'
        );

Measure &quot;Born in Europe outside the UK and Ireland&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0091',
          'denominator'
        );

Measure &quot;Born in Europe outside the UK and Ireland&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0091',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


## <a name="born-in-ireland"></a><a name="uk-ons-lc2205ew0082"></a>Born in Ireland

Measure &quot;Born in Ireland&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0082'
        );

Measure &quot;Born in Ireland&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0082'
        );

Measure &quot;Born in Ireland&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0082',
          'denominator'
        );

Measure &quot;Born in Ireland&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0082',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)


## <a name="born-in-the-americas-and-caribbean"></a><a name="uk-ons-lc2205ew0235"></a>Born in the Americas and Caribbean

Measure &quot;Born in the Americas and Caribbean&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0235'
        );

Measure &quot;Born in the Americas and Caribbean&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0235'
        );

Measure &quot;Born in the Americas and Caribbean&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0235',
          'denominator'
        );

Measure &quot;Born in the Americas and Caribbean&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0235',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)

Subcolumns of Born in the Americas and Caribbean

- [Born in Central and South America](#born-in-central-and-south-america)

- [Born in North America](#born-in-north-america)


### <a name="born-in-central-and-south-america"></a><a name="uk-ons-lc2205ew0253"></a>Born in Central and South America

Measure &quot;Born in Central and South America&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0253'
        );

Measure &quot;Born in Central and South America&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0253'
        );

Measure &quot;Born in Central and South America&quot; percent of &quot;Born in the Americas and Caribbean&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0253',
          'denominator'
        );

Measure &quot;Born in Central and South America&quot; percent of &quot;Born in the Americas and Caribbean&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0253',
          'denominator'
        );

* denominator: [Born in the Americas and Caribbean](#uk-ons-lc2205ew0235)


### <a name="born-in-north-america"></a><a name="uk-ons-lc2205ew0244"></a>Born in North America

Measure &quot;Born in North America&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0244'
        );

Measure &quot;Born in North America&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0244'
        );

Measure &quot;Born in North America&quot; percent of &quot;Born in the Americas and Caribbean&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0244',
          'denominator'
        );

Measure &quot;Born in North America&quot; percent of &quot;Born in the Americas and Caribbean&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0244',
          'denominator'
        );

* denominator: [Born in the Americas and Caribbean](#uk-ons-lc2205ew0235)


## <a name="born-in-the-middle-east-and-asia"></a><a name="uk-ons-lc2205ew0181"></a>Born in the Middle East and Asia

Measure &quot;Born in the Middle East and Asia&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0181'
        );

Measure &quot;Born in the Middle East and Asia&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0181'
        );

Measure &quot;Born in the Middle East and Asia&quot; percent of &quot;Total Population&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0181',
          'denominator'
        );

Measure &quot;Born in the Middle East and Asia&quot; percent of &quot;Total Population&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0181',
          'denominator'
        );

* denominator: [Total Population](../age_gender/#uk-ons-lc2102ew0001)

Subcolumns of Born in the Middle East and Asia

- [Born in Central Asia](#born-in-central-asia)

- [Born in Eastern Asia](#born-in-eastern-asia)

- [Born in Southeast Asia](#born-in-southeast-asia)

- [Born in Southern Asia](#born-in-southern-asia)

- [Born in the Middle East](#born-in-the-middle-east)


### <a name="born-in-central-asia"></a><a name="uk-ons-lc2205ew0226"></a>Born in Central Asia

Measure &quot;Born in Central Asia&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0226'
        );

Measure &quot;Born in Central Asia&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0226'
        );

Measure &quot;Born in Central Asia&quot; percent of &quot;Born in the Middle East and Asia&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0226',
          'denominator'
        );

Measure &quot;Born in Central Asia&quot; percent of &quot;Born in the Middle East and Asia&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0226',
          'denominator'
        );

* denominator: [Born in the Middle East and Asia](#uk-ons-lc2205ew0181)


### <a name="born-in-eastern-asia"></a><a name="uk-ons-lc2205ew0199"></a>Born in Eastern Asia

Measure &quot;Born in Eastern Asia&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0199'
        );

Measure &quot;Born in Eastern Asia&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0199'
        );

Measure &quot;Born in Eastern Asia&quot; percent of &quot;Born in the Middle East and Asia&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0199',
          'denominator'
        );

Measure &quot;Born in Eastern Asia&quot; percent of &quot;Born in the Middle East and Asia&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0199',
          'denominator'
        );

* denominator: [Born in the Middle East and Asia](#uk-ons-lc2205ew0181)


### <a name="born-in-southeast-asia"></a><a name="uk-ons-lc2205ew0217"></a>Born in Southeast Asia

Measure &quot;Born in Southeast Asia&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0217'
        );

Measure &quot;Born in Southeast Asia&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0217'
        );

Measure &quot;Born in Southeast Asia&quot; percent of &quot;Born in the Middle East and Asia&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0217',
          'denominator'
        );

Measure &quot;Born in Southeast Asia&quot; percent of &quot;Born in the Middle East and Asia&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0217',
          'denominator'
        );

* denominator: [Born in the Middle East and Asia](#uk-ons-lc2205ew0181)


### <a name="born-in-southern-asia"></a><a name="uk-ons-lc2205ew0208"></a>Born in Southern Asia

Measure &quot;Born in Southern Asia&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0208'
        );

Measure &quot;Born in Southern Asia&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0208'
        );

Measure &quot;Born in Southern Asia&quot; percent of &quot;Born in the Middle East and Asia&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0208',
          'denominator'
        );

Measure &quot;Born in Southern Asia&quot; percent of &quot;Born in the Middle East and Asia&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0208',
          'denominator'
        );

* denominator: [Born in the Middle East and Asia](#uk-ons-lc2205ew0181)


### <a name="born-in-the-middle-east"></a><a name="uk-ons-lc2205ew0190"></a>Born in the Middle East

Measure &quot;Born in the Middle East&quot;  density per sq. kilometer  for one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0190'
        );

Measure &quot;Born in the Middle East&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0190'
        );

Measure &quot;Born in the Middle East&quot; percent of &quot;Born in the Middle East and Asia&quot; at one point:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          CDB_LatLng(40.7, -73.9),
          'uk.ons.LC2205EW0190',
          'denominator'
        );

Measure &quot;Born in the Middle East&quot; percent of &quot;Born in the Middle East and Asia&quot; within an area:

    UPDATE {table_name}
      SET {new_numeric_column} =
        OBS_GetMeasure(
          ST_Buffer(CDB_LatLng(40.7, -73.9), 0.01),
          'uk.ons.LC2205EW0190',
          'denominator'
        );

* denominator: [Born in the Middle East and Asia](#uk-ons-lc2205ew0181)


