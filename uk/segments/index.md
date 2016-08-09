

  
# <a name="population-segments"></a>Population segments

Segmentations of the population

- [Group Area Classification](#group-area-classification)

- [Subgroup Area Classification](#subgroup-area-classification)

- [Supergroup Area Classification](#supergroup-area-classification)


## <a name="group-area-classification"></a><a name="uk-cdrc-grp"></a>Group Area Classification

The 2011 Area Classification for Output Areas (2011 OAC) is a UK geodemographic classification produced as a collaboration between the Office for National Statistics and University College London. Visit [here](http://www.ons.gov.uk/ons/guide-method/geography/products/area-classifications/ns-area-classifications/ns-2011-area-classifications/index.html) or [here](http://www.opengeodemographics.com) for further information regarding the 2011 OAC.

<blockquote>
<div>- <strong>Students Around Campus</strong>:

- <strong>Comfortable Cosmopolitans</strong>:

- <strong>Industrious Communities</strong>:

- <strong>Challenged Terraced Workers</strong>:

- <strong>Ageing City Dwellers</strong>:

- <strong>Aspiring and Affluent</strong>:

- <strong>Asian Traits</strong>:

- <strong>Ageing Rural Dwellers</strong>:

- <strong>White Communities</strong>:

- <strong>Semi-Detached Suburbia</strong>:

- <strong>Migration and Churn</strong>:

- <strong>Rented Family Living</strong>:

- <strong>Challenged Asian Terraces</strong>:

- <strong>Ageing Urban Living</strong>:

- <strong>Suburban Achievers</strong>:

- <strong>Inner-City Students</strong>:

- <strong>Farming Communities</strong>:

- <strong>Ethnic Family Life</strong>:

- <strong>Urban Professionals and Families</strong>:

- <strong>Hard-Pressed Ageing Workers</strong>:

- <strong>Aspirational Techies</strong>:

- <strong>Endeavouring Ethnic Mix</strong>:

- <strong>Ethnic Dynamics</strong>:

- <strong>Rural Tenants</strong>:

- <strong>Challenged Diversity</strong>:

- <strong>Constrained Flat Dwellers</strong>:

</div></blockquote>
Obtain category of &quot;Group Area Classification&quot; at one point:

    UPDATE {table_name}
      SET {new_text_column} =
        OBS_GetCategory(
          CDB_LatLng(40.7, -73.9),
          'uk.cdrc.grp'
        );


## <a name="subgroup-area-classification"></a><a name="uk-cdrc-subgrp"></a>Subgroup Area Classification

The 2011 Area Classification for Output Areas (2011 OAC) is a UK geodemographic classification produced as a collaboration between the Office for National Statistics and University College London. Visit [here](http://www.ons.gov.uk/ons/guide-method/geography/products/area-classifications/ns-area-classifications/ns-2011-area-classifications/index.html) or [here](http://www.opengeodemographics.com) for further information regarding the 2011 OAC.

<blockquote>
<div>- <strong>Pakistani Communities</strong>:

- <strong>Agricultural Communities</strong>:

- <strong>Rural Employment and Retirees</strong>:

- <strong>Rural White-Collar Workers</strong>:

- <strong>Professional Service Cosmopolitans</strong>:

- <strong>Established Tech Workers</strong>:

- <strong>Eastern European Communities</strong>:

- <strong>Bangladeshi Mixed Employment</strong>:

- <strong>Achieving Minorities</strong>:

- <strong>Multi-Ethnic Professionals with Families</strong>:

- <strong>White Suburban Communities</strong>:

- <strong>Families in Terraces and Flats</strong>:

- <strong>Challenged Transitionaries</strong>:

- <strong>Industrious Transitions</strong>:

- <strong>EU White-Collar Workers</strong>:

- <strong>Deprived Blue-Collar Terraces</strong>:

- <strong>Highly-Qualified Quaternary Workers</strong>:

- <strong>Social Renting Young Families</strong>:

- <strong>Constrained Commuters</strong>:

- <strong>Indian Tech Achievers</strong>:

- <strong>Comfortable Suburbia</strong>:

- <strong>Older Farming Communities</strong>:

- <strong>Transitional Eastern European Neighbourhoods</strong>:

- <strong>Old EU Tech Workers</strong>:

- <strong>New EU Tech Workers</strong>:

- <strong>Student Digs</strong>:

- <strong>Hard-Pressed Ethnic Mix</strong>:

- <strong>Endeavouring Flat Dwellers</strong>:

- <strong>Renting Hard-Pressed Workers</strong>:

- <strong>Inner City Ethnic Mix</strong>:

- <strong>Detached Rural Retirement</strong>:

- <strong>Delayed Retirement</strong>:

- <strong>Hampered Aspiration</strong>:

- <strong>Students and Commuters</strong>:

- <strong>Young Families and Students</strong>:

- <strong>Older Workers and Retirement</strong>:

- <strong>Renting Rural Retirement</strong>:

- <strong>Multicultural Student Neighbourhoods</strong>:

- <strong>Detached Retirement Living</strong>:

- <strong>Multicultural New Arrivals</strong>:

- <strong>Outer City Hardship</strong>:

- <strong>Retired Communal City Dwellers</strong>:

- <strong>Rural Workers and Families</strong>:

- <strong>Students and Professionals</strong>:

- <strong>Established Farming Communities</strong>:

- <strong>Semi-Detached Ageing</strong>:

- <strong>Ageing in Suburbia</strong>:

- <strong>Migrant Families</strong>:

- <strong>Deprived Neighbourhoods</strong>:

- <strong>Ageing Industrious Workers</strong>:

- <strong>Urban Cultural Mix</strong>:

- <strong>Retired Independent City Dwellers</strong>:

- <strong>Retired City Hardship</strong>:

- <strong>Ageing Rural Flat Tenants</strong>:

- <strong>Student Communal Living</strong>:

- <strong>Migrant Commuters</strong>:

- <strong>Commuters with Young Families</strong>:

- <strong>Asian Terraces and Flats</strong>:

- <strong>Multi-Ethnic Suburbia</strong>:

- <strong>Rural Life</strong>:

- <strong>Young Hard-Pressed Families</strong>:

- <strong>Private Renting New Arrivals</strong>:

- <strong>Hard-Pressed European Settlers</strong>:

- <strong>Ageing Communities and Families</strong>:

- <strong>Constrained Young Families</strong>:

- <strong>Established Renting Families</strong>:

- <strong>Communal Retirement</strong>:

- <strong>Self-Sufficient Retirement</strong>:

- <strong>Multi-Ethnic Professional Service Workers</strong>:

- <strong>Constrained Neighbourhoods</strong>:

- <strong>White Professionals</strong>:

- <strong>Ageing Rural Industry Workers</strong>:

- <strong>Multi-Ethnic Hardship</strong>:

- <strong>Striving Service Workers</strong>:

- <strong>Industrious Hardship</strong>:

- <strong>Hard-Pressed Rented Terraces</strong>:

</div></blockquote>
Obtain category of &quot;Subgroup Area Classification&quot; at one point:

    UPDATE {table_name}
      SET {new_text_column} =
        OBS_GetCategory(
          CDB_LatLng(40.7, -73.9),
          'uk.cdrc.subgrp'
        );


## <a name="supergroup-area-classification"></a><a name="uk-cdrc-sprgrp"></a>Supergroup Area Classification

The 2011 Area Classification for Output Areas (2011 OAC) is a UK geodemographic classification produced as a collaboration between the Office for National Statistics and University College London. Visit [here](http://www.ons.gov.uk/ons/guide-method/geography/products/area-classifications/ns-area-classifications/ns-2011-area-classifications/index.html) or [here](http://www.opengeodemographics.com) for further information regarding the 2011 OAC.

<blockquote>
<div>- <strong>Hard-Pressed Living</strong>:

- <strong>Cosmopolitans</strong>:

- <strong>Urbanites</strong>:

- <strong>Rural Residents</strong>:

- <strong>Suburbanites</strong>:

- <strong>Constrained City Dwellers</strong>:

- <strong>Multicultural Metropolitans</strong>:

- <strong>Ethnicity Central</strong>:

</div></blockquote>
Obtain category of &quot;Supergroup Area Classification&quot; at one point:

    UPDATE {table_name}
      SET {new_text_column} =
        OBS_GetCategory(
          CDB_LatLng(40.7, -73.9),
          'uk.cdrc.sprgrp'
        );


