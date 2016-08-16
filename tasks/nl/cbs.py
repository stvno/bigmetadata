#https://www.cbs.nl/nl-nl/maatwerk/2015/48/kerncijfers-wijken-en-buurten-2014
#https://www.cbs.nl/nl-nl/dossier/nederland-regionaal/geografische%20data/wijk-en-buurtkaart-2014
#http://download.cbs.nl/regionale-kaarten/shape-2014-versie-2-0.zip
# Import a test runner

from tests.util import runtask
from tasks.util import (TempTableTask, TableTask, ColumnsTask,
                        DownloadUnzipTask, CSV2TempTableTask,
                        underscore_slugify, shell, classpath,
                        Carto2TempTableTask)
from tasks.meta import current_session, DENOMINATOR

# We like OrderedDict because it makes it easy to pass dicts
# like {column name : column definition, ..} where order still
# can matter in SQL
from collections import OrderedDict
from luigi import IntParameter, Parameter
import os

from tasks.meta import OBSTable, OBSColumn, OBSTag

from tasks.tags import SectionTags, SubsectionTags, UnitTags

class CopyCBS(Carto2TempTableTask):

    subdomain = 'stvno'
    table = 'gem_2014'
