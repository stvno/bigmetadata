from tasks.us.census.tiger import GeoidColumns
from tasks.meta import OBSColumn, current_session
from tasks.us.census.tiger import DownloadTiger
from tasks.util import (classpath, shell, TempTableTask, TableTask,
                        ColumnsTask, remove_accents)
from tasks.tags import SectionTags, SubsectionTags, UnitTags

from collections import OrderedDict
from luigi import Task, LocalTarget, Parameter, WrapperTask
from xlrd import open_workbook
from string import maketrans

import os

class DownloadLifeObesityActivity(Task):

    URL = 'http://www.healthdata.org/sites/default/files/files/data_for_download/alcohol_life_expect/IHME_county_data_LifeExpectancy_Obesity_PhysicalActivity_NATIONAL.xlsx'

    def run(self):
        self.output().makedirs()
        shell('wget -O "{output}" "{url}"'.format(
            url=self.URL, output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class LifeObesityActivityColumns(ColumnsTask):

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'units': UnitTags(),
        }

    def columns(self):
        input_ = self.input()
        usa = input_['sections']['united_states']
        health = input_['subsections']['health']
        ratio = input_['units']['ratio']
        years = input_['units']['years']

        return OrderedDict([
            ('male_life_expectancy', OBSColumn(
                name='Male life expectancy',
                type='Numeric',
                description='',
                aggregate='',
                weight=8,
                tags=[usa, health, years])),
            ('female_life_expectancy', OBSColumn(
                name='Female life expectancy',
                type='Numeric',
                description='',
                aggregate='',
                weight=8,
                tags=[usa, health, years])),
            ('male_physical_activity', OBSColumn(
                name='Male sufficient physical activity prevalence',
                type='Numeric',
                description='',
                aggregate='',
                weight=4,
                tags=[usa, health, ratio])),
            ('female_physical_activity', OBSColumn(
                name='Female sufficient physical activity prevalence',
                type='Numeric',
                description='',
                aggregate='',
                weight=4,
                tags=[usa, health, ratio])),
            ('male_obesity', OBSColumn(
                name='Male obesity prevalence',
                type='Numeric',
                description='',
                aggregate='',
                weight=4,
                tags=[usa, health, ratio])),
            ('female_obesity', OBSColumn(
                name='Female obesity prevalence',
                type='Numeric',
                description='',
                aggregate='',
                weight=4,
                tags=[usa, health, ratio])),
        ])


class LifeObesityActivity(TableTask):

    year = Parameter()

    def timespan(self):
        return self.year

    def requires(self):
        return {
            'columns': LifeObesityActivityColumns(),
            'data': DownloadLifeObesityActivity(),
            'geomref': GeoidColumns(),
            'tiger': DownloadTiger(year=2014),
        }

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['geoid'] = input_['geomref']['county_geoid']
        cols.update(input_['columns'])
        return cols

    def populate(self):
        book = open_workbook(self.input()['data'].path)

        columns = self.columns()
        headers = dict()
        sheets = book.sheets()

        colnum2name = OrderedDict()
        for sheetnum, sheet in enumerate(sheets):
            headers.update(dict([
                (cell.value, (sheetnum, cellnum))
                for cellnum, cell in enumerate(sheet.row(0))
            ]))
        for out_colname, coltarget in columns.iteritems():
            col = coltarget._column
            colname = col.name.lower().replace(' ', '')
            for header_name, sheetnum_cellnum in headers.iteritems():
                if ', {year}'.format(year=self.year) not in header_name:
                    continue
                if header_name.lower().replace(' ', '').startswith(colname):
                    colnum2name[sheetnum_cellnum] = out_colname
                    break

        session = current_session()
        resp = session.execute(
            "SELECT s.name state_name, REPLACE(c.name, '''', '') county_name, c.geoid "
            'FROM tiger2014.county c, '
            '     tiger2014.state s '
            'WHERE c.statefp = s.statefp')
        geoid_lookup = dict([
            ((state_name.lower(), remove_accents(county_name.lower())), geoid)
            for state_name, county_name, geoid in resp.fetchall()
        ])
        for i in xrange(1, sheets[0].nrows):
            state_name = sheets[0].row(i)[0].value.lower()
            county_name = sheets[0].row(i)[1].value.lower()
            if not county_name:
                continue

            # weird data error in Alaska
            county_name = county_name.replace(' city and', '')

            if county_name.endswith(' city'):
                if county_name not in ('carson city', 'charles city', 'james city'):
                    county_name = county_name.replace(' city', '')

            # data error in Montana
            county_name = county_name.replace(' county and yellowstone national park', '')

            # LaSalle in Illinois is one word
            if state_name == 'illinois' and county_name == 'la salle':
                county_name = 'lasalle'

            # LaSalle in Illinois is one word
            if state_name == 'louisiana' and county_name == 'la salle':
                county_name = 'lasalle'

            # Ste not St for Ste Genevieve
            if state_name == 'missouri' and county_name == "st. genevieve":
                county_name = "ste. genevieve"

            # ...sigh...
            if state_name == 'virginia' and county_name == "halifax county with south boston":
                county_name = "halifax"

            geoid = geoid_lookup[(state_name, county_name)]
            values = [u"'" + geoid + u"'"]
            values.extend([
                str(sheets[sheetnum].row(i)[colnum].value)
                for sheetnum, colnum in colnum2name.keys()
            ])
            colnames = [columns.keys()[0]]
            colnames.extend(colnum2name.values())
            stmt = 'INSERT INTO {output} ({colnames}) ' \
                    'VALUES ({values})'.format(
                        output=self.output().table,
                        colnames=', '.join(colnames),
                        values=', '.join(values),
                    )
            session.execute(stmt)


class AllLifeObesityActivity(WrapperTask):

    def requires(self):
        for year in (1985, 1990, 1995, 2000, 2001, 2005, 2009, 2010, 2011, ):
            yield LifeObesityActivity(year=year)
