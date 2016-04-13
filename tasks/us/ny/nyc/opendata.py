from luigi import Task, Parameter, LocalTarget, WrapperTask
from tasks.util import shell, classpath, ColumnsTask, TableTask
from tasks.meta import OBSColumn, current_session

from collections import OrderedDict

import os


class Download(Task):
    '''
    results of download will need to be gunzipped
    '''

    dataset_id = Parameter()

    def url(self):
        return 'http://www.opendatacache.com/data.cityofnewyork.us/api/views/{id}/rows.csv'.format(id=self.dataset_id)

    def run(self):
        self.output().makedirs()
        shell('wget "{url}" -O "{output}"'.format(url=self.url(),
                                                  output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.dataset_id))


class ACRISColumns(ColumnsTask):

    def columns(self):
        return OrderedDict([
            ('document_id', OBSColumn(
                type='Text',
                name='Document ID'
            )),
            ('good_through_date', OBSColumn(
                type='Date',
                name='Good Through Date'
            )),
            ('record_type', OBSColumn(
                type='Text',
                name='Record Type'
            ))
        ])


class ACRISPartiesColumns(ColumnsTask):
    # DOCUMENT ID,RECORD TYPE,PARTY TYPE,NAME,ADDRESS 1,ADDRESS 2,COUNTRY,CITY,STATE,ZIP,GOOD THROUGH DATE
    # 2016020301344003,P,1,"BENYAMINOV, ANATOLIY",105-36 62 DRIVE,,US,FOREST HILLS,NY,11375,02/29/2016

    def requires(self):
        return ACRISColumns()

    def version(self):
        return 4

    def columns(self):
        session = current_session()
        return OrderedDict([
            ('document_id', self.input()['document_id']._column),
            ('record_type', self.input()['record_type']._column),
            ('party_type', OBSColumn(
                type='Integer',
                name='Party Type'
            )),
            ('name', OBSColumn(
                type='Text',
                name='Name'
            )),
            ('address1', OBSColumn(
                type='Text',
                name='Address 1'
            )),
            ('address2', OBSColumn(
                type='Text',
                name='Address 2'
            )),
            ('country', OBSColumn(
                type='Text',
                name='country'
            )),
            ('city', OBSColumn(
                type='Text',
                name='city'
            )),
            ('state', OBSColumn(
                type='Text',
                name='state'
            )),
            ('zip', OBSColumn(
                type='Text',
                name='zip'
            )),
            ('good_through_date', self.input()['good_through_date']._column)
        ])



class ACRISMasterColumns(ColumnsTask):

    # DOCUMENT ID,RECORD TYPE,CRFN,BOROUGH,DOC. TYPE,DOC. DATE,DOC. AMOUNT,RECORDED / FILED,MODIFIED DATE,REEL YEAR,REEL NBR,REEL PAGE,% TRANSFERRED,GOOD THROUGH DATE
    # 2016020500925001,A,2016000048837,1,MTGE,01/25/2016,400000,02/12/2016,02/12/2016,0,0,0,0,02/29/2016

    def requires(self):
        return ACRISColumns()

    def columns(self):
        session = current_session()
        return OrderedDict([
            ('document_id', self.input()['document_id']._column),
            ('record_type', self.input()['record_type']._column),
            ('crfn', OBSColumn(
                type='Text',
                name='City Reel File Number'
            )),
            ('borough', OBSColumn(
                type='Text',
                name='borough'
            )),
            ('doc_type', OBSColumn(
                type='Text',
                name='Document Type'
            )),
            ('doc_date', OBSColumn(
                type='Text', # correct to DATE but 0200 year
                name='Document Date'
            )),
            ('doc_amt', OBSColumn(
                type='Text', # TODO "Money"
                name='Document Amount'
            )),
            ('recorded_filed', OBSColumn(
                type='Text', # correct to DATE but 0200 year
                name='Recorded / Filed'
            )),
            ('modified_date', OBSColumn(
                type='Date',
                name='Modified Date'
            )),
            ('reel_year', OBSColumn(
                type='Integer',
                name='Reel Year'
            )),
            ('reel_nbr', OBSColumn(
                type='Text',
                name='Reel Number'
            )),
            ('reel_page', OBSColumn(
                type='Text',
                name='Reel Pgae'
            )),
            ('percent_transferred', OBSColumn(
                type='Text',
                name='precent_transferred'
            )),
            ('good_through_date', self.input()['good_through_date']._column)
        ])


class ACRISLegalsColumns(ColumnsTask):
    # DOCUMENT ID,RECORD TYPE,BOROUGH,BLOCK,LOT,EASEMENT,PARTIAL LOT,AIR RIGHTS,SUBTERRANEAN RIGHTS,PROPERTY TYPE,STREET NUMBER,STREET NAME,UNIT,GOOD THROUGH DATE
    # 2016021600859001,L,3,164,40,N,E,N,N,F1,48,HOYT STREET,,02/29/2016

    def requires(self):
        return ACRISColumns()

    def columns(self):
        session = current_session()
        return OrderedDict([
            ('document_id', self.input()['document_id']._column),
            ('record_type', self.input()['record_type']._column),
            ('borough', OBSColumn(
                type='Integer',
                name='Borough'
            )),
            ('block', OBSColumn(
                type='Integer',
                name='Block'
            )),
            ('lot', OBSColumn(
                type='Integer',
                name='Lot'
            )),
            ('easement', OBSColumn(
                type='Text',
                name='Easement'
            )),
            ('partial_lot', OBSColumn(
                type='Text',
                name='Partial Lot'
            )),
            ('air_rights', OBSColumn(
                type='Text',
                name='Air Rights'
            )),
            ('subterranean_rights', OBSColumn(
                type='Text',
                name='Subterranean Rights'
            )),
            ('property_type', OBSColumn(
                type='Text',
                name='Property Type'
            )),
            ('street_number', OBSColumn(
                type='Text',
                name='Street Number'
            )),
            ('street_name', OBSColumn(
                type='Text',
                name='Street Name'
            )),
            ('unit', OBSColumn(
                type='Text',
                name='Unit'
            )),
            ('good_through_date', self.input()['good_through_date']._column)
        ])


class ACRISParties(TableTask):

    def requires(self):
        return {
            'data': Download(dataset_id='636b-3b5g'),
            'meta': ACRISPartiesColumns()
        }

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def timespan(self):
        return '1966 - present'

    def columns(self):
        return self.input()['meta']

    def populate(self):
        shell(r"gunzip -c {input} | grep -v '{{' | psql -c '\copy {output} FROM STDIN WITH CSV HEADER'".format(
            input=self.input()['data'].path,
            output=self.output().get(current_session()).id
        ))


class ACRISMaster(TableTask):

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def timespan(self):
        return '1966 - present'

    def requires(self):
        return {
            'data': Download(dataset_id='bnx9-e6tj'),
            'meta': ACRISMasterColumns()
        }

    def columns(self):
        return self.input()['meta']

    def populate(self):
        shell(r"gunzip -c {input} | psql -c '\copy {output} FROM STDIN WITH CSV HEADER'".format(
            input=self.input()['data'].path,
            output=self.output().get(current_session()).id
        ))


class ACRISLegals(TableTask):

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def timespan(self):
        return '1966 - present'

    def requires(self):
        return {
            'data': Download(dataset_id='8h5j-fqxa'),
            'meta': ACRISLegalsColumns()
        }

    def columns(self):
        return self.input()['meta']

    def populate(self):
        # grep -v '{' to ignore weird serve dump lines
        shell(r"gunzip -c {input} | grep -v '{{' | psql -c '\copy {output} FROM STDIN WITH CSV HEADER'".format(
            input=self.input()['data'].path,
            output=self.output().get(current_session()).id
        ))



class ACRIS(WrapperTask):

    def requires(self):
        return {
            'parties': ACRISParties(),
            'master': ACRISMaster(),
            'legals': ACRISLegals(),
        }

    #def run(self):
    #    pass

    #def output(self):
    #    pass


