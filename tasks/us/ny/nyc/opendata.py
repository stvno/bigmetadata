from luigi import Task, Parameter, LocalTarget, WrapperTask
from tasks.util import shell, classpath

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


class ACRIS(WrapperTask):

    def requires(self):
        return {
            'parties': Download(dataset_id='636b-3b5g'),
            'master': Download(dataset_id='bnx9-e6tj'),
            'legals': Download(dataset_id='8h5j-fqxa')
        }

    #def run(self):
    #    pass

    #def output(self):
    #    pass
