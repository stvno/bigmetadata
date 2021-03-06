# http://www.scotlandscensus.gov.uk/ods-web/data-warehouse.html#bulkdatatab

from luigi import Task, Parameter, LocalTarget

from tasks.util import (TableTask, ColumnsTask, classpath, shell,
                        DownloadUnzipTask)

import os

class DownloadScotlandLocal(DownloadUnzipTask):

    URL = 'http://www.scotlandscensus.gov.uk/ods-web/download/getDownloadFile.html?downloadFileIds=Output%20Area%20blk'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(output=self.output().path,
                                                  url=self.URL))
