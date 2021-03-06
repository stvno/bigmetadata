'''
Sphinx functions for luigi bigmetadata tasks.
'''

import os
import re

from jinja2 import Environment, PackageLoader
from luigi import (WrapperTask, Task, LocalTarget, BooleanParameter, Parameter,
                   DateParameter)
from luigi.s3 import S3Target
from tasks.util import shell
from tasks.meta import current_session, OBSTag
from tasks.carto import GenerateStaticImage, ImagesForMeasure, GenerateThumb

from datetime import date

ENV = Environment(loader=PackageLoader('catalog', 'templates'))

def strip_tag_id(tag_id):
    '''
    Strip leading `tags.` when it exists.
    '''
    return tag_id.replace('tags.', '')

ENV.filters['strip_tag_id'] = strip_tag_id

SECTION_TEMPLATE = ENV.get_template('section.html')
SUBSECTION_TEMPLATE = ENV.get_template('subsection.html')



class GenerateRST(Task):

    force = BooleanParameter(default=False)
    format = Parameter()
    preview = BooleanParameter(default=False)
    images = BooleanParameter(default=True)

    def __init__(self, *args, **kwargs):
        super(GenerateRST, self).__init__(*args, **kwargs)
        if self.force:
            shell('rm -rf catalog/source/*/*')
        shell('cp -R catalog/img catalog/source/')
        shell('mkdir -p catalog/img_thumb')
        shell('cp -R catalog/img_thumb catalog/source/')

    def requires(self):
        session = current_session()
        requirements = {}
        for section_subsection, _ in self.output().iteritems():
            section_id, subsection_id = section_subsection
            subsection = session.query(OBSTag).get(subsection_id)
            if self.images:
                if '.. cartofigure:: ' in subsection.description:
                    viz_id = re.search(r'\.\. cartofigure:: (\S+)', subsection.description).groups()[0]
                    if self.format == 'pdf':
                        img = GenerateThumb(viz=viz_id)
                    else:
                        img = GenerateStaticImage(viz=viz_id)
                    requirements[viz_id] = img
                for column in subsection.columns:
                    if column.type.lower() == 'numeric' and column.weight > 0 and not column.id.startswith('uk'):
                        if self.format == 'pdf':
                            img = GenerateThumb(measure=column.id, force=False)
                        else:
                            img = ImagesForMeasure(measure=column.id, force=False)
                        requirements[column.id] = img

        return requirements

    def output(self):
        targets = {}
        session = current_session()
        i = 0
        for section in session.query(OBSTag).filter(OBSTag.type == 'section'):
            for subsection in session.query(OBSTag).filter(OBSTag.type == 'subsection'):
                i += 1
                if i > 1 and self.preview:
                    break
                targets[(section.id, subsection.id)] = LocalTarget(
                    'catalog/source/{section}/{subsection}.rst'.format(
                        section=strip_tag_id(section.id),
                        subsection=strip_tag_id(subsection.id)))
        return targets

    def template_globals(self):
        image_path = '../img_thumb' if self.format == 'pdf' else '../img'
        return {
            'IMAGE_PATH': image_path
        }

    def run(self):
        session = current_session()
        for section_subsection, target in self.output().iteritems():
            section_id, subsection_id = section_subsection
            section = session.query(OBSTag).get(section_id)
            subsection = session.query(OBSTag).get(subsection_id)
            target.makedirs()
            fhandle = target.open('w')

            if '.. cartofigure:: ' in subsection.description:
                viz_id = re.search(r'\.\. cartofigure:: (\S+)', subsection.description).groups()[0]
                viz_path = os.path.join('../', *self.input()[viz_id].path.split(os.path.sep)[2:])
                subsection.description = re.sub(r'\.\. cartofigure:: (\S+)',
                                                '.. figure:: {}'.format(viz_path),
                                                subsection.description)
            columns = []
            for col in subsection.columns:
                if section not in col.tags:
                    continue

                if col.weight < 1:
                    continue

                # tags with denominators will appear beneath that denominator
                if not col.has_denominators():
                    columns.append(col)

                # unless the denominator is not in this subsection
                else:
                    add_to_columns = True
                    for denominator in col.denominators():
                        if subsection in denominator.tags:
                            add_to_columns = False
                            break
                    if add_to_columns:
                        columns.append(col)

            columns.sort(lambda x, y: cmp(x.name, y.name))

            with open('catalog/source/{}.rst'.format(strip_tag_id(section.id)), 'w') as section_fhandle:
                section_fhandle.write(SECTION_TEMPLATE.render(
                    section=section, **self.template_globals()))
            if columns:
                fhandle.write(SUBSECTION_TEMPLATE.render(
                    subsection=subsection, columns=columns, format=self.format,
                    **self.template_globals()
                ).encode('utf8'))
            else:
                fhandle.write('')
            fhandle.close()


class Catalog(Task):

    force = BooleanParameter(default=False)
    format = Parameter(default='html')
    preview = BooleanParameter(default=False)
    images = BooleanParameter(default=True)

    def requires(self):
        return GenerateRST(force=self.force, format=self.format, preview=self.preview,
                           images=self.images)

    def complete(self):
        return False

    def run(self):
        shell('cd catalog && make {}'.format(self.format))


class PDFCatalogToS3(Task):

    timestamp = DateParameter(default=date.today())
    force = BooleanParameter(significant=False)

    def __init__(self, **kwargs):
        if kwargs.get('force'):
            try:
                shell('aws s3 rm s3://data-observatory/observatory.pdf')
            except:
                pass
        super(PDFCatalogToS3, self).__init__()

    def run(self):
        for target in self.output():
            shell('aws s3 cp catalog/build/observatory.pdf {output} '
                  '--acl public-read'.format(
                      output=target.path
                  ))

    def output(self):
        return [
            S3Target('s3://data-observatory/observatory.pdf'),
            S3Target('s3://data-observatory/observatory-{timestamp}.pdf'.format(
                timestamp=self.timestamp
            )),
        ]
