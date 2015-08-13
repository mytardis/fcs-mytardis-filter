# -*- coding: utf-8 -*-
#
# Copyright (c) 2010-2011, Monash e-Research Centre
#   (Monash University, Australia)
# Copyright (c) 2010-2011, VeRSI Consortium
#   (Victorian eResearch Strategic Initiative, Australia)
# All rights reserved.
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    *  Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#    *  Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#    *  Neither the name of the VeRSI, the VeRSI Consortium members, nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

"""
fcs.py

.. moduleauthor:: James Wettenhall <james.wettenhall@monash.edu>

"""
import logging

from django.conf import settings
from django.core.cache import caches

from tardis.tardis_portal.models import Schema, DatafileParameterSet
from tardis.tardis_portal.models import ParameterName, DatafileParameter
from tardis.tardis_portal.models import DataFile, DataFileObject
import subprocess
import re
import os
import traceback
import urlparse
from celery.task import task

logger = logging.getLogger(__name__)

LOCK_EXPIRE = 60 * 5  # Lock expires in 5 minutes


@task(name="tardis_portal.filters.fcs.fcsplot",
      ignore_result=True)
def run_fcsplot(fcsplot_path, inputfilename, df_id, schema_id):
    """
    Run fcsplot on a FCS file.
    """
    cache = caches['celery-locks']

    # Locking functions to ensure only one instance of
    # fcsplot operates on each datafile at a time.
    lock_id = 'fcs-filter-fcsplot-lock-%d' % df_id

    # cache.add fails if if the key already exists
    def acquire_lock(): return cache.add(lock_id, 'true', LOCK_EXPIRE)
    # cache.delete() can be slow, but we have to use it
    # to take advantage of using add() for atomic locking
    def release_lock(): cache.delete(lock_id)

    if acquire_lock():
        try:
            schema = Schema.objects.get(id=schema_id)
            datafile = DataFile.objects.get(id=df_id)
            ps = DatafileParameterSet.objects.filter(schema=schema,
                                                     datafile=datafile).first()
            if ps:
                prev_param = ParameterName.objects.get(schema__id=schema_id,
                                                       name='previewImage')
                if DatafileParameter.objects.filter(parameterset=ps,
                                                    name=prev_param).exists():
                    logger.info("FCS preview already exists for df_id %d"
                                % df_id)
                    return

            outputextension = "png"
            dfo = DataFileObject.objects.filter(datafile__id=df_id,
                                                verified=True).first()
            preview_image_rel_file_path = os.path.join(
                os.path.dirname(urlparse.urlparse(dfo.uri).path),
                str(df_id),
                '%s.%s' % (os.path.basename(inputfilename),
                           outputextension))
            preview_image_file_path = os.path.join(
                settings.METADATA_STORE_PATH, preview_image_rel_file_path)

            if not os.path.exists(os.path.dirname(preview_image_file_path)):
                os.makedirs(os.path.dirname(preview_image_file_path))

            cmdline = "'%s' '%s' '%s'" % \
                (fcsplot_path, inputfilename, preview_image_file_path)
            logger.info(cmdline)
            p = subprocess.Popen(cmdline, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, shell=True)
            stdout, _ = p.communicate()
            if p.returncode != 0:
                logger.error(stdout)
                return
            try:
                ps = DatafileParameterSet.objects.get(schema__id=schema_id,
                                                      datafile__id=df_id)
            except DatafileParameterSet.DoesNotExist:
                ps = DatafileParameterSet(schema=schema,
                                          datafile=instance)
                ps.save()
            param_name = ParameterName.objects.get(schema__id=schema_id,
                                                   name='previewImage')
            dfp = DatafileParameter(parameterset=ps, name=param_name)
            dfp.string_value = preview_image_rel_file_path
            dfp.save()
        except:
            logger.error(traceback.format_exc())
        finally:
            release_lock()


@task(name="tardis_portal.filters.fcs.showinf", ignore_result=True)
def run_showinf(showinf_path, inputfilename, df_id, schema_id):
    """
    Run showinf on FCS file to extract metadata.
    """
    cache = caches['celery-locks']

    # Locking functions to ensure only one instance of
    # showinf operates on each datafile at a time.
    lock_id = 'fcs-filter-showinf-lock-%d' % df_id

    # cache.add fails if if the key already exists
    def acquire_lock(): return cache.add(lock_id, 'true', LOCK_EXPIRE)
    # cache.delete() can be slow, but we have to use it
    # to take advantage of using add() for atomic locking
    def release_lock(): cache.delete(lock_id)

    if acquire_lock():
        try:
            schema = Schema.objects.get(id=schema_id)
            datafile = DataFile.objects.get(id=df_id)
            ps = DatafileParameterSet.objects.filter(schema=schema,
                                                     datafile=datafile).first()
            if ps:
                file_param = ParameterName.objects.get(schema__id=schema_id,
                                                       name='file')
                if DatafileParameter.objects.filter(parameterset=ps,
                                                    name=file_param).exists():
                    logger.info("FCS metadata already exists for df_id %d"
                                % df_id)
                    return

            cmdline = "'%s' '%s'" % (showinf_path, inputfilename)
            logger.info(cmdline)
            p = subprocess.Popen(cmdline, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, shell=True)
            stdout, _ = p.communicate()
            if p.returncode != 0:
                logger.error(stdout)
                return
            image_info_list = stdout.split('\n')

            metadata = {
                'file': "",
                'date': "",
                'time': "",
                'cytometer': "",
                'application': "",
                'numberOfCells': "",
                'parametersAndStainsTable': ""}

            readingParametersAndStainsTable = False

            for line in image_info_list:
                m = re.match("File: (.*)", line)
                if m:
                    metadata['file'] = m.group(1)
                m = re.match("Date: (.*)", line)
                if m:
                    metadata['date'] = m.group(1)
                m = re.match("Time: (.*)", line)
                if m:
                    metadata['time'] = m.group(1)
                m = re.match("Cytometer: (.*)", line)
                if m:
                    metadata['cytometer'] = m.group(1)
                m = re.match("Application: (.*)", line)
                if m:
                    metadata['application'] = m.group(1)
                m = re.match("# Cells: (.*)", line)
                if m:
                    numberOfCells = m.group(1)
                    try:
                        metadata['numberOfCells'] = \
                            "{:,d}".format(int(numberOfCells))
                    except ValueError:
                        metadata['numberOfCells'] = ""
                if line.strip() == "<ParametersAndStains>":
                    readingParametersAndStainsTable = True
                elif line.strip() == "</ParametersAndStains>":
                    readingParametersAndStainsTable = False
                elif readingParametersAndStainsTable:
                    metadata['parametersAndStainsTable'] += line

            try:
                ps = DatafileParameterSet.objects.get(schema__id=schema_id,
                                                      datafile__id=df_id)
            except DatafileParameterSet.DoesNotExist:
                ps = DatafileParameterSet(schema=schema, datafile=datafile)
                ps.save()

            param_name_strings = ['file', 'date', 'time', 'cytometer',
                                  'application', 'numberOfCells',
                                  'parametersAndStainsTable']
            for param_name_str in param_name_strings:
                param_name = ParameterName.objects.get(schema__id=schema_id,
                                                       name=param_name_str)
                dfp = DatafileParameter(parameterset=ps, name=param_name)
                dfp.string_value = metadata[param_name_str]
                dfp.save()
        except:
            logger.error(traceback.format_exc())
        finally:
            release_lock()


class FcsImageFilter(object):
    """This filter uses the Bioconductor flowCore and flowViz
    packages to extract metadata and plot preview images for
    FCS data files.

    :param name: the short name of the schema.
    :type name: string
    :param schema: the name of the schema
    :type schema: string
    :param queue: the name of the Celery queue to run tasks in.
    :type queue: string
    """
    def __init__(self, name, schema, fcsplot_path, showinf_path,
                 queue=None):
        self.name = name
        self.schema = schema
        self.fcsplot_path = fcsplot_path
        self.showinf_path = showinf_path
        self.queue = queue

    def __call__(self, sender, **kwargs):
        """post save callback entry point.

        :param sender: The model class.
        :param instance: The actual instance being saved.
        :param created: A boolean; True if a new record was created.
        :type created: bool
        """
        instance = kwargs.get('instance')
        schema = Schema.objects.get(namespace__exact=self.schema)

        if not instance.filename.lower().endswith('.fcs'):
            return None

        if DatafileParameterSet.objects.filter(schema=schema,
                                               datafile=instance).exists():
            ps = DatafileParameterSet.objects.get(schema=schema,
                                                  datafile=instance)
            logger.warning("Parameter set already exists for %s, "
                           "so we'll just return it." % instance.filename)
            return ps

        logger.info("Applying FCS filter to '%s'..." % instance.filename)

        # Instead of checking out to a tmpdir, we'll use dfo.get_full_path().
        # This won't work for object storage, but that's OK for now...
        dfo = DataFileObject.objects.filter(datafile=instance,
                                            verified=True).first()
        filepath = dfo.get_full_path()
        logger.info("filepath = '" + filepath + "'")

        try:
            kwargs = {'queue': self.queue} if self.queue else {}
            run_fcsplot.apply_async(args=[self.fcsplot_path,
                                          filepath,
                                          instance.id,
                                          schema.id],
                                    **kwargs)

            kwargs = {'queue': self.queue} if self.queue else {}
            run_showinf.apply_async(args=[self.showinf_path, filepath,
                                          instance.id, schema.id],
                                    **kwargs)
        except Exception, e:
            logger.error(str(e))
            return None


def make_filter(name='', schema='',
                fcsplot_path=None, showinf_path=None,
                queue=None):
    if not name:
        raise ValueError("FcsImageFilter "
                         "requires a name to be specified")
    if not schema:
        raise ValueError("FcsImageFilter "
                         "requires a schema to be specified")
    if not fcsplot_path:
        raise ValueError("FcsImageFilter "
                         "requires an fcsplot path to be specified")
    if not showinf_path:
        raise ValueError("FcsImageFilter "
                         "requires a showinf path to be specified")
    return FcsImageFilter(name, schema,
                          fcsplot_path, showinf_path,
                          queue)

make_filter.__doc__ = FcsImageFilter.__doc__
