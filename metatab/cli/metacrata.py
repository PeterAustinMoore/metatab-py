# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""
CLI program for storing pacakges in Socrata
"""

import mimetypes
import sys
from os import getenv, getcwd
from os.path import join, basename

from metatab import _meta, DEFAULT_METATAB_FILE, resolve_package_metadata_url, MetatabDoc, open_package, MetatabError
from metatab.cli.core import err, metatab_info
from rowgenerators import get_cache, Url
from .core import prt, warn, write_doc
from .metasync import update_dist


def metacrata():
    import argparse
    parser = argparse.ArgumentParser(
        prog='metacrata',
        description='Socrata management of Metatab packages, version {}'.format(_meta.__version__),
         )

    parser.add_argument('-i', '--info', default=False, action='store_true',
                   help="Show configuration information")

    parser.add_argument('-s', '--socrata', help="URL for Socrata instance")

    parser.add_argument('-a', '--api', help="Socrata API Key")

    parser.add_argument('-u', '--username', help="Socrata username")

    parser.add_argument('-p', '--password', help="Socrata password")

    parser.add_argument('-p', '--packages', action='store_true',
                        help="The file argument is a text file with a list of package URLs to load")

    parser.add_argument('metatabfile', nargs='?', default=DEFAULT_METATAB_FILE,
                        help='Path to a Metatab file, or an s3 link to a bucket with Metatab files. ')

    class MetapackCliMemo(object):
        def __init__(self, args):
            self.cwd = getcwd()
            self.args = args
            self.cache = get_cache('metapack')

            self.set_mt_arg(args.metatabfile)

        def set_mt_arg(self, metatabfile):

            self.mtfile_arg = metatabfile if metatabfile else join(self.cwd, DEFAULT_METATAB_FILE)

            self.mtfile_url = Url(self.mtfile_arg)
            self.resource = self.mtfile_url.parts.fragment

            self.package_url, self.mt_file = resolve_package_metadata_url(self.mtfile_url.rebuild_url(False, False))

            self.app_token = self.args.api or getenv('SOCRATA_APP_TOKEN')

            self.socrata_url = self.args.socrata or getenv('SOCRATA_URL')

            if not self.socrata_url:
                err("Set the --socrata option or the SOCRATA_URL env var to set the URL of a Socrata instance")

            if not self.api_key:
                err("Set the --api option SOCRATA_API_KEY env var  with the API key to a SOCRATA instance")

        def update_mt_arg(self, metatabfile):
            """Return a new memo with a new metatabfile argument"""
            o = MetapackCliMemo(self.args)
            o.set_mt_arg(metatabfile)
            return o

    m = MetapackCliMemo(parser.parse_args(sys.argv[1:]))

    if m.args.info:
        metatab_info(m.cache)
        exit(0)

    if m.mtfile_url.scheme == 's3':
        """Find all of the top level CSV files in a bucket and use them to create Socrata entries"""

        from metatab.s3 import S3Bucket

        b = S3Bucket(m.mtfile_arg)

        for e in b.list():
            key = e['Key']
            if '/' not in key and key.endswith('.csv'):
                url = b.access_url(key)
                prt("Processing", url)
                publish_to_socrata(m.update_mt_arg(url))

    elif m.args.packages:

        with open(m.mtfile_arg) as f:
            for line in f.readlines():
                url = line.strip()
                prt("Processing", url)
                try:
                    publish_to_socrata(m.update_mt_arg(url))
                except Exception as e:
                    warn("Failed to process {}: {}".format(line, e))

    else:
        publish_to_socrata(m)

    exit(0)

def publish_to_socrata(m):
    from sodapy import Socrata
    client = Socrata(m.socrata_url, m.app_token, username=m.username, password=m.password)
    # 1. Get Metadata from Metatab
    try:
        doc = MetatabDoc(m.mt_file, cache=m.cache)
    except (IOError, MetatabError) as e:
        err("Failed to open metatab '{}': {}".format(m.mt_file, e))
    # Get all datafile resources:
    # If there are no resources:
    if len(doc.find("root.datafile")) == 0:
        create_empty_dataset(doc)
    # Otherwise Process Resources
    else:
        create_socrata_resources(doc)

def create_empty_dataset(doc):
    new_dataset = dict()
    title = doc.find_first_value('Root.Title')
    new_dataset.update({"name":title})
    description = doc.find_first_value('Root.Description')
    new_dataset.update({"description":description})
    return

def create_socrata_resources(doc):
    new_datasets_raw = doc.find("root.datafile")
    new_datasets = []
    for dataset in new_datasets:
        new_dataset = dict()
        # Dataset creation requires:
        # title
        # Title of the Dataset
        title = dataset.get_value("title")
        new_dataset.update({"name":title})
        # Description
        description = doc.find_first_value('Root.Description')
        new_dataset.update({"description":description})
        # Tags
        tags = "t"
        new_dataset.update({"tags":tags})
        # Category
        category = "category"
        new_dataset.update({"category":category})
        # Columns
        columns = get_columns(doc, dataset.get_value("schema"))
        new_dataset.update({"columns":columns})
        new_datasets.append(new_dataset)
    publish(new_datasets)
    return

def get_columns(doc, schema):
    # Get the right table name
    # TODO: There MUST be a better way to do this
    for meta_schema in doc.get_section("schema"):
        if schema == meta_schema.value:
            table = meta_schema.term
    column_metadata_raw = doc.find("table.column")
    column_metadata = []
    for column in column_metadata_raw:
        new_column = dict()
        name = column.value
        new_column.update({"name":name})
        dataTypeName = map_type(column.get_value("datatype"))
        new_column.update({"dataTypeName":dataTypeName})
        description = column.get_value("description")
        new_column.update({"description":description})
        column_metadata.append(new_column)
    return column_metadata

def map_type(datatype):
    '''
    Map the metatab datatypes to Socrata types
    Maybe we may just want to force Socrata datatypes though...
    '''
    if(datatype == "str"):
        return "text"
    elif(datatype in ["int","float"]):
        return "number"
    elif(datatype == "bool"):
        return "text"

def publish(new_datasets):
    # 2. Create a Dataset w/ metadata

def add_metadata
