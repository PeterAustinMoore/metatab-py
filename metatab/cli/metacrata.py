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

    parser.add_argument('metatabfile', nargs='?', default=DEFAULT_METATAB_FILE,
                        help='Path to a Metatab file, or an s3 link to a bucket with Metatab files. ')

    class MetapackCliMemo(object):
        def __init__(self, args):
            self.cwd = getcwd()
            self.args = args
            self.cache = get_cache('metapack')

            self.set_mt_arg(args.metatabfile)

        def set_mt_arg(self, metatabfile):

            self.mt_file = self.args.metatabfile
            if not self.mt_file:
                err("Metatab file required")

            self.app_token = self.args.api or getenv('SOCRATA_APP_TOKEN')
            if not self.app_token:
                err("Set the --api option SOCRATA_API_KEY env var with the API key to a Socrata instance")

            self.socrata_url = self.args.socrata or getenv('SOCRATA_URL')
            if not self.socrata_url:
                err("Set the --socrata option or the SOCRATA_URL env var to set the URL of a Socrata instance")

            self.username = self.args.username
            if not self.username:
                err("Set the -u/--username option")

            self.password = self.args.password
            if not self.password:
                err("Set the -p/--password option")

        def update_mt_arg(self, metatabfile):
            """Return a new memo with a new metatabfile argument"""
            o = MetapackCliMemo(self.args)
            o.set_mt_arg(metatabfile)
            return o
    m = MetapackCliMemo(parser.parse_args(sys.argv[1:]))
    publish_to_socrata(m)
    exit(0)

def publish_to_socrata(m):
    from sodapy import Socrata
    client = Socrata(m.socrata_url, m.app_token, username=m.username, password=m.password)
    # 1. Get Metadata from Metatab
    try:
        doc = MetatabDoc(m.mt_file)
    except (IOError, MetatabError) as e:
        err("Failed to open metatab '{}': {}".format(m.mt_file, e))
    # Get all datafile resources:
    # If there are no resources:
    if len(doc.find("root.datafile")) == 0:
        create_parent_dataset(doc, client)
    # Otherwise Process Resources
    else:
        children = create_socrata_resources(doc, client)
        create_parent_dataset(doc, client, children=children)


def create_parent_dataset(doc, client, children=None):
    '''
    Creates the parent dataset to which child datasets
    can be attributed and linked, while retaining the independence
    of the child datasets as separate assets
    '''
    # Dataset Information
    title = doc.find_first_value('Root.Title')
    prt("Creating package parent: {}".format(title))
    description = doc.find_first_value('Root.Description')
    organization = doc.find_first_value("root.creator")
    # Metadata Fields
    metadata = {"renderTypeConfig": {"visible":{"href":"true"}},
                "accessPoints":{"URL":"http://"+doc.find_first_value("Root.Name").replace("-","/")},
                "availableDisplayTypes":["href"],
                "jsonQuery":{}
                }
    displayType = "href"
    displayFormat = {}
    query = {}
    columns = [{"name":"test","dataTypeName":"text"}]

    # Data Dictionary
    if doc.find_first_value("root.documentation"):
        metadata['additionalAccessPoints'] = [{
            "urls":metadata["accessPoints"],
            "describedBy":doc.find_first_value("root.documentation"),
            # TODO: get the actual file type
            "describedByType":"url"
            }]
    # Children Datasets
    if children:
        for child in children:
            new_child = {
                "urls":{"API":child['api'],"URL":child['source'],doc.find_first_value('Root.Format'):child["link"]},
                "title":child['title'],
                }
            metadata['additionalAccessPoints'].append(new_child)
    dataset_id = client.create(
        title,
        description=description,
        metadata=metadata,
        displayType=displayType,
        displayFormat=displayFormat,
        attribution=organization,
        query=query
        )
    parent_dataset = "https://{0}/d/{1}".format(client.domain,dataset_id['id'])
    prt("Parent dataset {} created at {}".format(title,parent_dataset))
    return parent_dataset

def create_socrata_resources(doc, client, parent=None):
    '''
    Creates separate Socrata assets with columnar schema and parent
    metadata
    @return: a dictionary of child url and titles
    '''
    new_datasets_raw = doc.find("root.datafile")
    prt("Creating {} new datasets".format(len(new_datasets_raw)))
    new_datasets = []
    for dataset in new_datasets_raw:
        new_dataset = dict()
        # Dataset creation requires:
        # title
        # Title of the Dataset
        title = dataset.get_value("title")
        prt("Gathering metadata for {}".format(title))
        new_dataset.update({"name":title})
        # Description
        description = doc.find_first_value('Root.Description')
        new_dataset.update({"description":description})
        # Tags
        tags = ["t"]
        new_dataset.update({"tags":tags})
        # Category
        category = "category"
        new_dataset.update({"category":category})
        # Columns
        columns = get_columns(doc, dataset.get_value("schema"))
        new_dataset.update({"columns":columns})
        # Source Link
        source = dataset.get_value("url")
        new_dataset.update({"attributionLink":source})
        # Source Organization
        organization = doc.find_first_value("root.creator")
        new_dataset.update({"attribution":organization})
        new_datasets.append(new_dataset)
    children = publish(new_datasets, client)
    return children

def get_columns(doc, schema):
    # Get the right table name
    # TODO: There MUST be a better way to do this
    for meta_schema in doc.get_section("schema"):
        if schema == meta_schema.value:
            table = meta_schema.term
    column_metadata_raw = doc.find(table+".column")
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
    '''
    if(datatype == "str"):
        return "text"
    elif(datatype in ["int","float"]):
        return "number"
    elif(datatype == "bool"):
        return "text"
    else:
        return "text"

def publish(new_datasets, client):
    '''
    Using sodapy, the child assets are uploaded as metadata only assets
    @return: array of child urls
    '''
    children = []
    for new_dataset in new_datasets:
        prt("Publishing {}".format(new_dataset["name"]))
        dataset_id = client.create(
            new_dataset['name'],
            description=new_dataset['description'],
            columns=new_dataset['columns'],
            tags=new_dataset['tags'],
            category=new_dataset['category'],
            attribution=new_dataset['attribution'],
            attributionLink=new_dataset['attributionLink']
            )
        child = dict(
            api="https://{}/resource/{}.json".format(client.domain, dataset_id['id']),
            source="https://{}/d/{}".format(client.domain, dataset_id['id']),
            link=new_dataset['attributionLink'],
            title=new_dataset['name']
        )
        prt("{} published to {}".format(child['title'],child['source']))
        children.append(child)
    return children
