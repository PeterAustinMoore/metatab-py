# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""
CLI program for storing pacakges in Socrata
"""

import mimetypes
import sys
from os import getenv, getcwd
from os.path import join, basename
from copy import deepcopy
import re
import requests

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

    parser.add_argument('--sync', help="Sync defined metatab file to package ID")

    parser.add_argument('metatabfile', nargs='?', default=DEFAULT_METATAB_FILE,
                        help='Path to a Metatab file. ')

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

            self.app_token = self.args.api or getenv('SODA_APP_TOKEN')
            if not self.app_token:
                err("Set the -a/--api option SODA_APP_TOKEN env var with the API key to a Socrata instance")

            self.socrata_url = self.args.socrata or getenv('SOCRATA_URL')
            if not self.socrata_url:
                err("Set the -s/--socrata option or the SOCRATA_URL env var to set the URL of a Socrata instance")

            self.username = self.args.username or getenv("SODA_USERNAME")
            if not self.username:
                err("Set the -u/--username option or SODA_USERNAME environmental variable")

            self.password = self.args.password or getenv("SODA_PASSWORD")
            if not self.password:
                err("Set the -p/--password option or SODA_PASSWORD environmental variable")

            self.ssync = self.args.sync

        def update_mt_arg(self, metatabfile):
            """Return a new memo with a new metatabfile argument"""
            o = MetapackCliMemo(self.args)
            o.set_mt_arg(metatabfile)
            return o
    m = MetapackCliMemo(parser.parse_args(sys.argv[1:]))
    publish_to_socrata(m)
    exit(0)

def publish_to_socrata(m):
    # Initialize Socrata client from sodapy
    from sodapy import Socrata
    client = Socrata(m.socrata_url, m.app_token, username=m.username, password=m.password)
    # 1. Get Metadata from Metatab
    try:
        doc = MetatabDoc(m.mt_file)
    except (IOError, MetatabError) as e:
        err("Failed to open metatab '{}': {}".format(m.mt_file, e))

    # Catch the bad dataset IDs
    if m.ssync:
        valid, message = validate_four_by_bour(m.ssync, client)
        if not valid:
            err(message)

    # Get all datafile resources:
    # If there are no resources:
    if len(doc.find("root.datafile")) == 0:
        if m.ssync:
            create_or_update_parent(doc, client, update=True, dataset=m.ssync)
        else:
            create_or_update_parent(doc, client)
    # If there is one resource, create a single dataset
    elif len(doc.find("root.datafile")) == 1:
        if m.ssync:
            create_or_update_resources(doc, client, update=True, dataset=m.ssync)
        else:
            create_or_update_resources(doc, client)
    # If there are multiple, create the parent-child structure
    else:
        if m.ssync:
            create_or_update_resources(doc, client, update=True, dataset=m.ssync)
            create_or_update_parent(doc, client, update=True, dataset=m.ssync)
        else:
            children = create_socrata_resources(doc, client)
            create_parent_dataset(doc, client, children=children)

def validate_four_by_bour(dataset_id, client):
    r = re.compile('^[a-z0-9]{4}-[a-z0-9]{4}$')
    if r.match(dataset_id):
        try:
            # Let Sodapy catch the 4x4s that aren't real datasets
            client.get(dataset_id)
        except requests.exceptions.HTTPError:
            message = "Dataset does not exist"
            return False, message
    else:
        message = "Dataset ID: {} not valid".format(dataset_id)
        return False, message
    return True


def create_or_update_parent(doc, client, children=None, update=False, dataset=None):
    '''
    Creates the parent dataset to which child datasets
    can be attributed and linked, while retaining the independence
    of the child datasets as separate assets
    '''
    new_parent = {}
    # Dataset Information
    title = doc.find_first_value('Root.Title')
    prt("Creating package parent: {}".format(title))
    new_parent.update({"title":title})
    # Description
    description = doc.find_first_value('Root.Description')
    new_parent.update({"description":description})
    # Default Fields
    displayType = "href"
    displayFormat = {}
    query = {}
    # Tags
    tags = doc.find_first_value('Root.Tags').split(",")
    new_parent.update({"tags":tags})
    # Category
    category = doc.find_first_value('Root.Category')
    new_parent.update({"category":category})
    # Metadata Fields
    metadata = {"renderTypeConfig": {"visible":{"href":"true"}},
                "accessPoints":{"URL":"http://"+doc.find_first_value("Root.Name").replace("-","/")},
                "availableDisplayTypes":["href"],
                "jsonQuery":{}
                }
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

    # Data.json Project Open Data Metadata v1
    metadata = project_open_data(doc, metadata)
    new_parent.update({"metadata":metadata})

    if update:
        # Update the parent
        dataset_id = sync(new_parent, dataset, client)
        parent_dataset = "https://{0}/d/{1}".format(client.domain,dataset_id['id'])
        prt("Parent dataset {} updated at {}".format(title,parent_dataset))
        return parent_dataset
    else:
        # Create the parent
        dataset_id = client.create(
            title,
            description=description,
            metadata=metadata,
            tags=tags,
            category=category,
            displayType=displayType,
            displayFormat=displayFormat,
            attribution=organization,
            query=query
            )
        parent_dataset = "https://{0}/d/{1}".format(client.domain,dataset_id['id'])
        prt("Parent dataset {} created at {}".format(title,parent_dataset))
        return parent_dataset

def create_or_update_resources(doc, client, parent=None, update=False, dataset=None):
    '''
    Creates separate Socrata assets with columnar schema and parent
    metadata
    @return: a dictionary of created child url and titles or number
            of datasets updated.
    '''
    # Update the resource
    if update:
        datasets_raw = doc.find("root.datafile")
        prt("Updating {} new datasets".format(len(datasets_raw)))
        updated_datasets = 0
        for dataset in datasets_raw:
            new_dataset = get_metadata(doc, dataset)
            updated_datasets += sync(new_datasets, dataset, client)
        prt("{} datasets updated".format(updated_datasets))
        return updated_datasets
    # Create the resource
    else:
        datasets_raw = doc.find("root.datafile")
        prt("Creating {} new datasets".format(len(datasets_raw)))
        new_datasets = []
        for dataset in datasets_raw:
            new_dataset = get_metadata(doc, dataset)
            new_datasets.append(new_dataset)
        children = publish(new_datasets, client)
        return children

def get_metadata(doc, dataset):
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
    tags = doc.find_first_value('Root.Tags').split(",")
    new_dataset.update({"tags":tags})
    # Category
    category = doc.find_first_value('Root.Category')
    new_dataset.update({"category":category})
    # Columns
    columns = get_columns(doc, dataset.get_value("schema"))
    new_dataset.update({"columns":columns})
    #
    attributionLink = dataset.get_value("url")
    new_dataset.update({"attributionLink":attributionLink})
    # Common Core Metadata
    metadata = {}
    metadata = project_open_data(doc, metadata)
    new_dataset.update({"metadata":metadata})
    return new_dataset

def project_open_data(doc, metadata):
    '''
    Map the project open data v1.1 metadata guide:
    https://project-open-data.cio.gov/v1.1/schema/
    @return: metadata dictionary
    '''
    metadata_fields = ["Publisher",
        "Contact Name",
        "Contact Email",
        "Bureau Code",
        "Program Code",
        "Public Access Level",
        "Access Level Comment",
        "Geographic Coverage",
        "Temporal Applicability",
        "Theme",
        "Described By",
        "Described By Type",
        "Is Quality Data",
        "Update Frequency",
        "Language",
        "Primary It Investment Uii",
        "System of Records",
        "Homepage",
        "Issued",
        "References",
        "License"]

    metadata.update({'custom_fields':{"Common Core":{}}})
    # Check that fields are data.json compliant
    for field in doc.get_section("common_core"):
        if field.term.title() in metadata_fields:
            if field.value:
                metadata['custom_fields']['Common Core'].update({field.term.title():field.value})
    return metadata


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
        # Column Name
        name = column.value
        new_column.update({"name":name})
        # Datatype mapped from Metatab standards
        # to Socrata datatypes
        dataTypeName = map_type(column.get_value("datatype"))
        new_column.update({"dataTypeName":dataTypeName})
        # Description (if there is one)
        description = "" if len(column.get_value("valuetype")) == 0 else column.get_value("valuetype") + " - "
        description += column.get_value("description") if column.get_value("description") else ""
        new_column.update({"description":description})

        column_metadata.append(new_column)
    return column_metadata

def map_type(datatype):
    '''
    Map the metatab datatypes to Socrata types
    @default: text
    @return: Socrata datatype
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
            metadata=new_dataset['metadata']
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

def sync(new_metadata, dataset_id, client):
    metadata_raw = client.get_metadata(dataset_id)
    metadata = deepcopy(metadata_raw)
    diff_metadata = diff(metadata, new_metadata)
    prt(diff_metadata)
    response = client.update_metadata(diff_metadata)
    return response

def diff(old, new):
    return({k:v for k,v in new.items() if k not in old or v != old[k]})
