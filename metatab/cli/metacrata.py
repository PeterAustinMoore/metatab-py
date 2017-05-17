# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""
CLI program for storing pacakges in Socrata
"""

import mimetypes
import sys
from os import getenv, getcwd
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

    parser.add_argument('--sync', action='store_true', help="Sync defined metatab file to package ID")

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

    # Get all datafile resources:
    # If there are no resources:
    if len(doc.find("root.datafile")) == 0:
        if m.ssync:
            create_or_update_parent(doc, client, update=True)
        else:
            create_or_update_parent(doc, client, m=m)

    # If there is one resource, create a single dataset
    elif len(doc.find("root.datafile")) == 1:
        if m.ssync:
            create_or_update_resources(doc, client, update=True)
        else:
            resources = create_or_update_resources(doc, client, m=m)
    # If there are multiple, create the parent-child structure
    else:
        if m.ssync:
            create_or_update_resources(doc, client, update=True)
            create_or_update_parent(doc, client, update=True)
        else:
            resources = create_or_update_resources(doc, client, m=m)
            create_or_update_parent(doc, client, children=resources, m=m)

    # Write the changes to the file
    doc.write_csv(m.mt_file)

def create_or_update_parent(doc, client, children=None, update=False, m=None):
    '''
    Creates the parent dataset to which child datasets
    can be attributed and linked, while retaining the independence
    of the child datasets as separate assets
    '''
    new_parent = {}
    # Dataset Information
    name = doc.find_first_value('Root.Title')
    if update:
        prt("Updating package parent: {}".format(name))
    else:
        prt("Creating package parent: {}".format(name))
    new_parent.update({"name":name})
    # Description
    description = doc.find_first_value('Root.Description')
    new_parent.update({"description":description})
    # Attribution
    attribution = doc.find_first_value("Root.Name")
    new_parent.update({"attribution":attribution})
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
        # Get resource Values
        for child in doc.find('datafile'):
            api = "https://{}/d/{}".format(client.domain,child.get_value('dataset_id'))
            new_child = {
                "urls":{"API":api,doc.find_first_value('Root.Format'):child.value},
                "title":child.get_value('title'),
                }
            metadata['additionalAccessPoints'].append(new_child)
        # Update the parent
        ssync(new_parent, doc['root'].get_term("parent").value, client)
        parent_dataset = "https://{0}/d/{1}".format(client.domain,doc['root'].get_term("parent").value)
        prt("Parent dataset {} updated at {}".format(name,parent_dataset))
        return parent_dataset
    else:
        # Create the parent
        dataset_id = client.create(
            name,
            description=description,
            metadata=metadata,
            tags=tags,
            category=category,
            displayType=displayType,
            displayFormat=displayFormat,
            attribution=attribution,
            query=query
            )
        # Update file
        doc['Root'].get_or_new_term("Parent",dataset_id['id'])

        # Give the user some feedback
        parent_dataset = "https://{0}/d/{1}".format(client.domain,dataset_id['id'])
        prt("Parent dataset {} created at {}".format(name,parent_dataset))
        return

def create_or_update_resources(doc, client, parent=None, update=False, m=None):
    '''
    Creates separate Socrata assets with columnar schema and parent
    metadata
    @return: a dictionary of created child url and titles
    '''
    # Update the resource
    if update:
        datasets_raw = doc.find("root.datafile")
        prt("Updating {} new datasets".format(len(datasets_raw)))
        updated_datasets = []
        for d in datasets_raw:
            new_dataset = get_metadata(doc, d)
            new_dataset['columns'] = get_columns_with_fieldnames(doc, d.get_value("schema"))
            synced_dataset = ssync(new_dataset, d.get_value("dataset_id"), client)
            updated_datasets.append(synced_dataset)
        prt("{} datasets updated".format(len(updated_datasets)))
        return updated_datasets
    # Create the resource
    else:
        datasets_raw = doc.find("root.datafile")
        prt("Creating {} new datasets".format(len(datasets_raw)))
        resources = []
        for dataset in datasets_raw:
            new_dataset = get_metadata(doc, dataset)
            child = publish(new_dataset, client)
            dataset.get_or_new_child("dataset_id",child['dataset_id'])
            set_column_fieldnames(doc, dataset.get_value("schema"), child['columns'])
            resources.append(child)
        return resources

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
    table = ""
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

def get_columns_with_fieldnames(doc, schema):
    # Get the right table name
    # TODO: There MUST be a better way to do this
    table = ""
    for meta_schema in doc.get_section("schema"):
        if schema == meta_schema.value:
            table = meta_schema.term
    column_metadata_raw = doc.find(table+".column")
    column_metadata = []
    for column in column_metadata_raw:
        new_column = dict()
        column_id = column.get_value("column_id") if column.get_value("column_id") else ""
        new_column.update({"fieldName":column_id})
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

def set_column_fieldnames(doc, schema, columns):
    table = ""
    for meta_schema in doc.get_section("schema"):
        if schema == meta_schema.value:
            table = meta_schema.term
    column_metadata_raw = doc.find(table+".column")
    i = 0
    for column in column_metadata_raw:
        column.get_or_new_child("column_id",columns[i]['fieldName'])
        i += 1
    return

def publish(new_dataset, client):
    '''
    Using sodapy, the child assets are uploaded as metadata only assets
    @return: child dictionary
    '''
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
        columns=dataset_id['columns'],
        dataset_id=dataset_id['id'],
        api="https://{}/resource/{}.json".format(client.domain, dataset_id['id']),
        source="https://{}/d/{}".format(client.domain, dataset_id['id']),
        link=new_dataset['attributionLink'],
        title=new_dataset['name']
    )
    prt("{} published to {}".format(child['title'],child['source']))
    return child

def ssync(new_metadata, dataset_id, client):
    # Catch the bad dataset IDs
    valid, message = validate_four_by_bour(dataset_id, client)
    if not valid:
        err(message)

    metadata_raw = client.get_metadata(dataset_id)
    metadata = deepcopy(metadata_raw)
    diff_metadata = diff(metadata, new_metadata)
    try:
        diff_metadata['columns'] = diff_cols(metadata['columns'],new_metadata['columns'])
    except KeyError:
        prt("Updating Parent")
    response = client.update_metadata(dataset_id, diff_metadata)
    return response

def validate_four_by_bour(dataset_id, client):
    '''
    Validate whether or not the dataset id supplied by the sync
    param is valid and that the dataset exists
    @return: Boolean, message
    '''
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
    return True, "Success"

def diff(old, new):
    return({k:v for k,v in new.items() if k not in old or v != old[k]})

def diff_cols(old, new):
    new_cols = []
    for n in new:
        for o in old:
            if n['fieldName'] == o['fieldName']:
                n['id'] = o['id']
                new_cols.append(n)
    return new_cols
