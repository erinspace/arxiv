## consumer for arxiv - using resourcesync

import re
import time
import json
import requests
from lxml import etree
from dateutil.parser import *
from nameparser import HumanName

from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

NAME = 'arxiv'
NAMESPACES = {'urlset': 'http://www.sitemaps.org/schemas/sitemap/0.9',
            'arxiv': 'http://arxiv.org/schemas/atom',
            'atom': 'http://www.w3.org/2005/Atom'}

# does not use the days back argument because of the changelist and resourcesync
def consume(days_back=1):
    changes_url = 'http://resync.library.cornell.edu/arxiv-all/changelist.xml'

    changelist = requests.get(changes_url)
    changeXML = etree.XML(changelist.content)

    urls_for_info = changeXML.xpath('//urlset:loc/node()', namespaces=NAMESPACES)
    export_base = 'http://export.arxiv.org/api/query?search_query='

    xml_list = []
    for url in urls_for_info[:5]:
        try:
            # matches everything after a slash then 4 numbers, a dot, 4 more numbers
            arxiv_id = re.search('(?<=/)\d{4}(\.)?\d{4}', url).group(0)
        except AttributeError:
            print 'Warning: malformed arxiv ID, skipping entry for {}'.format(url)    

        export_url = export_base + arxiv_id
        print export_url

        record_request = requests.get(export_url)
        record = etree.XML(record_request.content)

        xml_list.append(RawDocument({
                    'doc': etree.tostring(record),
                    'source': NAME,
                    'doc_id': arxiv_id,
                    'filetype': 'xml'
                }))
        time.sleep(2)

    return xml_list

def get_ids(doc, raw_doc):
    ids = {}
    ids['doi'] = (doc.xpath('//arxiv:doi/node()', namespaces=NAMESPACES) or [''])[0]
    ids['service_id'] = raw_doc.get('doc_id')
    links = doc.xpath('//atom:link[@title="doi"]/@href', namespaces=NAMESPACES)
    if len(links) == 0:
        links = doc.xpath('//atom:link[@rel="alternate"]/@href', namespaces=NAMESPACES)
    ids['url'] = links[0]
    return ids

def get_contributors(doc):
    contributor_list = []
    contributors = doc.xpath('//atom:author/atom:name/node()', namespaces=NAMESPACES)
    for person in contributors:
        name = HumanName(person)
        contributor = {
            "prefix": name.title,
            "given": name.first,
            "middle": name.middle,
            "family": name.last,
            "suffix": name.suffix,
            "email": "",
            "ORCID": ""}
        contributor_list.append(contributor)
    return contributor_list

def get_date_created(doc):
    date_created = doc.xpath('//atom:published/node()', namespaces=NAMESPACES)[0]
    return parse(date_created).isoformat()

def get_date_updated(doc):
    date_updated = doc.xpath('//atom:feed/atom:updated/node()', namespaces=NAMESPACES)[0]
    return parse(date_updated).isoformat()

def get_tags(doc):
    tags_list = doc.xpath('//atom:category/@term', namespaces=NAMESPACES)
    return [tag.lower() for tag in tags_list]

def get_properties(doc):
    links = (doc.xpath("//atom:entry/atom:link/@href", namespaces=NAMESPACES) or [""])
    comments = (doc.xpath("//arxiv:comment/node()", namespaces=NAMESPACES) or [""])[0]
    updated = (doc.xpath("//atom:entry/atom:updated/node()", namespaces=NAMESPACES) or [""])[0]
    pdf = ''
    for index, link in enumerate(links):
        if "pdf" in link:
            pdf = link
            links.pop(index)
    return {"links": links, "comments": comments, "pdf": pdf, "updated": updated}

def normalize(raw_doc, timestamp):
    raw_doc_text = raw_doc.get('doc')
    doc = etree.XML(raw_doc_text)

    normalized_dict = {
        "title": doc.xpath("//atom:entry/atom:title/node()", namespaces=NAMESPACES)[0],
        "contributors": get_contributors(doc),
        "properties": get_properties(doc),
        "description": (doc.xpath("//atom:summary/node()", namespaces=NAMESPACES) or [""])[0],
        "id": get_ids(doc, raw_doc),
        "source": NAME,
        "tags": get_tags(doc),
        "date_created": get_date_created(doc),
        "dateUpdated": get_date_updated(doc),
        "timestamp": str(timestamp)
    }

    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
