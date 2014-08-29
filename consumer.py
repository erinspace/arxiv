## consumer for arxiv - using resourcesync

import re
import time
import requests
from lxml import etree

from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

NAME = 'arxiv'
NAMESPACES = {'urlset': 'http://www.sitemaps.org/schemas/sitemap/0.9',
            'arxiv': 'http://arxiv.org/schemas/atom',
            'atom': 'http://www.w3.org/2005/Atom'}

def consume():
    changes_url = 'http://resync.library.cornell.edu/arxiv-all/changelist.xml'

    changelist = requests.get(changes_url)
    changeXML = etree.XML(changelist.content)

    urls_for_info = changeXML.xpath('//urlset:loc/node()', namespaces=NAMESPACES)

    export_base = 'http://export.arxiv.org/api/query?search_query='

    xml_list = []

    for url in urls_for_info:
        try:
            # matches everything after a slash then 4 numbers, a dot, 4 more numbers
            arxiv_id = re.search('(?<=/)\d{4}(\.)?\d{4}', url).group(0)
        except AttributeError:
            print 'Warning: malformed arxiv ID, skipping entry for {}'.format(url)    

        export_url = export_base + arxiv_id

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

def get_ids(doc):
    ids = {}
    ids['doi'] = (doc.xpath('//arxiv:doi/node()', namespaces=NAMESPACES) or [''])[0]
    ids['service_id'] = (doc.xpath('//atom:entry/atom:id/node()', namespaces=NAMESPACES) or [''])[0]
    links = doc.xpath('//atom:link[@title="doi"]/@href', namespaces=NAMESPACES)
    if len(links) == 0:
        links = doc.xpath('//atom:link[@rel="alternate"]/@href', namespaces=NAMESPACES)
    ids['url'] = links[0]

    return ids

def get_contributors(doc):
    contributor_list = []
    contributors = doc.xpath('//atom:author/atom:name/node()', namespaces=NAMESPACES)
    for person in contributors:
        contributor_list.append({'full_name': person, 'email': ''})

    return contributor_list


def normalize(raw_doc, timestamp):
    raw_doc = raw_doc.get('doc')
    doc = etree.XML(raw_doc)

    normalized_dict = {
        'title': doc.xpath('//atom:entry/atom:title/node()', namespaces=NAMESPACES)[0],
        'contributors': get_contributors(doc),
        'properties': {
                'links': (doc.xpath('//atom:link/@href', namespaces=NAMESPACES) or [''])[0],
                'comments': (doc.xpath('//arxiv:comment/node()', namespaces=NAMESPACES) or [''])[0]
        },
        'description': (doc.xpath('//atom:summary/node()', namespaces=NAMESPACES) or [''])[0],
        'meta': {},
        'id': get_ids(doc),
        'source': NAME,
        'tags': doc.xpath('//atom:category/@term', namespaces=NAMESPACES),
        'date_created': doc.xpath('//atom:published/node()', namespaces=NAMESPACES)[0],
        'timestamp': str(timestamp)
    }


    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
