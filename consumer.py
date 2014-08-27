## consumer for arxiv - using resourcesync

import re
import requests
from lxml import etree

from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

NAME = 'arxiv'
NAMESPACES = {'urlset': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

def consume():
    changes_url = 'http://resync.library.cornell.edu/arxiv-all/changelist.xml'

    changelist = requests.get(changes_url)
    changeXML = etree.XML(changelist.content)

    urls_for_info = changeXML.xpath('//urlset:loc/node()', namespaces=NAMESPACES)

    export_base = 'http://export.arxiv.org/api/query?search_query='

    xml_list = []

    urls_processed = 0
    for url in urls_for_info:
        print 'Now processing url: {}, number {}'.format(url, urls_processed)
        # matches everything after a slash then 4 numbers, a dot, 4 more numbers
        try:
            arxiv_id = re.search('(?<=/)\d{4}(\.)?\d{4}', url).group(0)
        except AttributeError:
            print 'Warning: malformed arxiv ID, skipping entry for {}'.format(url)
        print arxiv_id
        urls_processed += 1

    #     export_url = export_base + arxiv_id

    #     record_request = requests.get(export_url)

    #     record = etree.XML(record_request.content)

    #     xml_list.append(RawDocument({
    #                 'doc': record,
    #                 'source': NAME,
    #                 'doc_id': doc_id,
    #                 'filetype': 'xml'
    #             }))

    # return xml_list




consume()

# 0506253