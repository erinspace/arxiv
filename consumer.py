## consumer for arxiv - using resourcesync
from __future__ import unicode_literals

import re
import time
import requests
from lxml import etree
from dateutil.parser import *
from nameparser import HumanName

from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

NAME = 'arxiv'
NAMESPACES = {'urlset': 'http://www.sitemaps.org/schemas/sitemap/0.9',
              'arxiv': 'http://arxiv.org/schemas/atom',
              'atom': 'http://www.w3.org/2005/Atom'}

DEFAULT_ENCODING = 'UTF-8'
record_encoding = None

def copy_to_unicode(element):

    encoding = record_encoding or DEFAULT_ENCODING
    element = ''.join(element)
    if isinstance(element, unicode):
        return element
    else:
        return unicode(element, encoding=encoding)

# does not use the days back argument because of the changelist and resourcesync
def consume(days_back=1):
    changes_url = 'http://resync.library.cornell.edu/arxiv-all/changelist.xml'

    changelist = requests.get(changes_url)
    record_encoding = changelist.encoding
    changeXML = etree.XML(changelist.content)

    urls_for_info = changeXML.xpath('//urlset:loc/node()', namespaces=NAMESPACES)
    export_base = 'http://export.arxiv.org/api/query?search_query='

    xml_list = []
    print len(urls_for_info)
    for url in urls_for_info:
        try:
            # matches everything after a slash then 4 numbers, a dot, 4 more numbers
            arxiv_id = re.search('(?<=/)\d{4}(\.)?\d{4}', url).group(0)
        except AttributeError:
            print 'Warning: malformed arxiv ID, skipping entry for {}'.format(url)
            continue    

        export_url = export_base + arxiv_id

        record_request = requests.get(export_url)
        record_encoding = record_request.encoding
        record = etree.XML(record_request.content)

        xml_list.append(RawDocument({
                    'doc': etree.tostring(record),
                    'source': NAME,
                    'docID': copy_to_unicode(arxiv_id),
                    'filetype': 'xml'
                }))
        time.sleep(2)

    return xml_list

def get_ids(doc, raw_doc):

    doi = (doc.xpath('//arxiv:doi/node()', namespaces=NAMESPACES) or [''])[0]
    doi = copy_to_unicode(doi)

    links = doc.xpath('//atom:link[@title="doi"]/@href', namespaces=NAMESPACES)
    if len(links) == 0:
        links = doc.xpath('//atom:link[@rel="alternate"]/@href', namespaces=NAMESPACES)
    url = copy_to_unicode(links[0])

    ids = {
        'doi' : doi,
        'serviceID': raw_doc.get('docID'),
        'url': url
    }

    if url == '':
        raise Exception('Warning: No url provided!')

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
    isodatecreated = parse(date_created).isoformat()
    return copy_to_unicode(isodatecreated)

def get_date_updated(doc):
    date_updated = doc.xpath('//atom:feed/atom:updated/node()', namespaces=NAMESPACES)[0]
    isodateupdated = parse(date_updated).isoformat()
    return copy_to_unicode(isodateupdated)

def get_tags(doc):
    tags_list = doc.xpath('//atom:category/@term', namespaces=NAMESPACES)
    return [copy_to_unicode(tag.lower()) for tag in tags_list]

def get_properties(doc):
    links = (doc.xpath("//atom:entry/atom:link/@href", namespaces=NAMESPACES) or [""])
    comments = (doc.xpath("//arxiv:comment/node()", namespaces=NAMESPACES) or [""])[0]
    comments = copy_to_unicode(comments)
    updated = (doc.xpath("//atom:entry/atom:updated/node()", namespaces=NAMESPACES) or [""])[0]
    updated = copy_to_unicode(updated)
    pdf = ''
    unicode_links = []
    # if len(links) > 1:
    for index, link in enumerate(links):
        unicode_links.append(copy_to_unicode(link))
        if "pdf" in link:
            pdf = copy_to_unicode(link)
            # TODO - fix this strange error - index shouldn't error... 
            try:
                unicode_links.pop(index)
            except IndexError as e:
                print("{} - didn't remove pdf from links...".format(e))

    return {"links": unicode_links, "comments": comments, "pdf": pdf, "updated": updated}

def normalize(raw_doc):
    raw_doc_text = raw_doc.get('doc')
    doc = etree.XML(raw_doc_text)

    title = doc.xpath("//atom:entry/atom:title/node()", namespaces=NAMESPACES)[0]
    description = (doc.xpath("//atom:summary/node()", namespaces=NAMESPACES) or [""])[0]

    normalized_dict = {
        "title": copy_to_unicode(title),
        "contributors": get_contributors(doc),
        "properties": get_properties(doc),
        "description": copy_to_unicode(description),
        "id": get_ids(doc, raw_doc),
        "source": NAME,
        "tags": get_tags(doc),
        "dateCreated": get_date_created(doc),
        "dateUpdated": get_date_updated(doc),
        "raw": raw_doc_text
    }
    import pdb; pdb.set_trace()
    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
