#!/usr/bin/env python
import sys
import io
import gzip

import requests
from internetarchive import get_item


def get_gzip_file_from_url(url):
    r = requests.get(url)
    bi = io.BytesIO(r.content)
    return gzip.GzipFile(fileobj=bi, mode='rb')


if __name__ == '__main__':
    identifier = sys.argv[-1]
    item = get_item(identifier)
    count = 0
    for f in item.files():
        if f.format == 'Comma-Separated Values GZ':
            gf = get_gzip_file_from_url(f.url)
            count += len([x for x in gf if x]) - 1
    print count
    md = dict(imagecount=count)
    print item.metadata['metadata'].get('title')
    print '{0}\t{1}'.format(item.identifier, item.modify_metadata(md).get('status_code'))
