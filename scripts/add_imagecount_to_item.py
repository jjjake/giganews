#!/usr/bin/env python
import gevent.monkey
gevent.monkey.patch_all()
import gevent.pool
import gevent.queue

import sys
import io
import gzip

import requests
from internetarchive import get_item


Q = gevent.queue.Queue()


def get_gzip_file_from_url(url):
    r = requests.get(url)
    bi = io.BytesIO(r.content)
    gf = gzip.GzipFile(fileobj=bi, mode='rb')
    count = 0
    count += (len([x for x in gf if x]) - 1)
    Q.put(count)

def get_index_urls(item):
    for f in item.iter_files():
        if f.format == 'Comma-Separated Values GZ':
            yield 'http://archive.org/download/{0}/{1}'.format(item.identifier, f.name)

if __name__ == '__main__':
    identifier = sys.argv[-1]
    item = get_item(identifier)

    pool = gevent.pool.Pool(40)
    pool.map(get_gzip_file_from_url, get_index_urls(item))
    _imagecount = 0

    while not Q.empty():
        _imagecount += Q.get()

    if _imagecount == int(item.metadata.get('imagecount', 0)):
        sys.stdout.write('{0} - imagecount is up to date\n'.format(item.identifier))
        sys.exit(0)

    md = dict(imagecount=_imagecount)
    r = item.modify_metadata(md)
    if r.status_code == 200:
        sys.stdout.write('{0} - imagecount is up to date\n'.format(item.identifier))
        sys.exit(0)
    else:
        sys.stderr.write('{0} - error updating imagecount, {1}\n'.format(item.identifier, r.json().get('error')))
        sys.exit(1)
