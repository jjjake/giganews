#!/usr/bin/env python
import sys
import io
import gzip
import mailbox
import os
import re

import requests


IDX_FNAME = '{0}.mbox.csv.gz'.format(sys.argv[-1])
MBOX_FNAME = '{0}.mbox.gz'.format(sys.argv[-1])
IDENTIFIER = 'usenet-{0}'.format('.'.join(sys.argv[-1].split('.')[:2]))


def get_gzip_file_from_url(url):
    r = requests.get(url)
    bi = io.BytesIO(r.content)
    return gzip.GzipFile(fileobj=bi, mode='rb')


def count_idx():
    gf = get_gzip_file_from_url(u)
    return len([x for x in gf if x]) - 1


def test_mbox(idx):
    u = 'https://archive.org/download/{id}/{mbox}'.format(id=IDENTIFIER, mbox=MBOX_FNAME)
    gf = get_gzip_file_from_url(u)
    tmp_mbox_file = '/tmp/{0}'.format(MBOX_FNAME.strip('.gz'))
    with open(tmp_mbox_file, 'wb') as fp:
        fp.write(gf.read())
    mbox = mailbox.mbox(tmp_mbox_file)
    msg_count = len(mbox)

    msgs = [{k.lower(): v for (k,v) in m.items()} for m in mbox]
    assert len(mbox) == len(idx)
    for line in idx:
        rows = line.split('\t')
        date = rows[0]
        msg_id = rows[1]
        start = rows[-2]
        length = rows[-1]
        assert re.match('\d{14}', date)
        assert re.match('\d+', start)
        assert re.match('\d+', length)
        assert len(rows) == 8
        assert any(m['message-id'] == msg_id for m in msgs)

    for msg in msgs:
        assert any(msg['message-id'] in s for s in idx)

    os.remove(tmp_mbox_file)
    return msg_count


if __name__ == '__main__':
    idx_url = 'https://archive.org/download/{id}/{idx}'.format(id=IDENTIFIER, idx=IDX_FNAME)
    idx = [x.strip() for x in get_gzip_file_from_url(idx_url) if x and not '#date\t' in x]

    test_mbox(idx)
    print 'pass!'
