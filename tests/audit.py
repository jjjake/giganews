#!/usr/bin/env python
import sys
import io
import gzip
import mailbox
import os
import re
import time

import requests
from internetarchive import get_item


def get_gzip_file_from_url(url):
    r = requests.get(url)
    bi = io.BytesIO(r.content)
    return gzip.GzipFile(fileobj=bi, mode='rb')


def audit_item(identifier):
    item = get_item(identifier)
    files = list(item.files())
    idxs = [f for f in files if f.name.endswith('.csv.gz')]
    mboxs = [f for f in files if f.name.endswith('.mbox.gz')]
    imagecount = 0

    for m in mboxs:
        group = '.'.join(m.name.split('.')[:-3])
        if not group in item.metadata.get('state', {}):
            sys.stderr.write('{id}/{m} has no recorded state!\n'.format(id=identifier,
                                                                        m=m.name))
            sys.exit(1)

        # Test for matching index.
        if not any(m.name.replace('.mbox.gz', '') in i.name for i in idxs):
            sys.stderr.write('{id}/{m} does not have an index!\n'.format(id=identifier,
                                                                         m=m.name))
            sys.exit(1)

        idx_url = m.url.replace('.gz', '.csv.gz')
        idx = [
            x.strip() for x in get_gzip_file_from_url(idx_url) if x and not '#date\t' in x
        ]

        gf = get_gzip_file_from_url(m.url)
        tmp_mbox_file = '/tmp/{0}'.format(m.name.strip('.gz'))
        with open(tmp_mbox_file, 'wb') as fp:
            fp.write(gf.read())
        mbox = mailbox.mbox(tmp_mbox_file)
        imagecount += len(mbox)
        os.remove(tmp_mbox_file)
        msgs = [{k.lower(): v for (k, v) in x.items()} for x in mbox]
        for msg in msgs:
            if not any(msg.get('message-id') in s for s in idx):
                sys.stderr.write('{id}/{m} has invalid index!\n'.fomrat(id=identifier,
                                                                        m=m.name))
                sys.exit(1)

        # Test that the MBOX and IDX contain the same number of messages.
        if len(mbox) != len(idx):
            sys.stderr.write('{id}/{m} does not match index!\n'.format(id=identifier,
                                                                       m=m.name))
            sys.exit(1)

        for line in idx:
            idx_name = idx_url.split('/')[-1]
            columns = line.split('\t')
            date = columns[0]
            msg_id = columns[1]
            start = columns[-2]
            length = columns[-1]

            # Test for valid dates.
            if not re.match('\d{14}', date):
                sys.stderr.write(
                    '{id}/{i} contains invalid dates!\n'.format(id=identifier,
                                                                i=idx_name))
                sys.exit(1)

            # Test for valid start.
            if not re.match('\d+', start):
                sys.stderr.write(
                    '{id}/{i} contains invalid start!\n'.format(id=identifier,
                                                                i=idx_name))
                sys.exit(1)

            # Test for valid length.
            if not re.match('\d+', length):
                sys.stderr.write(
                    '{id}/{i} contains invalid length!\n'.format(id=identifier,
                                                                 i=idx_name))
                sys.exit(1)

            # Test for valid column count.
            if not len(columns) == 8:
                sys.stderr.write(
                    '{id}/{i} has extra or missing columns!\n'.format(id=identifier,
                                                                      i=idx_name))
                sys.exit(1)

            # Test for valid row count.
            if not any(m.get('message-id' '') == msg_id for m in msgs):
                sys.stderr.write(
                    '{id}/{i} has extra or missing rows!\n'.format(id=identifier,
                                                                   i=idx_name))
                sys.exit(1)

    for i in idxs:
        # Test for matching MBOX.
        if not any(i.name.replace('.mbox.csv.gz', '') in m.name for m in mboxs):
            sys.stderr.write('{id}/{i} does not have an mbox!\n'.format(id=identifier,
                                                                        i=i.name))
            sys.exit(1)

    # Test image count.
    ia_imagecount = int(item.metadata.get('metadata', {}).get('imagecount', 0))
    if ia_imagecount != imagecount:
        while True:
            resp = item.modify_metadata({'imagecount': imagecount})
            if resp.get('status_code') == 200:
                break
            else:
                time.sleep(3)

    return True


if __name__ == '__main__':
    identifier = sys.argv[-1]
    resp = audit_item(identifier)
    if resp is True:
        sys.stdout.write('{id} passed audit.\n'.format(id=identifier))
        sys.exit(0)
