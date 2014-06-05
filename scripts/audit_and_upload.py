#!/usr/bin/env python
import sys
import gzip
import mailbox
import os
import re
import json
import time
import subprocess
import logging
import glob
import StringIO

from internetarchive import get_item
import requests


# logging.
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# logging file handler.
fh = logging.FileHandler('audit_and_upload-error.log')
fh.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')
fh.setFormatter(formatter)
log.addHandler(fh)

# logging console handler.
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)


def audit_mbox(mbox_fname, idx_fname, state_fname):
    # Test for matching index.
    if not os.path.exists(idx_fname):
        sys.stderr.write('{m} does not have an index!\n'.format(m=mbox_fname))
        sys.exit(1)

    idx = [
        x.strip() for x in gzip.open(idx_fname, 'rb') if x and not '#date\t' in x
    ]

    tmp_mbox_fname = '/tmp/{0}'.format(mbox_fname.strip('.gz'))
    p = subprocess.Popen(['zcat', mbox_fname], stdout=open(tmp_mbox_fname, 'wb'))
    exit_code = p.wait()
    msg_count = 0
    msg_ids = []
    msg_count = len(list(mailbox.mbox(tmp_mbox_fname).iterkeys()))
    ###for i, m in enumerate(mailbox.mbox(tmp_mbox_fname)):
    ###    msg_count += 1 # message counter.

    ###    # Test that the given message is indexed.
    ###    if any(m.get('Message-ID', '') in l for l in idx) != True \
    ###        or not m.get('Message-ID'):
    ###        sys.stderr.write('{m} has invalid index!\n'.format(m=mbox_fname))
    ###        clean_up(mbox_fname, idx_fname, state_fname)
    ###    msg_ids.append(m.get('Message-ID', ''))

    # Test that the MBOX and IDX contain the same number of messages.
    if msg_count != len(idx):
        sys.stderr.write('{m} does not match index!\n'.format(m=mbox_fname))
        clean_up(mbox_fname, idx_fname, state_fname)
        sys.exit(1)

    for line in idx:
        columns = line.split('\t')
        date = columns[0]
        msg_id = columns[1]
        start = columns[-2]
        length = columns[-1]

        # Test for valid dates.
        if not re.match('\d{14}', date):
            sys.stderr.write(
                '{i} contains invalid dates!\n'.format(i=idx_fname))
            clean_up(mbox_fname, idx_fname, state_fname)
            sys.exit(1)

        # Test for valid start.
        if not re.match('\d+', start):
            sys.stderr.write(
                '{i} contains invalid start!\n'.format(i=idx_fname))
            clean_up(mbox_fname, idx_fname, state_fname)
            sys.exit(1)

        # Test for valid length.
        if not re.match('\d+', length):
            sys.stderr.write(
                '{i} contains invalid length!\n'.format(i=idx_fname))
            clean_up(mbox_fname, idx_fname, state_fname)
            sys.exit(1)

        # Test for valid column count.
        if not len(columns) == 8:
            sys.stderr.write(
                '{i} has extra or missing columns!\n'.format(i=idx_fname))
            clean_up(mbox_fname, idx_fname, state_fname)
            sys.exit(1)

        #### Test for valid row count.
        ###if not any(m == msg_id for m in msg_ids):
        ###    sys.stderr.write(
        ###        '{i} has extra or missing rows!\n'.format(i=idx_fname))
        ###    clean_up(mbox_fname, idx_fname, state_fname)
        ###    sys.exit(1)

    os.remove(tmp_mbox_fname)
    return msg_count


def clean_up(mbox_fname, idx_fname, state_fname):
    files = [mbox_fname, idx_fname]
    # Delete state file, if no other mbox files for the given group exist.
    pat = '.'.join(mbox_fname.split('.')[:2]) + '*'
    if not glob.glob(pat):
        files.append(state_fname)
    for f in files:
        try:
            os.remove(f)
        except OSError:
            continue


def servers_are_offline(item):
    servers = [x.split('.')[0] for x in
                  [item.metadata.get('d1'), item.metadata.get('d2')] if x]
    offline_servers = [x.strip() for x in open('offline_servers.txt') if x]
    for s in servers:
        if s in offline_servers:
            return s


if __name__ == '__main__':
    mbox = sys.argv[-1]
    idx = mbox.replace('.gz', '.csv.gz')
    group = '.'.join(mbox.split('.')[:-3])
    identifier = 'usenet-{0}'.format('.'.join(group.split('.')[:2])).replace('+', '-')
    if identifier in ['usenet-alt.fan', 'usenet-microsoft.public']:
        sys.exit(1)
    item = get_item(identifier)

    log.debug('uploading "{m}" to "{i}".'.format(m=mbox, i=item.identifier))

    # If server is offline, skip for now and upload later.
    servers_offline = servers_are_offline(item)
    if servers_offline:
        log.error('{s} is offline, not uploading "{m}".'.format(s=servers_offline, m=mbox))
        sys.exit(1)

    state_fname = '{id}_state.json'.format(id=item.identifier)
    if not os.path.exists(state_fname):
        clean_up(mbox, idx, state_fname)
        log.error('"{m}" does not have a recorded state, '
                  'deleting mbox and index!'.format(m=mbox))
        sys.exit(1)

    ## Audit mbox and index files. Returns message count for the given mbox.
    msg_count = audit_mbox(mbox, idx, state_fname)
    log.debug('"{m}" passed audit.'.format(m=mbox))

    if not msg_count:
        log.error('"{m}" did not pass audit!'.format(m=mbox))
        sys.exit(1)

    # Files passed audit! Upload!
    #_____________________________________________________________________________________
    item_group = item.identifier.replace('usenet-', '')
    ia_count = int(item.metadata.get('imagecount', 0))
    count = ia_count + msg_count
    subject = item.metadata.get('subject')
    if not isinstance(subject, list):
        subject = [subject]
    if group not in subject:
        subject.append(group)
    metadata = dict(
        title=('Usenet groups within {group} '
               'from giganews.com'.format(group=item_group)),
        operator='jake@archive.org',
        description=('Usenet newsgroups within "{group}", contributed courtesy '
                     'of <a href=//www.giganews.com/">giganews.com</a>. '
                     'These captures omit most binary '
                     'posts.'.format(group=item_group)),
        subject=group,
        contributor='Giganews',
        collection='giganews',
        imagecount=count,
    )

    files = [idx, mbox]
    ia_state = item.__dict__.get('state', {})
    try:
        local_state = {group: json.load(open(state_fname)).get(group)}
        _state = ia_state.copy()
        _state.update(local_state)
        state = json.dumps(_state)
        s = StringIO.StringIO() 
        s.write(state)
        r = item.upload_file(s, state_fname, metadata=metadata)
        r.raise_for_status()
        log.info('updated state of "{g}" in "{i}".'.format(g=group, i=item.identifier))
    except Exception as exc:
        log.error('failed to update state of "{g}" in "{i}". '
                  'error: "{e}".'.format(g=group, i=item.identifier, e=exc))
        sys.exit(1)

    # Upload MBOX & IDX.
    #_____________________________________________________________________________________
    i = 0
    while True:
        resps = item.upload([mbox, idx], metadata=metadata)
        if all(r.status_code == 200 for r in resps):
            break
        else:
            time.sleep(2)
            i +=1
        if i >= 10:
            sys.stderr.write('error uploading data.\n')
            sys.exit(1)
    log.info('uploaded "{m}" and "{idx}" to "{i}".'.format(m=mbox, idx=idx, 
                                                           i=item.identifier))

    # Update imagecount.
    #_____________________________________________________________________________________
    i = 0
    while True:
        resp = item.modify_metadata({'imagecount': count, 'subject': subject})
        error_msg = resp.json().get('error', '')
        if resp.status_code in [200, 0] or 'no changes' in error_msg:
            break
        else:
            time.sleep(2)
            i += 1
        if i >= 10 or 'error getting metadata' in error_msg:
            log.error('failed to update imagecount for "{i}". '
                      'imagecount is "{c}".'.format(i=item.identifier, c=count))
            sys.exit(1)
    log.info('updated imagecount for "{i}".'.format(m=mbox, idx=idx, i=item.identifier))

    # Item updated successfully! Cleanup files.
    clean_up(mbox, idx, state_fname)
    log.info('successfully archived "{m}".'.format(m=mbox))
