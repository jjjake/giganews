#!/usr/bin/env python
import time
import threading
import os.path
import sys
import email
import gzip
import cStringIO
import rfc822
import dateutil.parser
import datetime
import csv
from operator import itemgetter
import json

import nntplib
from internetarchive import get_item
import futures
import magic
import chardet


COLLECTION_DATE = time.strftime('%Y%m%d')
LIST_FILE = 'giganews_listfile_{date}.txt'.format(date=COLLECTION_DATE)
STATE_FILE = 'giganews_statefile_{date}.txt'.format(date=COLLECTION_DATE)
MBOX_LOCK = threading.RLock()
IDX_LOCK = threading.RLock()


def is_binary(string):
    """Check if `string` contains binary data.

    :type string: str
    :param string: The text buffer to be tested for binary data.

    :rtype: bool
    :returns: True if `string` is binary, else False.

    """
    data_type = magic.from_buffer(string)
    return True if data_type == 'data' else False


def encode_str(string, encoding='UTF-8'):
    """Attempt to detect the native encoding of `string`, and re-encode
    to utf-8

    :type string: str
    :param string: The string to be encoded.

    :rtype: str
    :returns: A utf-8 encoded string.

    """
    if not string:
        return ''
    src_enc = chardet.detect(string)['encoding']
    try:
        return string.decode(src_enc).encode(encoding)
    except:
        return string.decode('ascii', errors='replace').encode(encoding)


def inline_compress_chunk(chunk, level=9):
    """Compress a string using gzip.

    :type chunk: str
    :param chunk: The string to be compressed.

    :rtype: str
    :returns: `chunk` compressed.
    """
    b = cStringIO.StringIO()
    g = gzip.GzipFile(fileobj=b, mode='wb', compresslevel=level)
    g.write(chunk)
    g.close()
    return b.getvalue()


def get_utc_iso_date(date_str):
    try:
        utc_tuple = dateutil.parser.parse(date_str).utctimetuple()
    except ValueError:
        try:
            date_str = ' '.join(date_str.split(' ')[:-1])
            utc_tuple = dateutil.parser.parse(date_str).utctimetuple()
        except ValueError:
            date_str = ''.join(date_str.split('(')[:-1]).strip(')')
            utc_tuple = dateutil.parser.parse(date_str).utctimetuple()
    date_object = datetime.datetime.fromtimestamp(time.mktime(utc_tuple))
    utc_date_str = ''.join([x for x in date_object.isoformat() if x not in '-T:'])
    return utc_date_str


def compress_and_sort_index(group):
    idx_fname = '{group}.{date}.mbox.csv'.format(group=group, date=COLLECTION_DATE)
    reader = csv.reader(open(idx_fname), dialect='excel-tab')
    index = [x for x in reader if x]
    sorted_index = sorted(index, key=itemgetter(0))
    gzip_idx_fname = idx_fname + '.gz'

    # Include UTF-8 BOM in header.
    header = [
        '\xef\xbb\xbf#date', 'msg_id', 'from', 'newsgroups', 'subject', 'references',
        'start', 'length',
    ]

    s = cStringIO.StringIO()
    writer = csv.writer(s, dialect='excel-tab')
    writer.writerow(header)
    for line in sorted_index:
        writer.writerow(line)
    compressed_index = inline_compress_chunk(s.getvalue())

    with open(gzip_idx_fname, 'ab') as fp:
        fp.write(compressed_index)
    os.remove(idx_fname)


def download_article(article_number, group):
    i = 0
    while True:
        try:
            _s = nntplib.NNTP('news.giganews.com', readermode=True)
            _s.group(group)
            resp = _s.article(article_number)
            _s.quit()
            if resp:
                return resp
        except EOFError:
            i += 1
            time.sleep(1)
        except Exception as e:
            if e.response == '430 no such article' or i >= 10:
                return False


def save_article(article_number, group):
    resp = download_article(article_number, group)
    if not resp:
        return False
    _, _, _, msg_list = resp
    msg_str = '\n'.join(msg_list) + '\n\n'

    if is_binary(msg_str):
        sys.stderr.write(
            ' warning: skipping binary post, {g} {a}\n'.format(g=group, a=article_number))
        return False

    # Convert msg_list into an `email.Message` object.
    mbox = email.message_from_string(msg_str)
    mbox = mbox.as_string(unixfrom=True)

    # Compress chunk and append to gzip file.
    mbox_fname = '{group}.{date}.mbox.gz'.format(group=group, date=COLLECTION_DATE)
    compressed_chunk = inline_compress_chunk(mbox)
    length = sys.getsizeof(compressed_chunk)
    with MBOX_LOCK:
        with open(mbox_fname, 'a') as fp:
            start = fp.tell()
            fp.write(compressed_chunk)

    # Append index information to idx file.
    index_article(msg_str, article_number, start, length)
    sys.stdout.flush()
    sys.stdout.write('\r saved article #{a}'.format(a=article_number))
    sys.stdout.flush()
    return article_number


def index_article(msg_str, article_number, start, length):
    f = cStringIO.StringIO(msg_str)
    message = rfc822.Message(f)

    # Replace header dict None values with '', and any tabs or
    # newlines with ' '.
    h = dict()
    for key in message.dict:
        if not message.dict[key]:
            h[key] = ''
        h[key] = message.dict[key]
        h[key] = encode_str(message.dict[key])
        if '\n' in h[key]:
            h[key] = h[key].replace('\n', ' ')
        if '\t' in h[key]:
            h[key] = h[key].replace('\t', ' ')

    date = h.get('NNTP-Posting-Date')
    if not date:
        date = h.get('date', '')
    date = get_utc_iso_date(date)

    idx_line = (date, h.get('message-id'), h.get('from'), h.get('newsgroups'),
                h.get('subject'), h.get('references', ''), start, length)
    idx_fname = '{group}.{date}.mbox.csv'.format(group=group, date=COLLECTION_DATE)
    s = cStringIO.StringIO()
    writer = csv.writer(s, dialect='excel-tab')
    writer.writerow(idx_line)
    with IDX_LOCK:
        with open(idx_fname, 'a') as fp:
            fp.write(s.getvalue())
    return True


if __name__ == '__main__':
    s = nntplib.NNTP('news.giganews.com', readermode=True)
    if not os.path.exists(LIST_FILE):
        s.list(file=LIST_FILE)
        s.quit()
    try:
        local_state = json.load(open(STATE_FILE))
    except:
        local_state = {}

    for line in open(LIST_FILE):
        s = nntplib.NNTP('news.giganews.com', readermode=True)
        group, last, _first, flag = line.strip().split()
        identifier = 'usenet-{0}'.format('.'.join(group.split('.')[:2]))
        item = get_item(identifier)

        # Get state.
        remote_state = item.get_metadata(target='state')
        if local_state.get(group):
            first = str(local_state[group])
        elif remote_state.get(group):
            first = str(remote_state[group])
        else:
            first = _first
        state = {group: first}

        # Check for new articles.
        sys.stdout.write('\nArchiving {group}\n'.format(group=group))
        count = int(last) - int(first)
        if count <= 0:
            sys.stdout.write(' no new articles found\n'.format(c=count))
            continue

        if count > 10000:
            print 'skip'
            continue
        if '.' not in group:
            print 'skip'
            continue

        # Archive new articles.
        s.group(group)
        resp, article_list = s.xover(first, last)
        s.quit()

        articles_archived = []
        article_numbers = tuple(a[0] for a in article_list)
        sys.stdout.write(' articles found: {c}\n'.format(c=len(article_numbers)))
        sys.stdout.write(' first article: #{0}.\n'.format(first))
        sys.stdout.write(' last article: #{0}.\n'.format(max(article_numbers)))

        # Concurrently download and index all articles in group that
        # haven't been archived yet.
        with futures.ThreadPoolExecutor(max_workers=30) as e:
            try:
                future_to_article = {
                    e.submit(save_article, a, group): a for a in article_numbers
                }
                for future in futures.as_completed(future_to_article):
                    article_number = future_to_article[future]
                    result = future.result()
                    if result:
                        articles_archived.append(int(article_number))
            except:
                e.shutdown(wait=False)
                for f in future_to_article:
                    if not f.running() and not f.done():
                        f.cancel()
                while True:
                    pending_futures = []
                    for f in future_to_article:
                        if f.running():
                            pending_futures.append(f)
                        if f.done() and not f.cancelled():
                            result = f.result()
                            if result:
                                articles_archived.append(int(result))
                    if len(pending_futures) == 0:
                        break
                    time.sleep(1)
                raise
            finally:
                state[group] = max(articles_archived)
                with open(STATE_FILE, 'w') as fp:
                    json.dump(state, fp)

        # Compress and sort index, upload and record state.
        compress_and_sort_index(group)
        idx_fname = '{group}.{date}.mbox.csv.gz'.format(group=group,
                                                        date=COLLECTION_DATE)
        mbox_fname = '{group}.{date}.mbox.gz'.format(group=group,
                                                     date=COLLECTION_DATE)
        item_group = item.identifier.replace('usenet-', '')
        count = (int(item.metadata.get('metadata', {}).get('imagecount', 0)) +
                    len(articles_archived))
        subject = item.metadata.get('metadata', {}).get('subject')
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

        sys.stdout.write('\n uploading data to {0}\n'.format(item.identifier))
        resps = item.upload([idx_fname, mbox_fname], metadata=metadata)
        if [r.status_code for r in resps] != [200, 200]:
            sys.stderr.write('error uploading data.\n')
            sys.exit(1)

        sys.stdout.write(' updating imagecount\n')
        i = 0
        while True:
            resp = item.modify_metadata({'imagecount': count, 'subject': subject})
            if resp.get('status_code') == 200:
                break
            else:
                time.sleep(2)
                i += 1
            if i >= 10:
                sys.stderr.write(
                    ' error modifying imagecount. imagecount is: {0}\n'.format(count))
                sys.exit(1)

        sys.stdout.write(' updating state\n')
        i = 0
        while True:
            resp = item.modify_metadata(state, target='state')
            if resp.get('status_code') == 200:
                os.remove(idx_fname)
                os.remove(mbox_fname)
                break
            else:
                time.sleep(2)
                i += 1
            if i >= 10:
                sys.stderr.write(
                    'error modifying state. state is: {0}\n'.format(state.get('group')))
                sys.exit(1)
