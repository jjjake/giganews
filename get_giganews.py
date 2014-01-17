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

import nntplib
from internetarchive import get_item
import futures
import magic


COLLECTION_DATE = time.strftime('%Y%m%d')
LIST_FILE = 'giganews_listfile_{date}.txt'.format(date=COLLECTION_DATE)
MBOX_LOCK = threading.RLock()
IDX_LOCK = threading.RLock()


def is_binary(text_buffer):
    """Check a given text_buffer for binary data.

    :type text_buffer: str
    :param text_buffer: The text buffer to be tested for binary data.

    :rtype: bool
    :returns: True if `text_buffer` is binary, else False.

    """
    data_type = magic.from_buffer(text_buffer)
    return True if data_type == 'data' else False


def inline_compress_chunk(chunk, level=9):
    """Join a list with '\n' and compress using gzip.

    :type chunk: list
    :param chunk: A list of strings to be compressed by gzip.

    :rtype: str
    :returns: The gzip compressed chunk represented as a string.
    """
    b = cStringIO.StringIO()
    g = gzip.GzipFile(fileobj=b, mode='wb', compresslevel=level)
    g.write(chunk)
    #g.write('\n')
    g.close()
    return b.getvalue()


def compress_and_sort_index(group):
    idx_fname = '{group}.mbox.{date}.csv'.format(group=group, date=COLLECTION_DATE)
    reader = csv.reader(open(idx_fname), dialect='excel-tab')
    index = [x for x in reader if x]
    sorted_index = sorted(index, key=itemgetter(0))
    gzip_idx_fname = idx_fname + '.gz'

    header = ['#date', 'msg_id', 'from', 'newsgroups', 'subject', 'start', 'length']
    s = cStringIO.StringIO()
    writer = csv.writer(s, dialect='excel-tab')
    writer.writerow(header)
    for line in sorted_index:
        writer.writerow(line)
    compressed_index = inline_compress_chunk(s.getvalue())

    with open(gzip_idx_fname, 'ab') as fp:
        fp.write(compressed_index)
    os.remove(idx_fname)


def get_utc_iso_date(date_str):
    utc_tuple = dateutil.parser.parse(date_str).utctimetuple()
    date_object = datetime.datetime.fromtimestamp(time.mktime(utc_tuple))
    utc_date_str = ''.join([x for x in date_object.isoformat() if x not in '-T:'])
    return utc_date_str


def save_article(article_number, group):
    try:
        _s = nntplib.NNTP('news.giganews.com')
        _s.group(group)
        resp, _, msg_id, msg_list = _s.article(article_number)
        _s.quit()
        if is_binary('\n'.join(msg_list)):
            return None
    except nntplib.NNTPTemporaryError as e:
        sys.stderr.write(
            'error downloading article #{0}: {1}\n'.format(article_number, e))
        return None
    mbox = email.message_from_string('\n'.join(msg_list))
    mbox = mbox.as_string(unixfrom=True)

    # Compress chunk and append to gzip file.
    gzip_fname = '{group}.mbox.{date}.gz'.format(group=group, date=COLLECTION_DATE)
    compressed_chunk = inline_compress_chunk(mbox)
    length = sys.getsizeof(compressed_chunk)
    with MBOX_LOCK:
        with open(gzip_fname, 'a') as fp:
            start = fp.tell()
            fp.write(compressed_chunk)
            #end = fp.tell()

    # Append index information to idx file.
    index_article(msg_list, article_number, start, length)
    print(' saved article #{a}'.format(a=article_number))
    return (int(article_number) + 1)


def index_article(msg_list, article_number, start, length):
    text = '\n'.join(msg_list)
    f = cStringIO.StringIO(text)
    message = rfc822.Message(f)

    # Replace header dict None values with '', and any tabs or
    # newlines with ' '.
    h = dict()
    for key in message.dict:
        if not message.dict[key]:
            h[key] = ''
        else:
            h[key] = message.dict[key].replace('\t', '')
            h[key] = message.dict[key].replace('\n', '')

    date = h.get('date', '')
    if date:
        date = get_utc_iso_date(date)
    idx_line = (date, h.get('message-id'), h.get('from'), h.get('newsgroups'),
                h.get('subject'), start, length)
    idx_fname = '{group}.mbox.{date}.csv'.format(group=group, date=COLLECTION_DATE)
    s = cStringIO.StringIO()
    writer = csv.writer(s, dialect='excel-tab')
    writer.writerow(idx_line)
    with IDX_LOCK:
        with open(idx_fname, 'a') as fp:
            fp.write(s.getvalue())
    return idx_line


if __name__ == '__main__':
    s = nntplib.NNTP('news.giganews.com')
    if not os.path.exists(LIST_FILE):
        s.list(file=LIST_FILE)

    for line in open(LIST_FILE):
        group, last, _first, flag = line.strip().split()
        identifier = 'usenet-{0}'.format('.'.join(group.split('.')[:2]))
        item = get_item(identifier)
        state = item.get_metadata(target='state')
        first = state.get(group, _first)

        # Exclude all groups with binaries (is this too much?).
        if 'binaries' in group:
            print('Skipping {group}, appears to be a binaries group'.format(group=group))
            continue

        # Check for new articles.
        print('Archiving {group}'.format(group=group))
        count = int(last) - int(first)
        if count <= 0:
            print(' no new articles found'.format(c=count))
            continue
        print(' {c} new articles found'.format(c=count))

        # Archive new articles.
        s.group(group)
        resp, article_list = s.xover(first, last)
        articles_archived = []
        article_numbers = tuple(a[0] for a in article_list)
        try:
            with futures.ThreadPoolExecutor(max_workers=30) as e:
                future_to_article = {
                    e.submit(save_article, a, group): a for a in article_numbers
                }
                for future in futures.as_completed(future_to_article):
                    article_number = future_to_article[future]
                    result = future.result()
                    if result:
                        articles_archived.append(result)
        except (KeyboardInterrupt, SystemExit):
            e.shutdown()
        finally:
            compress_and_sort_index(group)
            state[group] = max(articles_archived)
            idx_fname = '{group}.mbox.{date}.csv.gz'.format(group=group,
                                                            date=COLLECTION_DATE)
            mbox_fname = '{group}.mbox.{date}.gz'.format(group=group,
                                                         date=COLLECTION_DATE)
            item.upload([idx_fname, mbox_fname], verbose=True)
            item.modify_metadata(state, target='state')
