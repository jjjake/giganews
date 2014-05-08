import gevent.monkey
gevent.monkey.patch_all()
import gevent

import email
import sys
import threading
import shutil
import csv
import rfc822
import traceback
import futures
from operator import itemgetter
import os
import json

import nntplib
from internetarchive import get_item
import cStringIO

from .utils import is_binary, inline_compress_chunk, utf8_encode_str, get_utc_iso_date
from .logging import set_logger


class NewsGroup(object):

    log_level = {
        'CRITICAL': 50,
        'ERROR': 40,
        'WARNING': 30,
        'INFO': 20,
        'DEBUG': 10,
        'NOTSET': 0,
    }

    def __init__(self, name, user=None, password=None, ia_sync=True, 
                 max_nntp_connections=50, logging_level='INFO'):

        self.MAX_NNTP_CONNECTIONS = max_nntp_connections
        self.log = set_logger(self.log_level[logging_level], 'giganews.log')

        self.name = name
        self.identifier = 'usenet-{0}'.format('.'.join(name.split('.')[:2])).replace('+', '-')
        self.item = get_item(self.identifier)
        self.state = self.get_item_state()
        self.articles_archived = []

        self.user = user
        self.password = password
        self.connections = []

        # load group.
        _c = self._load_connection()
        self._response =  _c.group(self.name)
        _, count, first, last, _ = self._response
        nntp_date = _c.date()        
        _c.quit()

        # nntp_date = ('111 20140508035909', '140508', '035909')
        self.date = nntp_date[0].split()[-1][:8]
        self.count = count
        self.first = first
        self.last = last
        self.name = name

        if ia_sync:
            self.first = self.state[self.name]

        self._mbox_lock = threading.RLock()
        self._idx_lock = threading.RLock()


    def get_item_state(self):
        # get local state.
        try:
            local_state_fname = '{id}_state.json'.format(id=self.identifier)
            local_state = json.load(open(local_state_fname))
        except:
            local_state = {}
        # get remote state.
        remote_state = self.item.__dict__.get('state', {})
        # merge state (local state over-rides remote state).
        state = dict(remote_state.items() + local_state.items())

        # Update ``first`` with recorded state.
        if state.get(self.name):
            first = str(state[self.name])
        else:
            first = 0
        state[self.name] = first
        return state

    @property
    def article_numbers(self):
        if self.connections:
            _c = self.connections[0]
        else:
            _c = self._load_connection()

        _c.group(self.name)
        _, article_list = _c.xover(self.first, self.last)
        
        # Only quit if a cached connections wasn't used.
        if self.connections:
            self.connections.append(_c)
        return tuple(a[0] for a in article_list)

    
    # _load_connection()
    # ____________________________________________________________________________________
    def _load_connection(self, cache_connection=False):
        c = nntplib.NNTP('news.giganews.com', user=self.user, password=self.password, 
                         readermode=True)
        self.log.debug('initialized new connection')
        if cache_connection:
            self.connections.append(c)
            self.log.debug('cached connection')
        return c


    # load_max_connections()
    # ____________________________________________________________________________________
    def load_max_connections(self):
        if len(self.connections) >= self.MAX_NNTP_CONNECTIONS:
            return

        # Load MAX_NNTP_CONNECTIONS asynchronously.
        if len(self.connections) < self.MAX_NNTP_CONNECTIONS:
            connections_needed = (self.MAX_NNTP_CONNECTIONS - len(self.connections))
        else:
            connections_needed = self.MAX_NNTP_CONNECTIONS

        threads = []
        for x in range(0, connections_needed):
            threads.append(gevent.spawn(self._load_connection, True))
        gevent.joinall(threads)


    # archive_articles()
    # ____________________________________________________________________________________
    def archive_articles(self):
        self.load_max_connections()

        with futures.ThreadPoolExecutor(self.MAX_NNTP_CONNECTIONS) as executor:
            future_to_article = {
                executor.submit(self._download_article, a): a for a in self.article_numbers
            }
            for future in futures.as_completed(future_to_article):
                article_number = future_to_article[future]
                try:
                    resp = future.result()
                    self.save_article(resp, article_number)
                except:
                    traceback.print_exc()

        self.connections = []
        self.compress_and_sort_index()
        self.state[self.name] = max(self.articles_archived)
        local_state_fname = '{identifier}_state.json'.format(**self.__dict__)
        with open(local_state_fname, 'w') as fp:
            json.dump(self.state, fp)

        ## Item is ready to upload, remove lock.
        mbox_fname = '{name}.{date}.mbox.gz'.format(**self.__dict__)
        mbox_lck_fname = mbox_fname + '.lck'
        shutil.move(mbox_lck_fname, mbox_fname)
        self.log.info('archived and indexed {0} '
                      'articles from {1}'.format(len(self.articles_archived), self.name))


    # _download_article()
    # ____________________________________________________________________________________
    def _download_article(self, article_number, max_retries=10):
        """Download a given article.

        :type article_number: str
        :param article_number: the article number to download.

        :type group: str
        :param group: the group that contains the article to be downloaded.

        :returns: nntplib article response object if successful, else False.

        """
        # Get connection.
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        while True:
            try:
                _connection = self.connections.pop()
                _connection.group(self.name)
                break
            except IndexError:
                self.log.warning('no connections available...')

        # Download Article.
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        i = 0
        while True:
            if i >= max_retries:
                self.log.error('failed to retrieve article')
                return False

            try:
                resp = _connection.article(article_number)
                return resp

            # Connection closed, transient error, retry forever.
            except EOFError:
                self.log.warning('EOFError, refreshing connection retrying -- '
                                 'article={0}, group={1}'.format(article_number, 
                                                                 self.name))
                _connection.quit()
                _connection = self._load_connection()

            # NNTP Error.
            except nntplib.NNTPError as exc:
                self.log.warning('NNTPError: {0} -- article={1}, '
                                 'group={2}'.format(exc, article_number, self.name))
                if any(s in exc.response for s in ['430', '423']):
                    # Don't retry, article probably doesn't exist.
                    i = max_retries
                else:
                    i += 1
            except Exception:
                return

            # Always return connection back to the pool!
            finally:
                self.connections.append(_connection)


    # save_article()
    # ____________________________________________________________________________________
    def save_article(self, response, article_number, max_retries=10, skip_binary=True):
        #response = self._download_article(article_number)
        r, _, _, msg_list = response
        msg_str = '\n'.join(msg_list) + '\n\n'

        if is_binary(msg_str):
            self.log.debug('skipping binary post, {0} {1}'.format(self.name, 
                                                                  article_number))
            return

        # Convert msg_list into an `email.Message` object.
        mbox = email.message_from_string(msg_str)
        mbox = mbox.as_string(unixfrom=True)

        # Compress chunk and append to gzip file.
        mbox_fname = '{name}.{date}.mbox.gz.lck'.format(**self.__dict__)
        compressed_chunk = inline_compress_chunk(mbox)
        length = sys.getsizeof(compressed_chunk)
        with self._mbox_lock:
            with open(mbox_fname, 'a') as fp:
                start = fp.tell()
                fp.write(compressed_chunk)

        # Append index information to idx file.
        self.index_article(msg_str, article_number, start, length)
        self.articles_archived.append(article_number)
        self.log.info('saved article #{0} from {1}'.format(article_number, self.name))


    # index_article()
    # ____________________________________________________________________________________
    def index_article(self, msg_str, article_number, start, length):
        """Add article to index file.

        :type msg_str: str
        :param msg_str: the message string to index.

        :type article_number: str
        :param article_number: the article number to index.

        :type start: int
        :param start: the byte-offset where a given message starts in the
                      corresponding mbox file.

        :type length: int
        :param length: the byte-length of the message.

        :rtype: bool
        :returns: True

        """
        f = cStringIO.StringIO(msg_str)
        message = rfc822.Message(f)

        # Replace header dict None values with '', and any tabs or
        # newlines with ' '.
        h = dict()
        for key in message.dict:
            if not message.dict[key]:
                h[key] = ''
            h[key] = message.dict[key]
            h[key] = utf8_encode_str(message.dict[key])
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
        idx_fname = '{name}.{date}.mbox.csv'.format(**self.__dict__)
        s = cStringIO.StringIO()
        writer = csv.writer(s, dialect='excel-tab')
        writer.writerow(idx_line)
        with self._idx_lock:
            with open(idx_fname, 'a') as fp:
                fp.write(s.getvalue())
        return True


    # compress_and_sort_index()
    # ____________________________________________________________________________________
    def compress_and_sort_index(self):
        """Sort index, add header, and compress.

        :rtype: bool
        :returns: True

        """
        idx_fname = '{name}.{date}.mbox.csv'.format(**self.__dict__)
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
        return True
