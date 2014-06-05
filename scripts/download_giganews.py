#!/home/jake/.virtualenvs/giganews/bin/python
import gevent.monkey; gevent.monkey.patch_all()
import gevent.pool

import sys
import time
import random

import logging
import yaml
from giganews import NewsGroup, GiganewsSession


log = logging.getLogger('giganews')


# archive_group()
# ________________________________________________________________________________________
def archive_group(group):
    # Hack to skip tricky groups for now.
    #if count > 11000 or 'binaries' in group:
    if 'binaries' in group:
        log.info('skipping {0} for now...'.format(group))
        return

    # TODO: fix this hack, which is used to skip groups only containig
    # a single level in their heirarchy. These groups conflict with items
    # from the historical usenet collection!
    if '.' not in group:
        return

    g = NewsGroup(group, session=sesh, logging_level='INFO', ia_sync=True)

    # Check for new articles.
    count = int(g.last) - int(g.first)
    if count <= 0:
        log.info('no new articles found for {0}'.format(group))
        return

    g.archive_articles()


# ________________________________________________________________________________________
if __name__ == '__main__':
    global sesh
    sesh = GiganewsSession()

    news_list = [x.split()[0] for x in open('giganews_listfile.txt')]
    random.shuffle(news_list)
    pool = gevent.pool.Pool(16)
    pool.map(archive_group, news_list)
    #for g in news_list:
    #    print g
    #    archive_group(g)
