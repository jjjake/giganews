#!/home/jake/.virtualenvs/giganews/bin/python
import sys
import time
import random

import logging
import yaml
import futures
from giganews import NewsGroup


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

    while True:
        try:
            user, password = ACCOUNTS.pop()
            break
        except IndexError:
            #log.debug('no accounts available, sleeping until one becomes available.')
            time.sleep(10)

    try:
        g = NewsGroup(group, user, password, logging_level='WARNING')

        # Check for new articles.
        count = int(g.last) - int(g.first)
        if count <= 0:
            log.info('no new articles found for {0}'.format(group))
            return

        g.archive_articles()

    finally:
        ACCOUNTS.append((user, password))


# init_accounts()
# ________________________________________________________________________________________
def init_accounts():
    global ACCOUNTS
    ACCOUNTS = yaml.load(open('/home/jake/.config/giganews.yml')).get('accounts', {}).items()


# ________________________________________________________________________________________
if __name__ == '__main__':
    init_accounts()

    news_list = [x.split()[0] for x in open('giganews_listfile.txt')]
    random.shuffle(news_list)

    try:
        # Concurrently archive all news for each newsgroup.
        with futures.ThreadPoolExecutor(max_workers=len(ACCOUNTS)) as e:
            future_to_group = {
                e.submit(archive_group, group): group for group in news_list
            }
            for future in futures.as_completed(future_to_group):
                group = future_to_group[future]
                result = group.result()
    except KeyboardInterrupt:
        sys.exit(1)
