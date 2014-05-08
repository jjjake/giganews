#!/usr/bin/env python
import argparse

from giganews import NewsGroup


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Archive Giganews.')
    parser.add_argument('group', metavar='<group>...', type=str, nargs='+')
    parser.add_argument('--user', metavar='<user>...', type=str)
    parser.add_argument('--password', metavar='<password>...', type=str)
    args = parser.parse_args()

    for group_name in args.group:
        g = NewsGroup(group_name, user=args.user, password=args.password)
        g.archive_articles()
