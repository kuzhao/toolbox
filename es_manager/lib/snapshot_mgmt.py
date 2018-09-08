#!/usr/bin/env python
from elasticsearch import Elasticsearch, helpers
from fnmatch import fnmatch
import argparse
import datetime
import exceptions
import re
import traceback
import sys


def list_repo_snapshot(es_handle):
    repo_list = es_handle.cat.repositories()
    if not repo_list:
        print 'No registered repo found in cluster'
        raise exceptions.AssertionError
    repo_list = repo_list.rstrip('\n').split('\n')
    repo_dict = dict()
    for repo in repo_list:
        repo_name = repo.split()[0]
        repo_dict[repo_name] = dict()
        snapshot_list = es_handle.cat.snapshots(repository=repo_name).rstrip('\n').split('\n')
        if not snapshot_list[0]:
            snapshot_list = list()
        repo_dict[repo_name]['snapshots'] = snapshot_list
        repo_dict[repo_name]['type'] = repo.split()[1]
    # Cat repo/snapshots in hierarchical style before return
    for item in repo_dict:
        print '-----------------------'
        print 'In REPO NAME '+item+' (TYPE '+repo_dict[item]['type']+'):'
        for tmp in repo_dict[item]['snapshots']:
            print tmp
    return repo_dict


def snapshot(es_handle, flag_clear=True):
    snapshot_list = list_repo_snapshot(es_handle)['backup']['snapshots']
    if not snapshot_list:
        pass
    else:
        if flag_clear:
            # Remove the first snapshot in repo
            for item in snapshot_list:
                if snapshot_list.index(item) == len(snapshot_list)-1:
                    break
                else:
                    es_handle.snapshot.delete(repository='backup', snapshot=item.split()[0])

    snapshot_name = 'snapshot_'+datetime.date.today().strftime('%Y%m%d')
    es_handle.snapshot.create(repository='backup', snapshot=snapshot_name)

if __name__ == '__main__':
    es = Elasticsearch('elasticsearch')
    parser = argparse.ArgumentParser(description='A small py script that help manage ELS snapshots')
    parser.add_argument('-L', '--list', dest='flag_list', action='store_true', default=False,
                        help='List current snapshot repos and snapshots')
    parser.add_argument('-D', '--del', dest='del_name', help='Delete snapshot of name SNAPSHOT', metavar='SNAPSHOT',
                        default=argparse.SUPPRESS)
    args = parser.parse_args()

    if hasattr(args, 'del_name'):
        snapshot_list = list_repo_snapshot(es)
        for item in snapshot_list['backup']['snapshots']:
            if item.split()[0] == args.del_name:
                es.snapshot.delete(repository='backup', snapshot=args.del_name)
                break
    else:
        if args.flag_list:
            list_repo_snapshot(es)
        else:
            snapshot(es)
