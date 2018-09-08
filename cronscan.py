#!/usr/bin/python
import os
import time
import socket
import json
import hashlib
import subprocess

# List cron files under /var/spool/cron
cronfile_list = os.listdir("/var/spool/cron")
ELK_HOST = 'http://elasticsearch:9200'
index = 'cronjob'

# Process each file
for cron in cronfile_list:
    common = {"user": cron, "timestamp": time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()), "host": socket.gethostname().split('.')[0]}
    cron_lines = open("/var/spool/cron/"+cron).read().split('\n')

    # Function: cronfile content parser
    def cron_parse(lines):
        cmd_list = list()
        for line in lines:
            if line.startswith('#') or not line:
                continue
            else:
                try:
                    json = dict()
                    tokens = line.split()
                    json['cmd'] = ' '.join(tokens[5:])
                    json['interval'] = ' '.join(tokens[:5])
                    cmd_list.append(json)
                except:
                    continue
        return cmd_list

    cmd_list = cron_parse(cron_lines)  # Parse cron file
    if not cmd_list:
        continue
    else:
        for cmd in cmd_list:
            json_data = dict(common)
            json_data.update(cmd)
            hasher = hashlib.md5()
            hasher.update(json_data['host'] + json_data['cmd'])
            id_number = hasher.hexdigest()
            print json_data
            command = ['/usr/bin/curl',
                       '-XPUT', '{host}/{index}/default/{id}'.format(host=ELK_HOST, index=index, id=id_number),
                       '-d', json.dumps(json_data)]
            p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
