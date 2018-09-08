#!/usr/bin/env python
# This script is used for data migration from InfluxDB v<0.9 to 0.9 and afterward.
# The primary operation is pulling data in old series format and pushing it in new measurement.<tag> format.


import pycurl
import json
from StringIO import StringIO

# Grab database/shard grp info via influx restapi
buffer = StringIO()
c = pycurl.Curl()
INFLUXDB_URL = 'http://root:root@influxtest.anim.odw.com.cn:8086/cluster/shard_spaces'
INFLUXDB_EXPORT_BASEURL = 'http://root:root@influxtest.anim.odw.com.cn:8086/export/'
c.setopt(c.URL, INFLUXDB_URL)
c.setopt(c.WRITEFUNCTION, buffer.write)
c.perform()
c.close()
result = buffer.getvalue()
result_json = json.loads(result)
dst_path = '/opt/tempinfluxdbdata/'
for record in result_json:
    db_name = str(record['database'])
    shard_name = str(record['name'])
    c = pycurl.Curl()
    c.setopt(c.URL, INFLUXDB_EXPORT_BASEURL+db_name+'/'+shard_name)
    file_handle = open(dst_path+'export_'+db_name+'_'+shard_name, 'wb')
    c.setopt(c.WRITEFUNCTION, file_handle.write)
    c.perform()
    c.close()
    file_handle.close()
