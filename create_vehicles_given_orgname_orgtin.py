from cassandra.cluster import Cluster
import sys
import logging
import traceback
import uuid
import datetime 
import time
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('organization')

#$org_name $org_phone

try:
   buffer = 'select org_id from org where org_name=' + '\'' + sys.argv[1] + '\'' + ' and org_tin=' + '\'' + sys.argv[2] + '\''
   print buffer
   rows = session.execute(buffer)
   print rows[0].org_id
   uuidbuff = str(rows[0].org_id)
   newuuid = ''
   for i in uuidbuff:
      if i == '-':
         newuuid = newuuid + '_'
      else:
         newuuid = newuuid + i
   print newuuid
   session.set_keyspace('org_' + newuuid)

   filename = sys.argv[3]

   fo = open(filename, "r")
   for line in fo:
      print line
      templine = line.rstrip('\n')
      ts = time.time()
      timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
      buffer = 'insert into deliveryvehicles (vehicle_id, vehicle_number) values (' + str(uuid.uuid4()) + ',' + '\'' + templine + '\'' +  ')' 
      session.execute(buffer)
   fo.close()  
except Exception as e:
   logging.error(traceback.format_exc())
   print "Failed to add the org"

