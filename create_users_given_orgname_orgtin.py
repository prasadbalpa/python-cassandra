from cassandra.cluster import Cluster
import sys
import logging
import traceback
import uuid
import datetime 
import time
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('organization')
usermapsession = cluster.connect('orgusermap')
#$org_name $org_phone

try:
   buffer = 'select org_id, org_name, org_phone from org where org_name=' + '\'' + sys.argv[1] + '\'' + ' and org_tin=' + '\'' + sys.argv[2] + '\''
   print buffer
   rows = session.execute(buffer)
   print rows[0].org_id
   print rows[0].org_name
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
      values = templine.split("|")
      ts = time.time()
      timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
      buffer = 'insert into users (user_id, user_username, user_userphone, user_passwd, user_role) values (' + str(uuid.uuid4()) + ',' + '\'' + values[0] + '\',' + '\'' + values[1] + '\'' + ',' + '\'' + '' + '\'' + ',' + '\'' + values[2] + '\''  + ')' 
      print buffer
      session.execute(buffer)
      #*************Update the user org map here*************
      buffer = 'insert into usermapbyphone (usermap_id, usermap_orgname, usermap_username, usermap_userphone) values (' + str(uuid.uuid4()) + ',\'' + rows[0].org_name + '\',' + '\'' + values[0] + '\',' + '\'' + values[1] + '\')' 
      print buffer
      usermapsession.execute(buffer)
   fo.close()  
except Exception as e:
   logging.error(traceback.format_exc())
   print "Failed to add the org"
