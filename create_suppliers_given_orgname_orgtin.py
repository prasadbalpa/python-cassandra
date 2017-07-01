from cassandra.cluster import Cluster
import sys
import logging
import traceback
import uuid
import datetime
import time
cluster = Cluster(['iaccounts-cassandra-1'])
session = cluster.connect('organization')

try:
   print "***************************************"
   print "Check if the organization exists with the TIN provided...."
   buffer = 'select org_id from org where org_name=' + '\'' + sys.argv[1] + '\'' + ' and org_tin=' + '\'' + sys.argv[2] + '\''
   print buffer
   rows = session.execute(buffer)
   print rows
   if rows:
      print rows[0].org_id
      uuidbuff = str(rows[0].org_id)
      newuuid = ''
      for i in uuidbuff:
         if i == '-':
           newuuid = newuuid + '_'
         else:
           newuuid = newuuid + i
      print newuuid
   else:
      print "***************************************"
      print "Failed to fetch the organization....Terminating...."
      sys.exit()

   session.set_keyspace('org_' + newuuid)
   filename = sys.argv[3]

   fo = open(filename, "r")
   for line in fo:
      print line
      templine = line.rstrip('\n')
      values = templine.split("|")
      ts = time.time()
      timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
      buffer = 'insert into supplierbyname (supplier_id, supplier_name, supplier_email, supplier_city, supplier_tin, supplier_phone) values (' + str(uuid.uuid4()) + ',' + '\'' + values[0] + '\',' + '\'' + values[1] + '\',' + '\'' + values[2] + '\',' +  '\'' +  values[3] + '\',' + '\'' + values[4] + '\'' + ')'
      session.execute(buffer)

   print "***************************************"
   print "Insertion of the customers is completed Successfully...."
   fo.close()
except Exception as e:
   logging.error(traceback.format_exc())
   print "Failed to add the customers....."

