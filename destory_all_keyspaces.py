from cassandra.cluster import Cluster
import sys
import logging
import traceback
import uuid
import datetime
import time
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

try:
   keyspaces = cluster.metadata.keyspaces.items()
   for keyspace in keyspaces:
       tempks = keyspace[0]
       if tempks[0:3] == 'org':
		buffer = 'drop keyspace if exists ' + tempks 
                print 'Dropping ' + buffer
                session.execute(buffer)
except Exception as e:
   logging.error(traceback.format_exc())
   print "Failed to add the org"
