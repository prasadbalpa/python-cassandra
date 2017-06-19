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
   print "***************************************"
   print "Creating the organization keyspace....."
   buffer = 'create keyspace organization with replication= {\'class\':\'SimpleStrategy\', \'replication_factor\': 1}'
   print buffer
   session.execute(buffer)
   print "organization keyspace created successfully..."
   print "***************************************"
except Exception as e:
   logging.error(traceback.format_exc())
   print "Failed to add the keyspace organization...."
   print "Terminating...."
   sys.exit()

try:
   print "***************************************"
   print "Creating the table org....."
   organization = 'organization'
   session.set_keyspace(organization)
   buffer = 'create table org (org_id uuid, org_city text, org_created timestamp, org_name text, org_tin text, org_email text, org_phone text, PRIMARY KEY(org_tin, org_name)) with clustering order by (org_name ASC)'
   print buffer
   session.execute(buffer)
   print "table org created successfully"
   print "***************************************"
   buffer = 'create table sessionbysessionid (session_id uuid, session_authorization text, session_otp text, session_sessionid uuid, session_created text, session_expired boolean, session_phone text, session_loggedin boolean, PRIMARY KEY(session_sessionid, session_created)) with clustering order by (session_created DESC)'
   print buffer
   session.execute(buffer)
   print "***************************************"
   buffer = 'create table sessionbyauthorization (session_id uuid, session_authorization text, session_otp text, session_sessionid uuid, session_created text, session_expired boolean, session_phone text, session_loggedin boolean, PRIMARY KEY(session_phone, session_created)) with clustering order by (session_created DESC)'
   print buffer
   session.execute(buffer)
   print "***************************************"
   buffer = 'create table sessionbysessionidphone (session_id uuid, session_authorization text, session_otp text, session_sessionid uuid, session_created text, session_expired boolean, session_phone text, session_loggedin boolean, PRIMARY KEY((session_sessionid, session_phone), session_id)) with clustering order by (session_id ASC)'
   print buffer
   session.execute(buffer)
   print "***************************************"
   buffer = 'create table sessionbyidnumberotp (session_id uuid, session_authorization text, session_otp text, session_sessionid uuid, session_created text, session_expired boolean, session_phone text, session_loggedin boolean, PRIMARY KEY((session_sessionid, session_phone, session_otp), session_id)) with clustering order by (session_id DESC)'
   print buffer
   session.execute(buffer)
   print "***************************************"
   buffer = 'create table sessionbyphone (session_id uuid, session_authorization text, session_otp text, session_sessionid uuid, session_created text, session_expired boolean, session_phone text, session_loggedin boolean, PRIMARY KEY(session_phone, session_id)) with clustering order by (session_id ASC)'
   print buffer
   session.execute(buffer)
   print "***************************************"
   buffer = 'create table rbac (rbac_id uuid, rbac_name text, rbac_map blob, PRIMARY KEY(rbac_name, rbac_map))'
   print buffer
   session.execute(buffer) 
   print "***************************************"

except Exception as e:
   logging.error(traceback.format_exc())
   print "Failed to create the tables under organization keyspace...Terminating..."
   sys.exit()

try:
   print "***************************************"
   print "Creating orgusermap keyspace..........."
   buffer = 'create keyspace orgusermap with replication= {\'class\':\'SimpleStrategy\', \'replication_factor\': 1}'
   print buffer
   session.execute(buffer)
   print "orgusermap keypsace created successfully..."
   print "***************************************"
except Exception as e:
   logging.error(traceback.format_exc())
   print "Failed to create orgusermap keyspace...Terminating..."
   sys.exit()

try:
   print "***************************************"
   print "Creating table usermap......"
   orgusermap = 'orgusermap'
   session.set_keyspace(orgusermap)
   buffer = 'create table usermapbyphone (usermap_id uuid, usermap_orgname text, usermap_orgtin text, usermap_username text, usermap_userphone text, PRIMARY KEY((usermap_userphone), usermap_id)) with clustering order by (usermap_id ASC)'
   print buffer
   session.execute(buffer)
   print "usermap table created Successfully...."
   print "***************************************"
except Exception as e:
   logging.error(traceback.format_exc())
   print "Failed to orusermap::tables......"
