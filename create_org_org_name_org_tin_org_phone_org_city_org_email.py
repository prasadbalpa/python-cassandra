from cassandra.cluster import Cluster
import sys
import logging
import traceback
import uuid
import datetime 
import time
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('organization')

#create.py $org_name $org_tin $org_phone $org_city $org_created

try:
   print "***************************************"
   print "Adding the organization entry...."
   ts = time.time()
   timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')   
   uui = uuid.uuid4()
   buffer = 'insert into org(org_id, org_email, org_city, org_created, org_name, org_phone, org_tin) values (' + str(uui) + ','  + '\'' + sys.argv[5] + '\'' + ',' +  '\'' + sys.argv[4] + '\'' + ',' + '\'' +  timestamp + '\'' +  ',' + '\'' + sys.argv[1] + '\'' +  ',' + '\'' + sys.argv[3] + '\'' +  ',' + '\'' + sys.argv[2] + '\'' +  ')'
   print buffer
   session.execute(buffer)
   print "organization entry added successfully..."
except Exception as e:
   logging.error(traceback.format_exc())
   print "Failed to add organization entry...Terminating..."
   sys.exit()

try:
   print "***************************************"
   print "Creating the organization specific keyspace...."
   uuidbuff = str(uui) 
   newuuid = ''
   for i in uuidbuff:
      if i == '-':
         newuuid = newuuid + '_'
      else:
         newuuid = newuuid + i
   print newuuid 
   buffer = 'create keyspace org_' + newuuid + ' with replication= {\'class\':\'SimpleStrategy\', \'replication_factor\': 1}' 
   print buffer
   session.execute(buffer)
   print "organization specific keyspace created Successfully..."
   print "***************************************"
except Exception as e:
   logging.error(traceback.format_exc())
   print "Failed to add organization specific keyspace"
   sys.exit()

try:
   print "***************************************"
   print "Creating customers table for the organization....."
   newcluster = 'org_' + newuuid
   print(newcluster)
   session.set_keyspace(newcluster)
   buffer = 'create table customerbytin (customer_id uuid, customer_city text, customer_tin text, customer_name text, customer_created text, customer_email text, customer_phone text, PRIMARY KEY((customer_tin), customer_name)) with clustering order by (customer_name ASC)'
   print buffer
   session.execute(buffer)
   print "customers table created Successfully"
   print "***************************************"
   
   print "Creating customers table for the organization....."
   newcluster = 'org_' + newuuid
   print(newcluster)
   session.set_keyspace(newcluster)
   buffer = 'create table customerbyname (customer_id uuid, customer_city text, customer_tin text, customer_name text, customer_created text, customer_email text, customer_phone text, PRIMARY KEY((customer_name), customer_tin)) with clustering order by (customer_tin ASC)'
   print buffer
   session.execute(buffer)
   print "customers table created Successfully"
   print "***************************************"
   print "Creating customers table for the organization....."
   newcluster = 'org_' + newuuid
   print(newcluster)
   session.set_keyspace(newcluster)
   buffer = 'create table customerbyphone (customer_id uuid, customer_city text, customer_tin text, customer_name text, customer_created text, customer_email text, customer_phone text, PRIMARY KEY((customer_phone), customer_name, customer_tin)) with clustering order by (customer_name ASC, customer_tin ASC)'

   print buffer
   session.execute(buffer)
   print "customers table created Successfully"
   print "***************************************"

   print "Creating vehicles table for the organization....."
   buffer = 'create table deliveryvehicles(vehicle_id uuid PRIMARY KEY, vehicle_number text)'
   print buffer
   session.execute(buffer)
   print "vehicles table added successfully...."
   print "***************************************"

   print "Creating products table for the organization..."
   buffer = 'create table products(product_id uuid PRIMARY KEY, product_name text, product_price decimal, product_discount decimal)'
   print buffer
   session.execute(buffer)
   print "products table created Successfully..."
   print "***************************************"
   
   print "Creating deliverylog table for the organization...."
   buffer = 'create table deliverylogbycustomer (delivery_id uuid, delivery_customer text, delivery_timestamp text, delivery_quantity int, delivery_vehicle text, PRIMARY KEY((delivery_customer), delivery_id, delivery_timestamp)) with clustering order by (delivery_id ASC, delivery_timestamp DESC)'
   print buffer
   session.execute(buffer)
   print "deliverylog table created Successfully...."
   print "***************************************"
   
   print "Creating deliverylog table for the organization...."
   buffer = 'create table deliverylogbytimestamp (delivery_id uuid, delivery_customer text, delivery_timestamp text, delivery_quantity int, delivery_vehicle text, PRIMARY KEY((delivery_timestamp), delivery_id, delivery_customer)) with clustering order by (delivery_id ASC, delivery_customer ASC)'
   print buffer
   session.execute(buffer)
   print "deliverylog table created Successfully...."
   print "***************************************"
   print "Creating deliverylog table for the organization...."
   buffer = 'create table deliverylogbytimestampcustomer (delivery_id uuid, delivery_customer text, delivery_timestamp text, delivery_quantity int, delivery_vehicle text, PRIMARY KEY((delivery_timestamp, delivery_customer), delivery_id)) with clustering order by (delivery_id ASC)'
   print buffer
   session.execute(buffer)
   print "deliverylog table created Successfully...."
   print "***************************************"

   print "Creating table users for the organization....."
   buffer = 'create table users(user_id uuid, user_userphone text PRIMARY KEY, user_passwd text, user_username text, user_role text)'
   print buffer
   session.execute(buffer)
   print "users table created Successfully..."
   print "***************************************"
except Exception as e:
   logging.error(traceback.format_exc())
   print "Failed to configure the tables for the organization...."
