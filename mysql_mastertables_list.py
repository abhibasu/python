# Run as premliminary step for importing data from mysql to hive metastore using scoop
# this code needs to be run to get the master list of tables to be moved to hive
# in case of failure due to network connection loss, we need master list of tables
# Dependecies - mypy python library
# Abhi Basu
# 08/20/15

#!/usr/bin/python
import time; 
import subprocess
import sys
import logging
import pymysql.cursors



# Connect to the mysql database
connection = pymysql.connect(host='<IP_ADRESS>', user='<USER>', passwd='<PASSWD>', db='<DBNAME>', charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

#Create a file with list of all table names that can be used in future when part of job fails
#Run this section of code the first time to get master list
with connection.cursor() as cursor:
        sql = "use <DBNAME>"
        cursor.execute(sql)
        # Extract all tables that need to be moved to Hive metastore
        sql = "SELECT distinct(TABLE_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = \'<DBNAME>\'"
        cursor.execute(sql)
        resultTables = cursor.fetchall()
        f = open("mastertables.txt", 'w')

        for row in resultTables:
                f.write(row['TABLE_NAME'] + '\n')

        f.close()
connection.close()

print "Master table list generated successfully!"

