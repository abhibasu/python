# ETL operations against MYSQL db using mypy library
# Dependecies - mypy python library (https://github.com/PyMySQL/PyMySQL/releases)
# The code shows how to deal with longblob column types in mysql, especially if you are planning 
# to do a sqoop import of mysql tables to hive.
# Abhi Basu 

#!/usr/bin/python
import time
import subprocess
import sys
import pymysql.cursors

# Connect to the database
connection = pymysql.connect(host='<IP ADDRESS>', user='<USER>', passwd='<PASSWORD>', db='MYSQL <DB NAME>', charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        sql = "use ucscdata"  -- change it to your db name
        cursor.execute(sql)

        sql = "SELECT distinct(TABLE_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = \'ucscdata\' AND DATA_TYPE = \'longblob\'"
        #print sql
        cursor.execute(sql)

        resultTables = cursor.fetchall()

        for row in resultTables:
            print row['TABLE_NAME']
            myTable = row['TABLE_NAME']

            # # select column_name from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='xenoRefSeqAli'  AND TABLE_SCHEMA = 'ucscdata' and DATA_TYPE = 'longblob';
            sql = "SELECT DISTINCT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME= \'" + myTable + "\' AND TABLE_SCHEMA = \'ucscdata\' AND DATA_TYPE = \'longblob\'"
            #print sql
            cursor.execute(sql)
            resultColumns = cursor.fetchall()

            addcolumnSql = ""
            updateSql = ""
            dropcolumnSql = ""

            #Goal here is to create a new TEXT column, convert contents of LONGBLOB column, save in new column
            #and delete old column. Our intention was to import these mysql tables into Hive using Sqoop. Sqoop does 
            #not like to process LONGBLOB column types.
            for row in resultColumns:
                print " ------ " + row['COLUMN_NAME']
                mycolumn = row['COLUMN_NAME']
                addcolumnSql = addcolumnSql + " ADD COLUMN " + mycolumn + "_new as TEXT,"
                updateSql = updateSql + " " + mycolumn + "_new = CONVERT(" + mycolumn + " USING utf8),"
                dropcolumnSql = dropcolumnSql + " DROP COLUMN " + mycolumn + ","

            addcolumnSql = addcolumnSql[:-1]
            updateSql = updateSql[:-1]
            dropcolumnSql  = dropcolumnSql[:-1]

            sqlstep1 =  " ------------" + "ALTER TABLE " + myTable + addcolumnSql
            cursor.execute(sqlstep1)
            sqlstep2 = " ------------" + "UPDATE TABLE " + myTable + " SET " + updateSql
            cursor.execute(sqlstep2)
            sqlstep3 =  " ------------" + "ALTER TABLE " + myTable + " " + dropcolumnSql
            cursor.execute(sqlstep3)

            #Commit transactions
            connection.commit()



finally:
    connection.close()



