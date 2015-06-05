# ETL operations against MYSQL db using mypy library
# Abhi Basu 

#!/usr/bin/python
import time
import subprocess
import sys
import pymysql.cursors

# Connect to the database
connection = pymysql.connect(host='10.4.2.3', user='root', passwd='P@ssw0rd', db='ucscdata', charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        sql = "use ucscdata"
        cursor.execute(sql)

        sql = "SELECT distinct(TABLE_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = \'ucscdata\' AND DATA_TYPE = \'longblob\'"
        #print sql
        cursor.execute(sql)
        #cursor.fetchone()
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
            print sqlstep1
            sqlstep2 = " ------------" + "UPDATE TABLE " + myTable + " SET " + updateSql
            print sqlstep2
            sqlstep3 =  " ------------" + "ALTER TABLE " + myTable + " " + dropcolumnSql
            print sqlstep3

            #Commit transactions
            #connection.commit()



    # connection is not autocommit by default. So you must commit to save
    # your changes.
    #connection.commit()

finally:
    connection.close()



