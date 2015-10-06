# ETL operations against MYSQL db using pymysql library (https://github.com/PyMySQL/PyMySQL/releases)
# to convert any columns with longblob to text data type
# so that Sqoop can be used (as next step) to import into Hive metastore.
# Abhi Basu
# 04/17/2015

#!/usr/bin/python
import time
import subprocess
import sys
import logging
import pymysql.cursors

reportName = 'mysql_etl_output.txt'
f = open(reportName, 'w')
f.write("Python MYSQL ETL Job Log " + '\n')
f.close()

# Connect to the database
connection = pymysql.connect(host='<IP_ADDRESS>', user='<USER>', passwd='<PASSWD>', db='<DBNAME>', charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


try:
    with connection.cursor() as cursor:
        sql = "use <DBNAME>"
        cursor.execute(sql)

        sql = "SELECT distinct(TABLE_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = \'<DBNAME>\' AND DATA_TYPE = \'longblob\' ORDER BY TABLE_NAME DESC"
        f = open(reportName, 'a')
        f.write(sql + "\n")
        f.close()
        cursor.execute(sql)

        resultTables = cursor.fetchall()

        for row in resultTables:
            print " ------ " + " ------ " + row['TABLE_NAME']
            f = open(reportName, 'a')
            f.write(" ------ " + " ------ " + row['TABLE_NAME'] + "\n")
            f.close()
            myTable = row['TABLE_NAME']

            sql = "SELECT DISTINCT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME= \'" + myTable + "\' AND TABLE_SCHEMA = \'<DBNAME>\' AND DATA_TYPE = \'longblob\'"
            #print sql
            f = open(reportName, 'a')
            f.write(sql + "\n")
            f.close()
            cursor.execute(sql)
            resultColumns = cursor.fetchall()

            addcolumnSql = ""
            updateSql = ""
            dropcolumnSql = ""
            rencolumnSql = ""

            for row in resultColumns:
                #print " ------ " + row['COLUMN_NAME']
                f = open(reportName, 'a')
                f.write(" ------ " + row['COLUMN_NAME'] + "\n")
                f.close()
                mycolumn = row['COLUMN_NAME']
                addcolumnSql = addcolumnSql + " ADD COLUMN " + mycolumn + "_new TEXT,"
                updateSql = updateSql + " " + mycolumn + "_new = CONVERT(" + mycolumn + " USING utf8),"
                dropcolumnSql = dropcolumnSql + " DROP COLUMN " + mycolumn + ","
                rencolumnSql = rencolumnSql + " CHANGE " + mycolumn + "_new " + mycolumn + " TEXT,"

            addcolumnSql = addcolumnSql[:-1]
            updateSql = updateSql[:-1]
            dropcolumnSql  = dropcolumnSql[:-1]
            rencolumnSql = rencolumnSql[:-1]

            sqlstep1 = "ALTER TABLE " + myTable + addcolumnSql
            f = open(reportName, 'a')
            f.write(sqlstep1 + "\n")
            f.close()
            cursor.execute(sqlstep1)
            connection.commit()

            sqlstep2 = "UPDATE " + myTable + " SET " + updateSql
            f = open(reportName, 'a')
            f.write(sqlstep2 + "\n")
            f.close()
            cursor.execute(sqlstep2)
            connection.commit()

            sqlstep3 = "ALTER TABLE " + myTable + " " + dropcolumnSql
            f = open(reportName, 'a')
            f.write(sqlstep3 + "\n")
            f.close()
            cursor.execute(sqlstep3)
            connection.commit()

            sqlstep4 = "ALTER TABLE " + myTable + rencolumnSql
            f = open(reportName, 'a')
            f.write(sqlstep4 + "\n")
            f.close()
            cursor.execute(sqlstep4)
            connection.commit()
except Exception as e:
            f = open(reportName, 'a')
            f.write("EXCEPTION" + "\n")
            f.write(str(e) + "\n")
            f.close()
            pass

finally:
    connection.close()


print " ----------- Tables Converted Successfully!  -----------"
f = open(reportName, 'a')
f.write(" ----------- Tables Converted Successfully!  -----------" + "\n")
f.close()
