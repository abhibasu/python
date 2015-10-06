# Run to import data from mysql to hive metastore using scoop
# Abhi Basu
# 08/24/15

#!/usr/bin/python
import time;
import subprocess



#BEFORE THIS CODE IS RUN, RUN mysql_master_table_list.py TO GENERATE THE MASTER LIST THIS CODE USES.
#IF FOR SOME REASON PART OF THE TABLES ARE EXPORTED BEFORE SOME CONNECTION ISSUE, MODIFY THE MASTER LIST TO
#DELETE THE TABLES EXPORTED OVER FOR NEXT TIME RUN.

reportName = 'mysql_scoop_import_hive_output_' + str(time.time()) + '.txt'
f = open(reportName, 'w')
f.write("MySQL to Hive ETL Script " + '\n')
f.close()


with open('ucsc_hg19_mastertables.txt', 'r') as masterTableList:
    mysqlTables = [line.strip() for line in masterTableList]

try:
    for item in mysqlTables:
        f = open(reportName, 'a')
        print " ------ " + " ------ " + item
        f.write(" ------ " + " ------ " + item + "\n")
        myTable = item


        #Sqoop command
        #sqoop import --connect jdbc:mysql://10.4.2.3:3306/ucscdata --username root --password P@ssw0rd --table xenoRefSeqAli
        # --hive-table hg19.xenoRefSeqAli --create-hive-table --hive-import --warehouse-dir /user/hive/warehouse/hg19.db -m 1 --direct
        command = "sqoop import --connect jdbc:mysql://<IP_ADDRESS>:3306/<DBNAME> --username <USER> --password <PASSWD> --table "  \
                  + myTable + " --hive-table <HIVEDBNAME>." + myTable + " --create-hive-table --hive-import --warehouse-dir" \
                                                                " /user/hive/warehouse/<HIVEDBNAME>.db -m 1 --direct"
        print "Processing - " + command
        f.write(command + "\n")
        f.close()
        subprocess.call(command, shell=True)

except Exception as e:
            f = open(reportName, 'a')
            f.write("EXCEPTION" + "\n")
            f.write(str(e) + "\n")
            f.close()
            pass


print " ----------- Tables Moved to Hive Metastore Successfully!  -----------"
f = open(reportName, 'a')
f.write(" ----------- Tables Moved to Hive Metastore Successfully!  -----------" + "\n")
f.close()
