#Test program to become familiar with Impyla Libraries and connecting to Impala from python.
#Dependencies: impyla library from Cloudera (https://github.com/cloudera/impyla) and latest version of Thrift library (https://thrift.apache.org/)


try:
    from impala.dbapi import connect
    conn = connect(host='<HOST_NAME>', port=21050, timeout=3600)
    cursor = conn.cursor()

    cursor.execute("use default")
    #Clean-up
    sqlString = "DROP TABLE IF EXISTS test_table"
    cursor.execute(sqlString)
    print "Drop table"

    sqlString = "CREATE TABLE test_table LIKE <SOME_TABLE>"
    cursor.execute(sqlString)
    print "Create table"

    sqlString = "INSERT OVERWRITE test_table select * from <SOME_TABLE> limit 250"
    print "Load table"



except Exception as e:
    print e
    pass

finally:
    conn.close()
