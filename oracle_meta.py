# Untested and not migrated to Oracle yet.
#
#
# The idea with this is pretty much that you can do a quick meta-info grab of a postgres database, then do a change or a different load, 
# or a restore from backup and get a reasonable compare. You can throw the outputs into a git diff or a traditional diff and make sense of what has 
# changed or is different.

#/usr/bin/python3
#
#
import sys
import cx_Oracle

# Try to parse

# print(len(sys.argv))


if len(sys.argv[1:]) == 0:
    print("Trying defaults. (No connection attributes specified.)")
    try:
        dsn_tns = cx_Oracle.makedsn('Host Name', 'Port Number', service_name='Service Name') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
        conn = cx_Oracle.connect(user=r'demo', password='', dsn=dsn_tns) # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    except:
        print("I am unable to connect to the database.")
        exit(-1)

elif len(sys.argv[1:]) == 1:
    print("Only one argument - assuming full connection string.")
    #try:
    #    conn=psycopg2.connect(sys.argv[1])
    #    dsn_tns = cx_Oracle.makedsn('Host Name', 'Port Number', service_name='Service Name') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    #    conn = cx_Oracle.connect(user=r'demo', password='', dsn=dsn_tns) # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'        
    #except:
    print("I am unable to connect to the database.")
    exit(-2)

else:
    print("Many args - attempting parse.")
    #conn_string = ""
    #while True:
    #    temp = sys.argv.pop()
    #    temp2 = temp.split("=")
    #    if temp2[0] in ('database','dbname','user','password','host','port'):
    #        conn_string = conn_string + temp + " "
    #    if len(sys.argv)==0:
    #        break;
    #print(conn_string)
    #try:
    #    conn=psycopg2.connect(conn_string)
    #except:
    print("I am unable to connect to the database.")
    exit(-3)

# dbname – the database name (database is a deprecated alias)
# user – user name used to authenticate
# password – password used to authenticate
# host – database host address (defaults to UNIX socket if not provided)
# port – connection port number (defaults to 5432 if not provided)


# Try to connect

try:
    conn=psycopg2.connect("dbname='demo' user='postgres' password=''")
except:
    print("I am unable to connect to the database.")
    
cur = conn.cursor()
try:
    cur.execute("""SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'; """)
except:
    print("Can't get table list for db.")
    exit()

rs = cur.fetchall()
#print(rs)
for val in rs:
    print(val[1]) 
    try:
        cur.execute("Select * FROM " + val[1]  + " LIMIT 0")
        colnames = [desc[0] for desc in cur.description]
        print(str(val[0]) + "." + str(val[1]) + ".fields" + "=" + str(colnames))
        cur.execute("Select count(*) count FROM " + val[1]) 
        counts = cur.fetchone()
        print(val[0] + "." + str(val[1]) + ".rowcount" + "=" + str(counts[0]))

        try:
            cur.execute("SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = '" + str(val[1])  + "'::regclass AND    i.indisprimary;")
            index_name = cur.fetchone()
        except:
            index_name = ("0",)
            conn.rollback()

        print(val[0] + "." + str(val[1]) + ".index_name" + "=" + index_name[0])

        try:
            cur.execute("SELECT md5(CAST((array_agg(f.* order by "+ index_name[0] +"))AS text)) FROM "+ val[1]  +" f; ")
            table_hash = cur.fetchone()
        except:
            table_hash = ("N/A",)
            conn.rollback()
        print(val[0] + "." + str(val[1]) + ".md5_hash" + "=" + str(table_hash[0]))


        try:
            cur.execute("select min("+ str(index_name[0])  +") from " + str(val[1]) )
            min_id = cur.fetchone()
        except:
            min_id = ("N/A",)
            conn.rollback()

        try:
            cur.execute("select max("+ str(index_name[0])  +") from " + str(val[1]) )
            max_id = cur.fetchone()
        except:
            max_id = ("N/A",)
            conn.rollback()

        try:
            cur.execute("select cast(avg("+ str(index_name[0])  +")::numeric(10,2) as varchar)  from " + str(val[1]) )
            avg_id = cur.fetchone()
        except:
            avg_id = ("N/A",)
            conn.rollback()

        try:
            cur.execute("select cast(stddev("+ str(index_name[0])  +")::numeric(10,2) as varchar)  from " + str(val[1]) )
            stddev_id = cur.fetchone()
        except:
            stddev_id = ("N/A",)
            conn.rollback()

        print(val[0] + "." + str(val[1]) + ".min_id" + "=" + str(min_id[0]))
        print(val[0] + "." + str(val[1]) + ".max_id" + "=" + str(max_id[0]))
        print(val[0] + "." + str(val[1]) + ".avg_id" + "=" + str(avg_id[0]))
        print(val[0] + "." + str(val[1]) + ".stddev_id" + "=" + str(stddev_id[0]))

        for col in colnames:
            try:
                cur.execute("select count(*) from (select "+ str(col) +" from "+ str(val[1]) +" group by "+ str(col) +") AS A;")
                cardinality = cur.fetchone()
                print(val[0] + "." + str(val[1]) + "."+ str(col)  +".cardinality" + "=" + str(cardinality[0]))
            except:
                print(val[0] + "." + str(val[1]) + "."+ str(col)  +".cardinality" + "=N/A")
                conn.rollback()

            try:
                cur.execute("select max(length(cast("+ col +" as varchar))) from "+ str(val[1]) )
                length_max = cur.fetchone()
                print(val[0] + "." + str(val[1]) + "."+ str(col)  +".length.max" + "=" + str(length_max[0]))
            except:
                print(val[0] + "." + str(val[1]) + "."+ str(col)  +".length.max" + "=N/A")
                conn.rollback()

            try:
                cur.execute("select avg(length(cast("+ col +" as varchar))) from "+ str(val[1]) )
                length_avg = cur.fetchone()
                print(val[0] + "." + str(val[1]) + "."+ str(col)  +".length.avg" + "=" + str(length_avg[0]))
            except:
                print(val[0] + "." + str(val[1]) + "."+ str(col)  +".length.avg" + "=N/A")
                conn.rollback()

            try:
                cur.execute("select stddev(length(cast("+ col +" as varchar))) from "+ str(val[1]) )
                length_stddev = cur.fetchone()
                print(val[0] + "." + str(val[1]) + "."+ str(col)  +".length.stddev" + "=" + str(length_stddev[0]))
            except:
                print(val[0] + "." + str(val[1]) + "."+ str(col)  +".length.stddev" + "=N/A")
                conn.rollback()

            try:
                cur.execute("select min(length(cast("+ col +" as varchar))) from "+ str(val[1]) )
                length_min = cur.fetchone()
                print(val[0] + "." + str(val[1]) + "."+ str(col)  +".length.min" + "=" + str(length_min[0]))
            except:
                print(val[0] + "." + str(val[1]) + "."+ str(col)  +".length.min" + "=N/A")
                conn.rollback()

    except:
        print("Can't get field list for a table or other query failed after. ("+ val[1]  +")")
        conn.rollback()
