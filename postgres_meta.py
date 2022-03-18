# The idea with this is pretty much that you can do a quick meta-info grab of a postgres database, then do a change or a different load, 
# or a restore from backup and get a reasonable compare. You can throw the outputs into a git diff or a traditional diff and make sense of what has 
# changed or is different.

import psycopg2

try:
    conn=psycopg2.connect("dbname='' user='' password=''")
except:
    print("I am unable to connect to the database.")
    
cur = conn.cursor()
try:
    cur.execute("""SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'; """)
except:
    print("Can't get table list for db.")
    exit()

rs = cur.fetchall()
print(rs)
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

        print(val[0] + "." + str(val[1]) + ".min_id" + "=" + str(min_id[0]))
        print(val[0] + "." + str(val[1]) + ".max_id" + "=" + str(max_id[0]))
        print(val[0] + "." + str(val[1]) + ".avg_id" + "=" + str(avg_id[0]))

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
                cur.execute("select min(length(cast("+ col +" as varchar))) from "+ str(val[1]) )
                length_min = cur.fetchone()
                print(val[0] + "." + str(val[1]) + "."+ str(col)  +".length.min" + "=" + str(length_min[0]))
            except:
                print(val[0] + "." + str(val[1]) + "."+ str(col)  +".length.min" + "=N/A")
                conn.rollback()

    except:
        print("Can't get field list for a table or other query failed after. ("+ val[1]  +")")
        conn.rollback()
