# write and delete functions for the database
import psycopg2
import time
import datetime
def postgres_insert(msg,postgres_count,conn,cursor):
    s = msg['window']['end'].replace('T',' ').replace('Z','')[:-4]
    timestamp = time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timetuple())

    postgres_insert_query = """ INSERT INTO testtable4 (post, numcomments, timestamp) VALUES (%s,%s,%s)"""
    record_to_insert = (msg['post'],msg['count'],timestamp)
    cursor.execute(postgres_insert_query, record_to_insert)
    if postgres_count%10000==0:
        conn.commit()
    return
def postgres_delete(msg,conn,cursor):
    s = msg['window']['end'].replace('T',' ').replace('Z','')[:-4]
    ts = time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timetuple())
    sql_delete_query = """Delete from testtable4 where timestamp < %s"""
    cursor.execute(sql_delete_query, (ts, ))
    conn.commit()
    return
