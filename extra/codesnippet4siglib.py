
con = None

try:

    con = psycopg2.connect(database='testdb', user='janbodnar') 
    cur = con.cursor()
    cur.execute('SELECT version()')          
    ver = cur.fetchone()
    print ver    


except psycopg2.DatabaseError, e:
    print 'Error %s' % e    
    sys.exit(1)


finally:

    if con:
        con.close()