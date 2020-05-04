from flask import Flask, render_template, url_for,jsonify, request
import pymysql,xlrd
#Define global variables
update_file = False
init_table = False

app = Flask(__name__)
def create_database(hname,uid,pwd,db):
    mydb = pymysql.connect( host = hname,  user =uid,  passwd=pwd) 
    cursor = mydb.cursor()
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        q = "CREATE DATABASE IF NOT EXISTS "+db
        cursor.execute(q)
    cursor.close()
    mydb.commit()
    mydb.close()

def create_table(hname,uid,pwd,db,tb):
    mydb = pymysql.connect( host = hname ,  user =uid ,  passwd = pwd , db = db)  
    cursor = mydb.cursor() 
    stmt = "SHOW TABLES LIKE '"+tb+"'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    if result:
        ret =  "Table named " + tb + " already exists!"
    else:
        q = """CREATE TABLE IF NOT EXISTS """+ tb+"""(
       Name varchar(32) NOT NULL,
       Intro varchar(255) NOT NULL,
        Location varchar(32) NOT NULL,
        Job varchar(255) NOT NULL,
        About varchar(5000) NOT NULL,
        Education varchar(5000) NOT NULL,
        Skills varchar(5000) NOT NULL,
        URL varchar(255) NOT NULL
    )"""
        cursor.execute(q)
        ret =  "Table named " + tb + " does not exist! Table Created..."
    cursor.close()
    mydb.commit()
    mydb.close()
    return ret

def initialize_table(hname,uid,pwd,db,tb,myfile):
    #Initialize the Table
    mydb = pymysql.connect( host = hname ,  user =uid ,  passwd = pwd , db = db) 
    mydb.autocommit(True)
    cursor = mydb.cursor() 
    global init_table,update_file
    if((update_file == True) | (init_table == False)):
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            q = "DROP TABLE IF EXISTS "+tb
            cursor.execute(q)
        
        q = """CREATE TABLE IF NOT EXISTS """+ tb+"""(
       Name varchar(32) NOT NULL,
       Intro varchar(255) NOT NULL,
        Location varchar(32) NOT NULL,
        Job varchar(255) NOT NULL,
        About varchar(5000) NOT NULL,
        Education varchar(5000) NOT NULL,
        Skills varchar(5000) NOT NULL,
        URL varchar(255) NOT NULL
    )"""
        cursor.execute(q)
    
        xl_data = xlrd.open_workbook(file_contents=myfile.read())
        sheet = xl_data.sheet_by_index(0)
        query = """INSERT INTO """+tb+""" (Name, Intro, Location, Job, About, Education, Skills, URL)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """                    
        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        for i in range(1,sheet.nrows):
            Name = sheet.cell(i,0).value
            Intro = sheet.cell(i,1).value
            Location = sheet.cell(i,2).value
            Job = sheet.cell(i,3).value
            About = sheet.cell(i,4).value
            Education = sheet.cell(i,5).value
            Skills = sheet.cell(i,6).value
            URL = sheet.cell(i,7).value
            values = (Name,Intro,Location,Job,About,Education,Skills,URL)
            cursor.execute(query,values)

        init_table = True
        ret =  "Table Initialized!"
    else:
        ret = 'Table Already Initialized!'
    cursor.close()
    mydb.commit()
    mydb.close()
    return ret


def search(hname,uid,pwd,db,tb,t): 
    from tabulate import tabulate
    mydb = pymysql.connect( host = hname ,  user =uid ,  passwd = pwd , db = db)  

    cursor = mydb.cursor() 
    e = tb
    #t = str(input("Enter a Text Query:"))
    f_at = t.find(' at ')
    f_in = t.find(' in ')

    if((f_at == -1) & (f_in==-1)):
        q = """SELECT * FROM """+e+""" WHERE Location LIKE "%"""+t+"""%"
        OR Job LIKE "%"""+t+"""%"
        OR Skills LIKE "%"""+t+"""%" """
        cursor.execute(q)
        res = cursor.fetchall()
        if(len(res) == 0):
            cursor.close()
            quer = """ALTER TABLE """+e+""" ADD FULLTEXT(Location, Job, Skills)"""
            query = """SELECT * FROM """+e+""" WHERE MATCH(Location, Job, Skills)
                    AGAINST("""+"\'"+t+"\'"+""" IN BOOLEAN MODE)"""
            cursor = mydb.cursor() 
            cursor.execute(quer)
            cursor.execute(query)
            res = cursor.fetchall()

    elif((f_at != -1) & (f_in==-1)):
        before_at = t[0:f_at]
        after_at = t[f_at+4:]
        #print(before_at,after_at)
        q = """SELECT * FROM """+e+""" WHERE ((Job LIKE "%"""+t+"""%")
       OR ((Job LIKE "%"""+before_at+"""%"
       OR Skills LIKE "%"""+before_at+"""%")
       AND Location LIKE "%"""+after_at+"""%"))"""
        cursor.execute(q)
        res = cursor.fetchall()
        if(len(res) == 0):
            cursor.close()
            quer = """ALTER TABLE """+e+""" ADD FULLTEXT(Location, Job, Skills)"""
            query = """SELECT * FROM """+e+""" WHERE MATCH(Location, Job, Skills)
                    AGAINST("""+"\'"+t+"\'"+""" IN BOOLEAN MODE)"""
            cursor = mydb.cursor() 
            cursor.execute(quer)
            cursor.execute(query)
            res = cursor.fetchall()


    elif((f_at == -1) & (f_in!=-1)):
        before_in = t[0:f_in]
        after_in = t[f_in+4:]
        q = """SELECT * FROM """+e+""" WHERE ((Location LIKE '%"""+after_in+"""%')
       AND ((Job LIKE '%"""+before_in+"""%'
       OR Skills LIKE '%"""+before_in+"""%')))"""
        cursor.execute(q)
        res = cursor.fetchall()
        if(len(res) == 0):
            cursor.close()
            quer = """ALTER TABLE """+e+""" ADD FULLTEXT(Location, Job, Skills)"""
            query = """SELECT * FROM """+e+""" WHERE MATCH(Location, Job, Skills)
                    AGAINST("""+"\'"+t+"\'"+""" IN BOOLEAN MODE)"""
            cursor = mydb.cursor() 
            cursor.execute(quer)
            cursor.execute(query)
            res = cursor.fetchall()


    else:
        between_at_in = t[f_at+4:f_in]
        after_in = t[f_in+4:]
        before_at = t[0:f_at]
        before_in = t[0:f_in]
        q = """SELECT * FROM """+e+""" WHERE (Location LIKE '%"""+after_in+"""%'
       AND ((Job LIKE '%"""+before_in+"""%'
       OR Skills LIKE '%"""+before_at+"""%')))"""
        cursor.execute(q)
        res = cursor.fetchall()
        if(len(res) == 0):
            cursor.close()
            quer = """ALTER TABLE """+e+""" ADD FULLTEXT(Location, Job, Skills)"""
            query = """SELECT * FROM """+e+""" WHERE MATCH(Location, Job, Skills)
                    AGAINST("""+"\'"+t+"\'"+""" IN BOOLEAN MODE)"""
            cursor = mydb.cursor() 
            cursor.execute(quer)
            cursor.execute(query)
            res = cursor.fetchall()

    res = list(res)
    
    for i in range(len(res)):
        res[i] = list(res[i])
        del res[i][4 : 7]


    for r in res:
        r[1] = r[1][0:32] + "[...]"
        r[2] = r[2][0:32] + "[...]"
        r[3] = r[3][8:40] + "[...]"
    return res

def refresh(hname,uid,pwd,db,tb):
    mydb = pymysql.connect( host = hname ,  user = uid ,  passwd = pwd, db = db) 
    cursor = mydb.cursor()
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        q = "DROP TABLE IF EXISTS "+tb
        cursor.execute(q)
        global init_table
        init_table = False
    cursor.close()
    mydb.commit()
    mydb.close()
    return 'Table Deleted if exists. Create the Table again!'

@app.route('/')
def login_forms():
    return render_template('login_forms.html')

@app.route('/', methods=['POST','GET'])
def login():
    global init_table,update_file
    if(request.method=="POST"):
        hname = request.form['hname']
        uid = request.form['uid']
        pwd = request.form['pwd']
        db = request.form['db']
        tb = request.form['tb']
        myfile = request.files['myfile']
        t = request.form["text"]
        if(request.form["b"]=="Create Database"):
            create_database(hname,uid,pwd,db)
            return "Database Created if not exists"
        if(request.form["b"]=="Create Table"):
            s = create_table(hname,uid,pwd,db,tb)
            return s
        if(request.form["b"]=="Initialize Table"):
            if(request.form["checkbox"] == "checked"):
                update_file=True
            else:
                update_file=False
            s1 = initialize_table(hname,uid,pwd,db,tb,myfile)
            return s1
        if(request.form["b"]=="Search"):
            res = search(hname,uid,pwd,db,tb,t)
            return render_template('index.html',result = res)
        if(request.form["b"]=="Delete Table"):
            s = refresh(hname,uid,pwd,db,tb)
            return s

if __name__ == "__main__":
    app.run(debug=True)
