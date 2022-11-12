import pymysql
from sshtunnel import SSHTunnelForwarder

tabl='dbox_bec' 
tabl2='token_db' 
tabl3='dbox_stat' 

server = SSHTunnelForwarder(
    ('149.248.8.216', 22),
    ssh_username='root',
    ssh_password='XUVLWMX5TEGDCHDU',
    remote_bind_address=('127.0.0.1', 3306)
)

# Cоздаем подключение !  
def _getConnection(): 
    server.start()
    # Вы можете изменить параметры соединения.
    connection = pymysql.connect(host='127.0.0.1', port=server.local_bind_port, user='chai_cred',
                      password='Q12w3e4003r!', database='credentals',
                      cursorclass=pymysql.cursors.DictCursor)
    return connection


# Cоздаем базу данных ! 
def create_table2():
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {tabl2} (
                                    id int PRIMARY KEY AUTO_INCREMENT,
                                    dbox_token TEXT ,
                                    status TEXT DEFAULT ("False") )""")

def create_table_stat():
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {tabl3} (
                                    id int PRIMARY KEY AUTO_INCREMENT,
                                    ip_a VARCHAR(40) NOT NULL UNIQUE,
                                    data TEXT DEFAULT ("None"))""")



# Cоздаем базу данных ! 
def create_table():
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {tabl} (
                                    id int PRIMARY KEY AUTO_INCREMENT,
                                    drive TEXT ,
                                    folder_name TEXT,
                                    name TEXT   ,
                                    id_files TEXT  ,
                                    json TEXT  ,
                                    dbox_token TEXT ,
                                    status TEXT DEFAULT ("False") )""")


# Добавим значения в базу !
def add(drive,folder_name,name,files,json,dbox_token:str):
    dbox_token = dbox_token.replace("\"","#")
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute(f"""INSERT INTO {tabl} (drive,folder_name,name,id_files,json,`dbox_token`) VALUES 
                                             ("{drive}","{folder_name}","{name}","{files}","{json}","{dbox_token}")""")
        db.commit()

def add_stat(ip_a,data:str):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute(f"""INSERT IGNORE INTO {tabl3} (ip_a,data) VALUES 
                                               ("{ip_a}","{data}")""")
        db.commit()


def sets_stat(ip_a,data:str):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute('UPDATE %s SET data = "%s" WHERE ip_a = "%s"' %(tabl3,data,ip_a,))
        db.commit()
        print("OK")
        
# Получить все данные 
def get_all():
    with _getConnection() as db:
        cursor = db.cursor()
        #cur.execute( f"SELECT * FROM {tabl} WHERE status = 'False' " ) # запросим все данные  
        cursor.execute( f"SELECT * FROM {tabl}" ) # запросим все данные  
        str_ok = cursor.fetchall()
        for str_okv in str_ok:
           print(str_okv)
        db.commit()

def get_all2():
    with _getConnection() as db:
        cursor = db.cursor()
        #cur.execute( f"SELECT * FROM {tabl} WHERE status = 'False' " ) # запросим все данные  
        cursor.execute( f"SELECT * FROM {tabl2}" ) # запросим все данные  
        str_ok = cursor.fetchall()
        for str_okv in str_ok:
           print(str_okv)
        db.commit()


# Получить один файл установить ВОРК
def get_one_false():
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute(f'SELECT * FROM {tabl} WHERE status = "False" ') 
        str_ok = cursor.fetchall()[0]
        #print(str_ok)
        db.commit()
        try:
            #print(str_ok[0][0])
            cursor.execute(f'UPDATE {tabl} SET status = "Work" WHERE id = "{str_ok["id"]}"')
            db.commit()
        except IndexError :
            return None
    return str_ok['id'],str_ok['drive'],str_ok['folder_name'],str_ok['name'],str_ok['id_files'],str_ok['json'],str_ok['dbox_token'].replace("#","\"")
    

# Установить True   
# Установить True   
# Установить True   
def sets_true(id_t):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute('UPDATE %s SET status = "%s" WHERE id = %s' %(tabl,"True",id_t,))
        db.commit()
        print("OK")


# Установить False
def sets_false(id_t):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute('UPDATE %s SET status = "%s" WHERE id = %s' %(tabl,"False",id_t))
        db.commit()
        print("OK")  

# Установить False        
def sets_false(id_t):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute('UPDATE %s SET status = "%s" WHERE id = %s' %(tabl,"False",id_t))
        db.commit()
        print("OK")  


# Получить данные  - Условие
def get_all_usl():
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute( f'SELECT * FROM {tabl} WHERE status = "Work"' ) # запросим все данные  
        print(len(cursor.fetchall()), ' - Work')        
        cursor.execute( f"SELECT * FROM {tabl} WHERE status = 'False' " ) # запросим все данные  
        print(len(cursor.fetchall()), ' - False')
        cursor.execute( f'SELECT * FROM {tabl} WHERE status = "True"' ) # запросим все данные  
        print(len(cursor.fetchall()), ' - True')
        cursor.execute( f'SELECT * FROM {tabl} WHERE status = "Error"' ) # запросим все данные  
        print(len(cursor.fetchall()), ' - Error')

        #str_ok = cursor.fetchall()
        #for str_okv in str_ok:
        #   print(str_okv)
        db.commit()
        #print(len(str_ok))

#print(get_one_false())
#sets_true(4200)
#sets_false(4200)
#get_all()
#get_all_usl()

# Получить все данные 
def get_all_token(id_t):
    with _getConnection() as db:
        dbox_token = id_t.replace("\"","#")[185:-50]
        #dbox_token = '{#access_token#:#sl.BRKQrcOeGs8crDdkP_WJVrFohfxgMPASBB4gj3jSOBBnE9mKTIMsqIQRPg4TYz_FBjINEMjn6il0DLcNPsw3JJ_3Ad2tIlNzTEo8DoLUM-S88uvtk2tALTQR8NXwdlwsr_k2KHsT#,#token_type#:#bearer#,#refresh_token#:#XX9NY4QFV2wAAAAAAAAAAV5YeIcdpVT3V-HfD4XT3qYrXBrPf10NS_0_dChY6VOt#,#expiry#:#2022-10-15T16:24:44.6999644+03:00#}'
        print(dbox_token)
        cursor = db.cursor()
        #cursor.execute( r"SELECT * FROM dbox_bec WHERE dbox_token LIKE '%access_token#:#sl.BRKQrcOeGs8crDdkP_WJVrFohfxgMPASBB4gj3jSOBBnE9mKTIMsqIQRPg4TYz_FBjINEMjn6il0%' " ) # запросим все данные  
        cursor.execute( f"SELECT id_files,name FROM dbox_bec WHERE dbox_token LIKE '%{dbox_token}%'" )
        #cursor.execute( "SELECT * FROM %s WHERE dbox_token = '%s'" % (tabl,dbox_token)) # запросим все данные  # запросим все данные  
        str_ok = cursor.fetchall()
        for str_okv in str_ok:
           print(str_okv)
        db.commit()

def get_all_token1():
    with _getConnection() as db:
        cursor = db.cursor()
        #cursor.execute( r"SELECT * FROM dbox_bec WHERE dbox_token LIKE '%access_token#:#sl.BRKQrcOeGs8crDdkP_WJVrFohfxgMPASBB4gj3jSOBBnE9mKTIMsqIQRPg4TYz_FBjINEMjn6il0%' " ) # запросим все данные  
        #cursor.execute( f"SELECT id_files,name FROM dbox_bec WHERE dbox_token LIKE '%{dbox_token}%'" )
        cursor.execute( f"SELECT DISTINCT dbox_token FROM dbox_bec " )
        #cursor.execute( "SELECT * FROM %s WHERE dbox_token = '%s'" % (tabl,dbox_token)) # запросим все данные  # запросим все данные  
        str_ok = cursor.fetchall()
        for str_okv in str_ok:
           print(str_okv)
        db.commit()

def get_one_false2():
    with _getConnection() as db:
        try:
            cursor = db.cursor()
            cursor.execute(f'SELECT * FROM {tabl} WHERE dbox_token = (SELECT dbox_token FROM token_db WHERE status = "False" LIMIT 1) and status = "False" ') 
            str_ok = cursor.fetchall()
            db.commit()
        
            #print(str_ok)
            name_list=[x['name'] for x in str_ok]
            id_list=[x['id_files'] for x in str_ok]
            #print(name_list)
            #print(id_list)
            cursor.execute(f'UPDATE {tabl2} SET status = "Work" WHERE dbox_token = "{str_ok[0]["dbox_token"]}"')
            db.commit()
        except IndexError :
            return None
    return str_ok[0]['id'],str_ok[0]['drive'],str_ok[0]['folder_name'],name_list,id_list,str_ok[0]['json'],str_ok[0]['dbox_token'].replace("#","\"")


#get_all_token1()
#create_table2()

#print(get_one_false2())
#get_all2()

# INSERT INTO token_db dbox_token SELECT DISTINCT dbox_token FROM dbox_bec;
#get_all_usl()
#create_table_stat()
