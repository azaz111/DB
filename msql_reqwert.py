import pymysql
from sshtunnel import SSHTunnelForwarder

tabl='dbox_bec' 

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
    return str_ok['id'],str_ok['drive'],str_ok['folder_name'],str_ok['name'],str_ok['json'],str_ok['dbox_token'].replace("#","\"")


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


# Получить данные  - Условие
def get_all_usl():
    with _getConnection() as db:
        cursor = db.cursor()
        #cur.execute( f"SELECT * FROM {tabl} WHERE status = 'False' " ) # запросим все данные  
        cursor.execute( f'SELECT * FROM {tabl} WHERE status = "True"' ) # запросим все данные  
        str_ok = cursor.fetchall()
        for str_okv in str_ok:
           print(str_okv)
        db.commit()

#print(get_one_false())
#sets_true(4200)
#sets_false(4200)
#get_all_usl()