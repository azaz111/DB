import pymysql
import time
import datetime
from sshtunnel import SSHTunnelForwarder
import os

tabl='token_transfer' 
tabl2='json_transfer' 
tabl3='id_drive_transfer' 


now=int(time.time())

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


def _create_table(tabl):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {tabl} (
                                    id int PRIMARY KEY AUTO_INCREMENT,
                                    json_name VARCHAR(110) NOT NULL UNIQUE,
                                    json_content TEXT ,
                                    status_work BOOLEAN DEFAULT (True),
                                    time_work INT DEFAULT (0)
                                    )""")
def _create_table_id(tabl):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {tabl} (
                                    id int PRIMARY KEY AUTO_INCREMENT,
                                    id_drive VARCHAR(110) NOT NULL UNIQUE,
                                    status_work BOOLEAN DEFAULT (FALSE),
                                    time_work INT DEFAULT (0)
                                    )""")
        



def add_opt(dop_bazu:list,tabl:str ='token_transfer' ):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.executemany(f"""INSERT IGNORE INTO {tabl} (json_name,json_content,status_work,time_work) VALUES (%s,%s,%s,%s) """ , dop_bazu)
        db.commit()
        print('OK')
        
def add_id(id_drive, tabl:str ='id_drive_transfer' ):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute(f"""INSERT IGNORE INTO {tabl} (id_drive,time_work) VALUES ('{id_drive}',{int(time.time())})""")
        db.commit()
        print('OK')


def set_false(set:str , where:str,tabl:str ='token_transfer' ):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute(f"""UPDATE {tabl} SET {set} WHERE {where} """ )
        db.commit()
        print('OK')


# Получить все данные 
def get_data(ust , where ,tabl:str ='token_transfer' ):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute( f"SELECT {ust} FROM {tabl} WHERE {where} " ) # запросим все данные  
        str_ok = cursor.fetchall()
    return str_ok

# Получить все данные 
def get_one_data(tabl:str ='token_transfer' ):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute("START TRANSACTION")
        cursor.execute(f'SELECT json_name,json_content,id FROM {tabl} WHERE status_work IS TRUE and time_work = (SELECT MIN(time_work) FROM {tabl}) LIMIT 1 FOR UPDATE SKIP LOCKED') 
        try:
            str_ok = cursor.fetchall()[0]
            #print(str_ok["id"])
            cursor.execute(f'UPDATE {tabl} SET time_work = {int(time.time())} WHERE id = "{str_ok["id"]}"')
            db.commit()
        except IndexError :
            db.commit()
            return None
    return str_ok


# Удалить ячейку
def delet_yach(where):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute( f"DELETE FROM {tabl} WHERE {where} " ) 
        db.commit()
        print('OK')

_create_table('token_transfer')
_create_table('json_transfer')
_create_table_id(tabl3)

if __name__ == "__main__":
    #print('0')
    add_id('0054554asdwadadw')
    #set_false(f'status_work = FALSE ' , f"json_name = '12.json'" , tabl2 )