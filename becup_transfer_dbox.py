import os
from time import sleep
import ast
import subprocess
import paramiko
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, time, date
try:
   import pymysql
   from sshtunnel import SSHTunnelForwarder
   import apprise
   import paramiko
except:
   os.system('pip install pymysql')
   os.system('pip install sshtunnel')
   os.system('pip install apprise')
   os.system('pip install paramiko')
   import pymysql
   from sshtunnel import SSHTunnelForwarder
   import apprise
   import paramiko

apobj = apprise.Apprise()
apobj.add('tgram://5458358981:AAHmEsED5yN09uD2yNrpFdi9lBia7yZ59CQ/183787479')
n=0
tabl='dbox_bec'

def reqest_sql_get():
    ssh=connect('74.207.227.175','JSoU9PPV')
    ssh.send(f'python3 get_set.py get \n')
    sleep(2)
    pr_data=str(ssh.recv(5000))
    #print(str(ssh.recv(5000)))
    try:
       s_data=pr_data[pr_data.find(r'\r\n(')+5:pr_data.find(r')\r\n\x1b')]
       #print(s_data)
       kort=ast.literal_eval(s_data)
       rz= kort[0],kort[1],kort[2],kort[3]
    except SyntaxError:
       rz= None
    finally:
        ssh.close()
    return rz

def reqest_sql_set(id_true):
    ssh=connect('74.207.227.175','JSoU9PPV')
    ssh.send(f'python3 get_set.py set {id_true} \n')
    ssh.close()


def reqest_sql_set_false(id_true):
    ssh=connect('74.207.227.175','JSoU9PPV')
    ssh.send(f'python3 get_set.py False {id_true} \n')
    ssh.close()


def reqest_sql_set_potok(id_p):
    ssh=connect('74.207.227.175','JSoU9PPV')
    ssh.send(f'python3 get_set.py potok {id_p} \n')
    ssh.close()


def connect(host,passw):
   for ttt in range(16):
       try:
         ssh = paramiko.SSHClient()
         ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
         if passw.find('.pem') > -1:
            print('connect to key')
            ssh.connect(hostname=host, username='root',key_filename=passw )
         else:
            print('connect to passwd')
            ssh.connect(hostname=host, username='root',password=passw )
         print("\033[32m{}\033[0m".format('conect:'))
         ssh=ssh.invoke_shell() 
         sleep(4)
         break
       except:
         sleep(2)
         print('Жду соединения')
         ssh=False
   return ssh

server = SSHTunnelForwarder(
    ('149.248.8.216', 22),
    ssh_username='root',
    ssh_password='XUVLWMX5TEGDCHDU',
    remote_bind_address=('127.0.0.1', 3306))

def getConnection(): 
    connection = pymysql.connect(host='127.0.0.1', port=server.local_bind_port, user='chai_cred',
                      password='Q12w3e4003r!', database='credentals',
                      cursorclass=pymysql.cursors.DictCursor)
    return connection


def download_token():
   server.start()
   mybd = getConnection()
   cur = mybd.cursor()
   cur.execute( f"SELECT * FROM {tabl} WHERE len=(SELECT min(len) FROM {tabl}) and status = 'True' " ) # запросим все данные  
   rows = cur.fetchall()
   print(len(rows))
   if len(rows) == 0:
      print('Нет свободных токенов !!!')
      mybd.commit()
      mybd.close()
      server.stop()
      sleep(20)
      return download_token()
   token=rows[0]['token']
   print(token)
   id=rows[0]['id']
   cur.execute( f"UPDATE {tabl} set status = 'False' WHERE id = {rows[0]['id']} ") # Обнавление данных
   mybd.commit()
   mybd.close()
   server.stop()
   return [token,id]

def vernem_true(id_v , conf_d):
   try:
      com=f'rclone ls {conf_d}:'
      comls= com.split(' ')
      process = subprocess.Popen(comls, stdout=subprocess.PIPE)
      process.wait()
      stat_len=len(str(process.communicate()[0]).split('\\n'))
      print('[ ! ] Plot account ' , str(stat_len))
   except:
      stat_len='error'

   server.start()
   mybd = getConnection()
   cur = mybd.cursor()
   cur.execute( f"UPDATE {tabl} set status = 'True' , len = '{stat_len}'  WHERE id = {id_v}") # Обнавление данных
   mybd.commit()
   mybd.close()
   server.stop()

def new_config(sektor): # Получаем токен dropbox и записываем в конфиг
   tokens=download_token()
   with open('/root/.config/rclone/rclone.conf', 'a') as f:
       f.write(f'\n[dbox_{sektor}]\ntype = dropbox\ntoken = {tokens[0]}\n')
   return tokens[1]

def drive_new_config(sektor): # Получаем токен  GDrive и записываем в конфиг
   d_tokens=reqest_sql_get()
   #print(d_tokens)
   token_read=open("osnova_token.txt", 'r').read()[:-1]
   if d_tokens:
      with open('/root/.config/rclone/rclone.conf', 'a') as f:
         f.write(f'\n[osnova_{sektor}]\ntype = drive\nscope = drive\ntoken = {token_read}\nteam_drive = {d_tokens[1]}\n')
      sleep(2) 
   else:
      print(" Нет доступных фалов или доступа к базе ")
      sleep(20)
      return drive_new_config(sektor)
   return d_tokens


def trak(t):
   for x in  range(10):
      sleep(8)
      print(t)

def stat_progect(potok): # передача с помощью суб процесса
   print("Старт потока ",potok)
   try:
      some_date = datetime.now()
      # Формируем токены и файл для передачи 
      id_db=new_config(potok)
      data_drive=drive_new_config(potok)
      id_gd=data_drive[0]
      # Формируем Команду 
      com=f'rclone copy osnova_{potok}:{data_drive[2]}/{data_drive[3]} dbox_{potok}: --drive-stop-on-upload-limit --transfers 1 -P -v --log-file /root/rclone1.log'
      comls= com.split(' ')
      process = subprocess.Popen(comls, stdout=subprocess.PIPE, universal_newlines=True)
      print( str(process.pid) )
      while True:
         line = process.stdout.readline()
         print(line)
         if line.find('Transferred:')>-1:
             sleep(8)
             print('['+str(process.pid)+'] - '+line)
         if not line:
            break
      print('['+str(process.pid)+'] - PEREDAN vernem True id' + str(id_db))
      now_date = datetime.now()
      apobj.notify(body=f'✅ Передан 🕰️ Время выполнения {timedelta(seconds=a.seconds)}')
      if now_date - some_date > 2000:
         print("Долго выходити  подтвердим")
         reqest_sql_set(id_gd)
      else:
         reqest_sql_set_false(id_gd)
         print("Быстрый выход вернем False")
      reqest_sql_set_potok(data_drive[1])
      vernem_true(id_db, f'dbox_{potok}')
   except Exception as err: 
      apobj.notify(body=f'🚨 Ошибока {err}')


def main(): 
   executor =ThreadPoolExecutor(max_workers=6)
   for x in range(1,10000):
      sleep(5)
      executor.submit(stat_progect,x)


if __name__ == '__main__':
   try:
      os.remove('/root/.config/rclone/rclone.conf')
   except:
      pass
   main()
