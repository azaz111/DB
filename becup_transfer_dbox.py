from dataclasses import replace
import os
from time import sleep , time
import ast
import subprocess
import paramiko
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import re
try:
   import pymysql
   from sshtunnel import SSHTunnelForwarder
   import apprise
   import paramiko
   from loguru import logger
except:
   os.system('pip install pymysql')
   os.system('pip install sshtunnel')
   os.system('pip install apprise')
   os.system('pip install paramiko')
   os.system('pip install loguru')
   import pymysql
   from sshtunnel import SSHTunnelForwarder
   import apprise
   import paramiko
   from loguru import logger

apobj = apprise.Apprise()
apobj.add('tgram://5458358981:AAHmEsED5yN09uD2yNrpFdi9lBia7yZ59CQ/183787479')
logger.add('logger_beckup.log', format="{time} - {level} - {message}")
n=0
tabl='dbox_bec'

def reqest_sql_get():
    ssh=connect('74.207.227.175','JSoU9PPV')
    ssh.send(f'python3 get_set.py get \n')
    sleep(2)
    pr_data=str(ssh.recv(5000))
    #print(pr_data)
    try:
       s_data=pr_data[pr_data.find(r'\r\n(')+5:pr_data.find(r')\r\n\x1b')]
       try:
          s_data=re.sub(r'\\', '', s_data)
       except:
         pass
       kort=ast.literal_eval(s_data)
       #print(kort)
       rz= kort[0],kort[1],kort[2],kort[3],kort[4],kort[5]
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

def reqest_sql_ok(id_true):
    ssh=connect('74.207.227.175','JSoU9PPV')
    ssh.send(f'python3 get_set.py ok {id_true} \n')
    sleep(5)
    pr_data=str(ssh.recv(5000))
    print(pr_data)
    ssh.close()

def connect(host,passw): # соеденение paramico
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

def drive_new_config(sektor): # Получаем токены
   d_tokens=reqest_sql_get()
   print(d_tokens)
   token_read=open("osnova_token.txt", 'r').read()[:-1]
   if d_tokens:
      # GDrive и записываем в конфиг
      with open('/root/.config/rclone/rclone.conf', 'a') as f:
         f.write(f'\n[osnova_{sektor}]\ntype = drive\nscope = drive\ntoken = {token_read}\nteam_drive = {d_tokens[1]}\n')
      sleep(2)
      # dropbox и записываем в конфиг
      with open('/root/.config/rclone/rclone.conf', 'a') as f:
        f.write(f'\n[dbox_{sektor}]\ntype = dropbox\ntoken = {d_tokens[5]}\n')
   else:
      print(" Нет доступных фалов или доступа к базе ")
      logger.error(f"🚨 Нет доступных фалов или доступа к базе 'drive' ")
      sleep(20)
      return drive_new_config(sektor)
   return d_tokens

def stat_progect(potok): # передача с помощью суб процесса
   logger.debug(f"Старт потока {potok}")
   try:
      some_date = datetime.now()
      start_time= time()
      # Формируем токены и файл для передачи 
      data_drive=drive_new_config(potok)
      id_gd=data_drive[0]
      # Формируем Команду 
      com=f'rclone copy osnova_{potok}:{data_drive[2]}/{data_drive[3]} dbox_{potok}: --drive-stop-on-upload-limit --transfers 1 -P --dropbox-chunk-size 150Mi --drive-service-account-file /root/DB/accounts/{data_drive[4]} -v --log-file /root/rclone1.log'
      print(com)
      comls= com.split(' ')
      process = subprocess.Popen(comls, stdout=subprocess.PIPE, universal_newlines=True)
      print( str(process.pid) )
      logger.info(f'[{(process.pid)}] Start {data_drive[3]}')
      sleep(5)
      while True:
         line = process.stdout.readline()
         #print(line)
         #input("["+line+"]")
         if line.find('*')>-1:
             sleep(8)
             trans=line.find('Transferred')
             print('['+str(process.pid)+'] - '+line[:trans])
         elif line.find('Checks:                 1 / 1, 100%')>-1:
            #apobj.notify(body=f'✅ Передан RCLONE')
            logger.info(f"✅ Передан RCLONE")
            start_time=start_time-2001
            break
         elif line.find('Errors:                 1 ')>-1:
            apobj.notify(body=f'🚨 Ошибка RCLONE')
            logger.error(f"🚨 Ошибка RCLONE {potok}")
            break
         elif not line:
            break
      now_date = datetime.now()
      a=now_date - some_date
      logger.info(f'[{(process.pid)}] Время выполнения {timedelta(seconds=a.seconds)} PEREDAN : {data_drive[3]}')
      reqest_sql_ok(data_drive[3])
      if time() - start_time > 2000:
         apobj.notify(body=f'✅ Передан 🕰️ Время выполнения {timedelta(seconds=a.seconds)}')
         reqest_sql_set(id_gd)
      else:
         reqest_sql_set_false(id_gd)
         print("Быстрый выход вернем False")
   except Exception as err: 
      apobj.notify(body=f'🚨 Ошибка {err}')
      logger.error(f"🚨 Ошибка {err}")
      #print(f'[ERROR] {err}')
      

def main(): 
   
   executor =ThreadPoolExecutor(max_workers=int(input('Укажи количество потоков : ')))
   for x in range(1,10000):
      sleep(5)
      executor.submit(stat_progect,x)

if __name__ == '__main__':
   try:
      os.remove('/root/.config/rclone/rclone.conf')
   except:
      pass
   main()
