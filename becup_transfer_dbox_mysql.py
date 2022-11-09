from msql_reqwert import get_one_false , sets_false , sets_true
from gdrive_respons import new_drive_and_json , move_one_file_round , delete_drive 
from sys import argv
import os
from time import sleep , time
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from googleapiclient.errors import HttpError
try:
   from sshtunnel import SSHTunnelForwarder
   import apprise
   from loguru import logger
except:
   os.system('pip install sshtunnel')
   os.system('pip install apprise')
   os.system('pip install loguru')
   from sshtunnel import SSHTunnelForwarder
   import apprise
   from loguru import logger

apobj = apprise.Apprise()
apobj.add('tgram://5035704615:AAE7XGex57LYUN23CxT2T67yNCknzgyy7tQ/183787479')
logger.add('logger_beckup.log', format="{time} - {level} - {message}")
n=0
tabl='dbox_bec'
token_read=open("osnova_token.txt", 'r').read()[:-1]


#@logger.catch
def drive_new_config(sektor): # Подготовка конфигураций 
   d_tokens=get_one_false()  # Получили все данные с базы 
   if d_tokens:
      print(d_tokens) 
      # Создаем диск для переноса  и Вяжем джисон   
      id_drive_peredachi=new_drive_and_json(sektor,f'accounts/{d_tokens[5]}')
      # Переносим Файл 20 Попыток 
      if not move_one_file_round(d_tokens[4],id_drive_peredachi):
         print('Перенос не удался')
      # GDrive и записываем в конфиг
      with open('/root/.config/rclone/rclone.conf', 'a') as f:
         f.write(f'\n[osnova_{sektor}]\ntype = drive\nscope = drive\ntoken = {token_read}\nteam_drive = {id_drive_peredachi}\n')
      sleep(2)
      # dropbox и записываем в конфиг
      with open('/root/.config/rclone/rclone.conf', 'a') as f:
        f.write(f'\n[dbox_{sektor}]\ntype = dropbox\ntoken = {d_tokens[6]}\n')

      id_drive_peredachi=(id_drive_peredachi,)
   else:
      print(" Нет доступных фалов или доступа к базе ")
      logger.error(f"🚨 Нет доступных фалов или доступа к базе 'drive' ")
      sleep(20)
      return drive_new_config(sektor)
   return d_tokens+id_drive_peredachi

#@logger.catch
def stat_progect(potok, ip_ser): # передача с помощью суб процесса
   logger.debug(f"Старт потока {potok} {ip_ser}")
   try:
      some_date = datetime.now()
      start_time= time()
      # Формируем токены и файл для передачи 
      data_drive=drive_new_config(potok)
      if data_drive:
         id_gd=data_drive[0]
         # Формируем Команду 
         com=f'rclone copy osnova_{potok}:{data_drive[3]} dbox_{potok}: --drive-stop-on-upload-limit --dropbox-chunk-size 150Mi --transfers 1 -P --cache-chunk-no-memory --drive-service-account-file /root/DB/accounts/{data_drive[5]} -v --log-file /root/rclone1.log'
         print(com)
         comls= com.split(' ')
         process = subprocess.Popen(comls, stdout=subprocess.PIPE, universal_newlines=True)
         print( str(process.pid) )
         logger.info(f'[{(process.pid)}] Start {data_drive[3]}')
         sleep(10)
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
               logger.info(f"✅ Передан RCLONE {ip_ser}")
               start_time=start_time-2001
               break
            elif line.find('Errors:                 1 ')>-1:
               apobj.notify(body=f'🚨 Ошибка RCLONE {ip_ser}')
               logger.error(f"🚨 Ошибка RCLONE {ip_ser}")
               break
            elif not line:
               break
         now_date = datetime.now()
         a=now_date - some_date
         logger.info(f'[{(process.pid)}] Время выполнения {timedelta(seconds=a.seconds)} PEREDAN : {data_drive[3]}')
         #reqest_sql_ok(data_drive[3])
         if time() - start_time > 2000:
            apobj.notify(body=f'✅ Передан 🕰️ Время выполнения {timedelta(seconds=a.seconds)} {ip_ser}')
            sets_true(id_gd)
            if move_one_file_round(data_drive[4],data_drive[1]):
               delete_drive(data_drive[-1])
         else:
            sets_false(id_gd)
            print("Быстрый выход вернем False")
            if move_one_file_round(data_drive[4],data_drive[1]):
               delete_drive(data_drive[-1])

   except TimeoutError as err: 
      apobj.notify(body=f'🚨[{ip_ser}] Ошибка {err} ')
      logger.error(f"🚨[{ip_ser}] Ошибка {err}")
      if move_one_file_round(data_drive[4],data_drive[1]):
         delete_drive(data_drive[-1])
         
   except AttributeError as err: 
      logger.error(f"🚨[{ip_ser}] Ошибка AttributeError Удаляем диск {data_drive[-1]}")
      delete_drive(data_drive[-1])
      sets_false(id_gd)
#
   except IndexError : 
      logger.error(f"[{ip_ser}]⚠️ Нет свободных фалов в базе  ")
      apobj.notify(body=f"[{ip_ser}]⚠️ Нет свободных фалов в базе  ")
      sleep(30)
   
   except Exception as err: 
      apobj.notify(body=f'🚨[{ip_ser}] Ошибка {err}')
      logger.error(f"🚨[{ip_ser}] Ошибка {err}")
   

def main(workers,ip_servv=''): 
   
   executor =ThreadPoolExecutor(max_workers=workers)
   for x in range(1,10000):
      if os.path.exists('Stop'):
         apobj.notify(body=f'🚨 Stop : ждем завершения текущих процессов ')
         break
      sleep(5)
      executor.submit(stat_progect,x,ip_servv)

if __name__ == '__main__':
   try:
      os.remove('/root/.config/rclone/rclone.conf')
   except:
      pass
   if len(argv) == 3:
      main(int(argv[1]), argv[2])
   else:
      main(int(argv[1]))
