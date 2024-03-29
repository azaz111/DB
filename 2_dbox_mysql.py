from msql_reqwert import  get_one_false2 , sets_stat , sets_false_token , sets_ok
from gdrive_respons import *
from sys import argv
import os
from time import sleep , time
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError
from requests import get
try:
   ip_address = get('http://ipinfo.io/json').json()['ip']
except:
   ip_address="00.00.00.000"
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
def ls_dbox(sektor):
   full_plot=[]
   com=f'rclone ls dbox_{sektor}:'
   comls= com.split(' ')
   process = subprocess.Popen(comls, stdout=subprocess.PIPE)
   process.wait()
   plots=process.communicate()[0].decode('utf-8').split('\n')[:-1]
   for x in plots:
      full_plot.append(x[13:])
   return full_plot
   

#@logger.catch
def drive_new_config(sektor): # Подготовка конфигураций 
   global list_transfer
   try:
      d_tokens=get_one_false2()  # Получили все данные с базы
   except:
      sleep(30)
      d_tokens=None
      
   if d_tokens== 'not found':
      logger.info('У токена не осталось фалов для передачи')
      return
   if d_tokens:
      print(d_tokens)
      try:
         service=service_avtoriz_v3()
         try:
            service.files().get(fileId=d_tokens[1], supportsAllDrives=True, fields='id').execute()
         except Exception as error :
            print('Error:', error)
            logger.warning(f'Error:  {error} ')  
      except HttpError : 
         apobj.notify(body=f'🚨[Забанен исходник !!! Возвращаю False') 
         logger.warning(f' Забанен исходник !!! Возвращаю False ')   
         sleep(30)
         sets_false_token(d_tokens[6])
         return


      

      # dropbox и записываем в конфиг
      with open('/root/.config/rclone/rclone.conf', 'a') as f:
        f.write(f'\n[dbox_{sektor}]\ntype = dropbox\ntoken = {d_tokens[6]}\n')
      ls_dboxs=ls_dbox(sektor)


      list3=list(set(d_tokens[3])-set(ls_dboxs)) 

      list_transfer=[]
      for x in range(len(d_tokens[3])):
         if d_tokens[3][x] in list3:
            list_transfer.append(d_tokens[4][x])
      logger.info(f'Перенос файлов {len(list_transfer)} шт.')
      while True:
         # Создаем диск для переноса  и Вяжем джисон  
         id_drive_peredachi=new_drive_and_json(sektor,f'accounts/{d_tokens[5]}')
         # Переносим Файл 20 Попыток 
         if not move_list_file_round(list_transfer,id_drive_peredachi):
            print('Перенос не удался')
            delete_drive(id_drive_peredachi)
         else:
            print('Файлы успешно перенесены id:',id_drive_peredachi)
            break

      # GDrive и записываем в конфиг
      with open('/root/.config/rclone/rclone.conf', 'a') as f:
         f.write(f'\n[osnova_{sektor}]\ntype = drive\nscope = drive\ntoken = {token_read}\nteam_drive = {id_drive_peredachi}\n')
      sleep(2)

      logger.debug(f"Задание: {len(d_tokens[3])} | id_Token : {d_tokens[0]} | Ls dbox : {len(ls_dboxs)} | Передать {len(list3)}")
      # Список имен для передачи
      with open('f.txt', 'w') as f:
         f.write(f'\n'.join(list3))

      id_drive_peredachi=(id_drive_peredachi,)
   else:
      print(" Нет доступных фалов или доступа к базе ")
      logger.error(f"🚨 Нет доступных фалов или доступа к базе  ")
      apobj.notify(body=f"[{ip_address}]⚠️🚨 Нет доступных фалов или доступа к базе ")
      sleep(120)
      return drive_new_config(sektor)
   return d_tokens+id_drive_peredachi

#@logger.catch
def stat_progect(potok, ip_ser , work ): # передача с помощью суб процесса
   try:
      service=service_avtoriz_v3()
   except RefreshError : 
      logger.error(f"НЕВАЛИДНЫЙ ТОКЕН {potok} {ip_ser}")
      apobj.notify(body=f"[{ip_ser}]⚠️ НЕВАЛИДНЫЙ ТОКЕН ")
      sleep(25)
      return stat_progect(potok, ip_ser , work )

   logger.debug(f"Старт потока {potok} {ip_ser}")
   try:
      some_date = datetime.now()
      start_time= time()
      # Формируем токены и файл для передачи 
      data_drive=drive_new_config(potok)
      if data_drive:
         id_gd=data_drive[0]
         # Формируем Команду 
         com=f'rclone copy osnova_{potok}: dbox_{potok}: --drive-stop-on-upload-limit --transfers {work} -P --drive-service-account-file /root/DB/accounts/{data_drive[5]} -v --log-file /root/rclone1.log --files-from f.txt --tpslimit 12'
         print(com)
         comls= com.split(' ')
         process = subprocess.Popen(comls, stdout=subprocess.PIPE, universal_newlines=True)
         print( str(process.pid) )
         logger.info(f'[{(process.pid)}] Start {len(data_drive[3])} .шт')
         x=0
         tr=''
         pr=''
         ti=''
         er='None'
         while True:
             line = process.stdout.readline()
             #print(line)
             if line.find('Transferred')>-1 and line.find('%,')>-1:
                tr=line.split('Transferred')[1][3:-2]
             elif line.find('Transferred')>-1 and line.find('%,')==-1:
                pr=line.split('Transferred')[1][12:-2]
             elif line.find('Elapsed time:')>-1:
                ti=line[10:-2]
             elif not line:
                print('Завершено')
                er='OK'
             x+=1
             if x == 400:
                 now = datetime.now() + timedelta(minutes=480)
                 try:
                     sets_stat(ip_ser, id_gd ,int(time()), f' Work : {tr} | peredano : {pr} | time_wok {ti}')
                 except Exception as err: 
                     apobj.notify(body=f'🚨test:[{ip_ser}] Ошибка {err}')
                     logger.error(f"🚨[{ip_ser}] Ошибка {err}")
                 print(ip_ser,f'data {now.strftime("%d-%m-%Y %H:%M")} | Work : {tr} | peredano : {pr} | time_wok {ti}')
                 x=0
             elif er!='None':
                 break
         now_date = datetime.now()
         a=now_date - some_date
         logger.info(f'[{(process.pid)}] Время выполнения {timedelta(seconds=a.seconds)} PEREDAN : {data_drive[3]}')
         #reqest_sql_ok(data_drive[3])
         if time() - start_time > 2000:
            # Повторно считаем файлы на дропбоксе
            logger.info(f' Пересчет файлов на дропбоксе ') 
            pov_dbox=ls_dbox(potok)
            for x in pov_dbox:
               sets_ok(x)
            # Переносим обратно
            logger.info(f' Возвращаем файлы в плот : {len(list_transfer)} шт') 
            if not move_list_file_round(list_transfer,data_drive[1]):
               logger.debug('Перенос обратно не удался')
            # Возврашаем фалс токену
            logger.info(f' Возвращаем фалс токену ')
            sets_false_token(data_drive[6])
            peredan=[x for x in data_drive[3] if x in pov_dbox]
            apobj.notify(body=f'[{ip_ser}]✅ Передан 🕰️ Время: {timedelta(seconds=a.seconds)} Передано {len(peredan)}/{len(data_drive[3])}')

   except Exception as err: 
      apobj.notify(body=f'🚨[{ip_ser}] Ошибка {err}')
      logger.error(f"🚨[{ip_ser}] Ошибка {err}")
      if data_drive[6]:
         sets_false_token(data_drive[6])
   

def main(workers,ip_servv=''): 

   for x in range(1,10000):
      if os.path.exists('Stop'):
         apobj.notify(body=f'🚨 Stop : ждем завершения текущих процессов ')
         break
      sleep(5)
      stat_progect(x,ip_servv,workers)

if __name__ == '__main__':
   try:
      os.remove('/root/.config/rclone/rclone.conf')
   except:
      pass
   if len(argv) == 3:
      main(int(argv[1]), argv[2])
   else:
      main(int(argv[1]),ip_address)
