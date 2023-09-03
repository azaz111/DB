import os  , time
from argparse import ArgumentParser
from competed import rclone_config_from_transfer as podgotovka
from colorama import Fore , init
init(autoreset=True)

def transfer(dirs,email):

   rclone_path='/root/.config/rclone/rclone.conf'
   if os.path.exists(rclone_path):
      with open(rclone_path, 'w') as f:
         f.write('')
   
   unique=[]
   n=0
   while True:
       dirfiles = os.listdir(dirs)
       
       for qqq in  dirfiles:
          if qqq not in unique and os.stat(dirs+'/'+qqq).st_size > 108650979000:
              if qqq.endswith(".plot"):
                 n+=1
                 download_json=podgotovka(n , email)
                 time.sleep(5)
                 print('запускаю транс  ' + qqq + 'На джисоне номер :' + str(n))
                 com=f'screen -dmS trans{n} rclone move {dirs}/{qqq} crypt_{n}: --drive-stop-on-upload-limit --transfers 1 -P --drive-service-account-file {download_json} -v --log-file /root/rclone.log --drive-upload-cutoff=1000T'
                 print(Fore.GREEN+'Запуск команды :' , '\n' , com)
                 os.system(com)
                 unique.append(qqq)
                 if len(unique)>40: 
                    unique=unique[-40:]
       time.sleep(30)



if __name__ == '__main__':
    parse = ArgumentParser(description=' Настройка трансфера плотов .')
    parse.add_argument('--pach', default='/disk1', help='Путь c плотами .')
    parse.add_argument('--email', '-e' , help='email общего аакаунта для привязки всех дисков.')
    args = parse.parse_args()
    transfer(
       dirs=args.pach,
       email=args.email
    )

