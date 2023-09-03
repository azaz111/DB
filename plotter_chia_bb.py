import os , time , threading
from argparse import ArgumentParser
from random import choice, randint
import subprocess
try:
   from mem_edit import Process
except:
   pass

try:
   os.system(f'apt install cputool')
   os.system(f'pip install mem-edit')
except:
   pass
from mem_edit import Process

def zamorozit_nagruz(nagruz):
   time.sleep(4)
   pid = Process.get_pid_by_name('bladebit')
   print(pid) 
   while Process.get_pid_by_name('bladebit')==pid:
      print("\033[32m{}\033[0m".format(' Замена нагрузки ! '))
      proc=subprocess.Popen(['cputool','--cpu-limit',str(choice(nagruz)),'-p',str(pid)])
      time.sleep(randint(80,450))
      proc.kill()
   print('ВЫключили заморозку нагрузки')


def plot(scolco,dirs,monitorind,cpu):
   while True:
      if len(os.listdir(monitorind)) < int(scolco) :
         time.sleep(5)
         print('Запускаю Плотинг с ограничением нагрузки ')
         zap_namber=f'./bladebit/build/bladebit -n 5000 -f b8e1d57e3e2dbb40ac8f2b257b762d05fcfc5b79c32a22255424644b7d183daa7c454624783f2d959c02eb1d2a4ba3a3 \
                                                     -p 91ea997633345082b15f83b957449180037030b6b7485f07ed4ee7558d08d3efbccf2c3d68ba724f5b3a8281a0055e27 ramplot {dirs}'
         print(zap_namber)
         x = threading.Thread(target=zamorozit_nagruz, args=([cpu],))
         x.start()
         os.system(f'cd && ' + zap_namber)    
      else:
         print('\r Статуст : {}'.format('Ожидаю  передачи '), end='')  
      time.sleep(20)


if __name__ == '__main__':
    parse = ArgumentParser(description=' Включаем плоттер в зависимрсти от заданного количества .')
    parse.add_argument('--scolco','-s', default='30', help='Укажи количество плотов в папке .')
    parse.add_argument('--pach', default='/disk1', help='Путь в который плотить .')
    parse.add_argument('--monitorind', default='/disk1', help='Путь по которому мониторить .')
    parse.add_argument('--zagruzka' ,'-cpu ', type=int , default='10000', help='Лимит загрузки CPU .')
    args = parse.parse_args()
    plot(
       scolco=args.scolco,
       dirs=args.pach,
       monitorind=args.monitorind,
       cpu=args.zagruzka,
    )

