from mysql_db import * 
from google_drive_respons import * 
import configparser
from colorama import Fore , init
init(autoreset=True)

token_db='token_transfer' 
json_db='json_transfer' 
path_sesion='path_sesion_tokens'
sektor=0
#my_email='1@xylor.online'
rclone_path='/root/.config/rclone/rclone.conf'

def rclone_config_from_transfer(sektor , my_email):

    if not os.path.exists(path_sesion):
       os.mkdir(path_sesion)
    #shutil.rmtree(path_sesion)
    
    while True:
        d_tokens=get_one_data(token_db)
        if d_tokens:
            with open(path_sesion+'/'+d_tokens['json_name'], 'w') as f:
              f.write(d_tokens['json_content'])
            if chek_ref(path_sesion+'/'+d_tokens['json_name']):
               break
            else:
               os.remove(path_sesion+'/'+d_tokens['json_name'])
               set_false(f'status_work = FALSE ' , f"json_name = '{d_tokens['json_name']}'" ,token_db )
        else:
            print('НЕВОЗМОЖНО ПРОДОЛЖИТЬ НЕТ ВАЛИДНЫХ ТОКЕНОВ')
            exit()
    
    while True:
        d_json=get_one_data(json_db)
        if d_json:
            with open(path_sesion+'/'+d_json['json_name'], 'w') as f:
               f.write(d_json['json_content'])
    
            if check_json(path_sesion+'/'+d_json['json_name']):
               break
            else:
               os.remove(path_sesion+'/'+d_json['json_name'])
               set_false(f'status_work = FALSE ' , f"json_name = '{d_json['json_name']}'" ,json_db )
        else:
            print('НЕВОЗМОЖНО ПРОДОЛЖИТЬ НЕТ ВАЛИДНЫХ СЕРВИСНЫХ АККАУНТОВ')
            exit()
    
    
    service=service_avtoriz_v3(path_sesion+'/'+d_tokens['json_name'])
    
    team_drive=new_drive(service , str(sektor))
    print(Fore.GREEN+"СОЗДАН ДИСК : " , team_drive)
    add_id(team_drive)
    new_parents(service,team_drive,json=path_sesion+'/'+d_json['json_name'])
    new_parents(service,team_drive,email=my_email)
    print(Fore.GREEN+" ... привязаны джисоны основной юзер к диску  : " , )

    config = configparser.ConfigParser()
    config[f'drive_{sektor}'] = {'type': 'drive', 
                                 'token': d_tokens['json_content'],  
                                 'team_drive': team_drive}

    config[f'crypt_{sektor}'] = {
                               'type': 'crypt',
                               'remote': f'drive_{sektor}:',
                               'password': '-V6FhNf6jLb9CJ-2ItyBT2EYLUkPoVf7',
                               'password2': 'Mbe7Sn3oSPv4E3k5rTS69jfyOel1FHc4'
                                }
    
    with open(rclone_path, 'a') as f:
       config.write(f)
    #sektor+=1
    print(Fore.GREEN+"СОЗДАН rclone.conf : " , rclone_path )
    return path_sesion+'/'+d_json['json_name']
