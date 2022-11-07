from importlib.metadata import files
import os
from msql_reqwert import create_table , add
from time import sleep
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
import pickle
from BIB_API import perenos_fails_list , folder_all , new_drive , drive_ls
from loguru import logger
logger.add('logger_go.log', format="{time} - {level} - {message}")
import sqlite3





# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive",
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/iam"]

# Авторизация гугл
def service_avtoriz():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('drive', 'v3', credentials=creds)
    return service

# Список всех файлов на аккаунте без папок ! в папке PLOT
def ls_files(rodit): 
    file_spis=[]
    page_token = None
    service=service_avtoriz()
    while True:
        response = service.files().list(q=f"mimeType != 'application/vnd.google-apps.folder' and '{rodit}' in parents",
                                        corpora='allDrives',
                                        includeItemsFromAllDrives=True,
                                        supportsAllDrives=True,
                                        fields='nextPageToken, files(id, name , parents)',
                                        pageToken=page_token).execute() 
        for file in response.get('files', []):
            file_spis.append(file)
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break  
    return file_spis[:]


def driver_god():
    while True:
        try:
            # Перенос
            service=service_avtoriz()
            lenss=folder_all(service) 
            
            dname=[x["name"] for x in drive_ls(service)]
            for x in lenss:
                if x['name'] not in dname:
                    r=new_drive(service,x["name"])
                    perenos_fails_list([x['id']],r['id'],service)
            # Доп проверка        
            for x in drive_ls(service):
                if len(folder_all(service, x["id"])) == 1 :
                    print(f'{x["name"]} - Ok')
                else:
                    print(f'{x["name"]} - Warning perenos !')
                    gluk=x['id']
                    perenos_fails_list([h['id'] for h in lenss if h['name']== x['name']],x['id'],service)
            break
        except Exception as er :
            print(str(er))
            #dell_pustoi(gluk,service)

    logger.debug(' C дисками порядок ')


def sdor_files(id_drive,plot_id):
    create_table()
    sp_f=ls_files(plot_id['id'])
    print('Найдено фалов ' , len(sp_f) )
    json=[]
    [json.extend((range(1,601))) for x in range(10)]
    db_tokens=completed_dbox(len(sp_f))
    for data in sp_f:
        try:
            add(data['parents'][0],plot_id['name'],data['name'],data['id'],f'{json.pop()}.json',f'{db_tokens.pop()}')
        except Exception as er :
            print(str(er))
    logger.debug(f"Cохраняем список фалов в файл ")


def main():
    # определим Диск 
    drive_ish=drive_ls(service_avtoriz())
    if len(drive_ish)!=1:
        print(' Больше одного диска \n By ')
        exit()
    else: 
        paps=[x for x in folder_all(service_avtoriz()) if x['name']== 'PLOT']
        if paps:
            if len(paps)!=1:
               input("Больше одной папки")
            logger.info(f"Начали...!")   
            sdor_files(drive_ish[0]['id'],paps[0])
        else:
            print('Нет папки Плот ')
            exit()
    logger.info(f"Закончили...")


def completed_dbox(n):
    with open('registered.txt', 'r') as f :
       str=f.read().split('\n')
    log=[x.split(':')[0] for x in str ]
    pas=[x.split(':')[1] for x in str ]
    token=[':'.join(x.split(':')[2:]) for x in str ]
    logger.info(f'Доступно {len(token)} аккаунта , сортировка по {n/len(token)}')
    while len(token)<n:
        token+=token
    return token



if __name__ == '__main__':
    main()
    
    
    
    
