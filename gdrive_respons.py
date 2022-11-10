from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
import time
from random import randint
from json import loads

def service_avtoriz_v3(token='token.json'):# АВТОРИЗАЦИЯ  Drive API v3  
    SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/iam',
    ]
    creds = Credentials.from_authorized_user_file(token, SCOPES)
    service = build('drive', 'v3', credentials=creds)
    return service

def new_drive_and_json( name:str , json:str , service=service_avtoriz_v3()): # Создаем новый диск подключаем джисоны вход : желаемое имя  выход data drive
    new_grive=None
    email=loads(open(json, 'r').read())['client_email']
    for x in range(5):
        try:
            if not new_grive:
                new_grive = service.teamdrives().create(requestId=randint(1,9999999), body={"name":f"perenos_{name}"}).execute() #создать диск
            
            service.permissions().create(fileId=new_grive['id'], 
                                         fields='emailAddress', 
                                         supportsAllDrives=True, 
                                         body={
                                               "role": "fileOrganizer",
                                               "type": "user",
                                               "emailAddress": email
                                         }).execute()
            return new_grive['id'] 

        except HttpError as err: 
            print(f'[ERROR Create Drive] {err}' )
            time.sleep(2)

def move_one_file_round(new_file_l,id_foldnazna,service=service_avtoriz_v3()):  # Перенос  файла в указанную папрку или диск вход : Список айди которые нужно перенести и айди родителя     
    file=None
    for x in range(20):
        try:
            if not file:
                file = service.files().get(fileId=new_file_l, supportsAllDrives=True, fields='parents').execute()
                previous_parents = ",".join(file.get('parents'))
            #print(f'perenos :{new_file_l}')
            service.files().update(fileId=new_file_l,
                                   addParents=id_foldnazna,
                                   supportsAllDrives=True, 
                                   removeParents=previous_parents, fields='id, parents').execute()# перемещаем в бекапную папку
            return True
        except HttpError as err: 
            print(f'[ERROR MOVE] {err}' )
            time.sleep(2)
    return False

def move_list_file_round(new_file_l,id_foldnazna,service=service_avtoriz_v3()):  # Перенос  файла в указанную папрку или диск вход : Список айди которые нужно перенести и айди родителя     
    try:
        for new_file in new_file_l:
            file = service.files().get(fileId=new_file, supportsAllDrives=True, fields='parents').execute()
            previous_parents = ",".join(file.get('parents'))
            print(f'perenos :{new_file}')
            file = service.files().update(fileId=new_file,
                                          addParents=id_foldnazna,
                                          supportsAllDrives=True, 
                                          removeParents=previous_parents, fields='id, parents').execute()# перемещаем в бекапную папку
        return True
    except HttpError as err: 
        print(f'[ERROR MOVE] Будем менять Диск Oшибка: {err}' )
        time.sleep(2)
        return False

def delete_drive(s_iddrive,service=service_avtoriz_v3()):  # Перенос  файла в указанную папрку или диск вход : Список айди которые нужно перенести и айди родителя     
    for x in range(5):
        try:
            service.drives().delete(driveId=s_iddrive).execute()
            return True
        except HttpError as err: 
            print(f'[ERROR DELETE Drive] {err}' )
            time.sleep(2)
    return False
