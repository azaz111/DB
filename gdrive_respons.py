from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import time , os
from random import randint
from json import loads

def service_avtoriz_v3(token='token.json'):# АВТОРИЗАЦИЯ  Drive API v3  
    SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/iam',
    'https://www.googleapis.com/auth/cloudplatformprojects'
    ]
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token, 'w') as tokenw:
            tokenw.write(creds.to_json())

    service = build('drive', 'v3', credentials=creds)
    return service


def new_drive_and_json( name:str , json:str ): # Создаем новый диск подключаем джисоны вход : желаемое имя  выход data drive
    service=service_avtoriz_v3()
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

def move_one_file_round(new_file_l,id_foldnazna):  # Перенос  файла в указанную папрку или диск вход : Список айди которые нужно перенести и айди родителя     
    service=service_avtoriz_v3()
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

def delete_drive(s_iddrive):  # Перенос  файла в указанную папрку или диск вход : Список айди которые нужно перенести и айди родителя     
    service=service_avtoriz_v3()
    for x in range(5):
        try:
            service.drives().delete(driveId=s_iddrive).execute()
            return True
        except HttpError as err: 
            print(f'[ERROR DELETE Drive] {err}' )
            time.sleep(2)
    return False
