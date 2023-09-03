from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import time
import os
import json
from random import randint
from json import loads
from colorama import Fore , init
init(autoreset=True)

SCOPES = ["https://www.googleapis.com/auth/drive",
                  "https://www.googleapis.com/auth/cloud-platform",
                  "https://www.googleapis.com/auth/iam",
                  ]



def chek_ref(token='token.json'):
    creds = None
    if os.path.exists(token):
       creds = Credentials.from_authorized_user_file(token, SCOPES)
       # If there are no (valid) credentials available, let the user log in.
       if not creds or not creds.valid:
           try:
              creds.refresh(Request())
              print(Fore.BLUE+'+ : ','ОБНОВЛЕН')
           except Exception as er:
              print(Fore.RED+"НЕВАЛИДНЫЙ ТОКЕН")
              print(er) 
              print(er) 
              return False
    else :
        return "НЕТ ДЖИСОНА"
    
    #servise=_service_avtoriz_v3(token)
    print(Fore.GREEN+"ТОКЕН ВАЛИДНЫЙ")
    return "ТОКЕН ВАЛИДНЫЙ"

def check_json(json_pach): # Авторизация с новым джисоном вход номер джисона выход новый сервисе
   SERVICE_ACCOUNT_FILE = json_pach
   try:
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=credentials)
        f=service.files().list(fields='files(id, name , parents)').execute() 
        print(Fore.GREEN+"СЕРВИСНЫЙ АККАУНТ ВАЛИДНЫЙ")
        return "СЕРВИСНЫЙ АККАУНТ ВАЛИДНЫЙ"
   
   except Exception as err: 
        print(err)
        print(Fore.RED+"СЕРВИСНЫЙ АККАУНТ НЕВАЛИДНЫЙ")
        return False

def service_avtoriz_v3(token='token.json'):# АВТОРИЗАЦИЯ  Drive API v3  
    SCOPES = ["https://www.googleapis.com/auth/drive",
              "https://www.googleapis.com/auth/cloud-platform",
              "https://www.googleapis.com/auth/iam"]
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token):
        creds = Credentials.from_authorized_user_file(token, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token, 'w') as token:
            token.write(creds.to_json())

    service = build('drive', 'v3', credentials=creds)
    return service


def new_drive( service , name:str ): # Создаем новый диск подключаем джисоны вход : 
    new_grive=None
    for x in range(5):
        try:
            if not new_grive:
                new_grive = service.teamdrives().create(requestId=randint(1,9999999), body={"name":f"ploting_{name}"}).execute() #создать диск
                return new_grive['id'] 

        except HttpError as err: 
            print(f'[ERROR Create Drive] {err}' )
            time.sleep(2)

def new_parents( service , id_drive ,  json:str = None, email:str = None ): # Подключаем джисоны :    
    if json:
        email=loads(open(json, 'r').read())['client_email']

    for x in range(5):
        try:         
            service.permissions().create(fileId=id_drive, 
                                         fields='emailAddress', 
                                         supportsAllDrives=True, 
                                         body={
                                               "role": "organizer",
                                               "type": "user",
                                               "emailAddress": email
                                         }).execute()
            return 'OK'

        except HttpError as err: 
            print(f'[ERROR Create Drive] {err}' )
            time.sleep(2)


def delete_drive(s_iddrive):      
    service=_service_avtoriz_v3()
    for x in range(5):
        try:
            service.drives().delete(driveId=s_iddrive).execute()
            return True
        except HttpError as err: 
            print(f'[ERROR DELETE Drive] {err}' )
            time.sleep(2)
    return False