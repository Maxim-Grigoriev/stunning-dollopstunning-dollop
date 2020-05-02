import requests
from requests.exceptions import ConnectionError
from time import sleep
from datetime import date, timedelta, datetime
import json
import sys
import numpy as np
import pandas as pd


from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.http import MediaFileUpload





#1. Выполняем запрос в API Директа

if sys.version_info < (3,):
    def u(x):
        try:
            return x.encode("utf8")
        except UnicodeDecodeError:
            return x
else:
    def u(x):
        if type(x) == type(b''):
            return x.decode('utf8')
        else:
            return x



# --- Входные данные ---
# Адрес сервиса Reports для отправки JSON-запросов (регистрозависимый)
ReportsURL = 'https://api.direct.yandex.com/json/v5/reports'

# OAuth-токен пользователя, от имени которого будут выполняться запросы
token = '##############################' #токен приложения

# Логин клиента рекламного агентства
# Обязательный параметр, если запросы выполняются от имени рекламного агентства
#clientLogin = '###############' #убрать решётку в начале строки и внутри кавычек ввести логин

# --- Подготовка запроса ---
# Создание HTTP-заголовков запроса
headers = {
           # OAuth-токен. Использование слова Bearer обязательно
           "Authorization": "Bearer " + token,
           # Логин клиента рекламного агентства
           #"Client-Login": clientLogin, #убрать решётку в начале строки
           # Язык ответных сообщений
           "Accept-Language": "ru",
           # Режим формирования отчета
           "processingMode": "auto"
           # Формат денежных значений в отчете
           # "returnMoneyInMicros": "false",
           # Не выводить в отчете строку с названием отчета и диапазоном дат
           # "skipReportHeader": "true",
           # Не выводить в отчете строку с названиями полей
           # "skipColumnHeader": "true",
           # Не выводить в отчете строку с количеством строк статистики
           # "skipReportSummary": "true"
           }

today = date.today()
start_date = (today - timedelta(days=1)).strftime("%Y-%m-%d") # Дата начала. Можно указать прозвольное значение, например "2020-04-22"
end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d") # Дата завершения Можно указать прозвольное значение, например "2020-04-23"




# Создание тела запроса
body = {
    "method":"get",
    "params": {
        "SelectionCriteria": {
            "DateFrom": start_date,
            "DateTo": end_date
        },
        "FieldNames": [
            "Date",
            "CampaignId",
            "AdNetworkType",
            "CampaignName",
            "AdGroupId",
            "AdId",
            "CriterionId",
            "Criterion",
            "CriterionType",
            "Device",
            "TargetingLocationName",
            "Placement",
            "Impressions",
            "Clicks",
            "Cost"
        ],
        "ReportName": u("Report "+ str(today)),
        "ReportType": "CUSTOM_REPORT",
        "DateRangeType": "CUSTOM_DATE",
        "Format": "TSV",
        "IncludeVAT": "NO",
        "IncludeDiscount": "NO"
    }
}

# Кодирование тела запроса в JSON
body = json.dumps(body, ensure_ascii=False).encode('utf8')

# --- Запуск цикла для выполнения запросов ---
# Если получен HTTP-код 200, то выводится содержание отчета
# Если получен HTTP-код 201 или 202, выполняются повторные запросы
while True:
    try:
        req = requests.post(ReportsURL, body, headers=headers)
        req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
        if req.status_code == 400:
            print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
            print("RequestId: {}".format(req.headers.get("RequestId", False)))
            print("JSON-код запроса: {}".format(u(body)))
            print("JSON-код ответа сервера: \n{}".format(u(req.json())))
            break
        elif req.status_code == 200:
            print("Отчет Директа создан успешно")
            #print("RequestId: {}".format(req.headers.get("RequestId", False)))
            #print("Содержание отчета: \n{}".format(u(req.text)))
            my_file = open("my_file.txt", "w", encoding="utf-8")
            my_file.write(req.text)
            my_file.close()
            break
        elif req.status_code == 201:
            print("Отчет успешно поставлен в очередь в режиме офлайн")
            retryIn = int(req.headers.get("retryIn", 60))
            print("Повторная отправка запроса через {} секунд".format(retryIn))
            print("RequestId: {}".format(req.headers.get("RequestId", False)))
            sleep(retryIn)
        elif req.status_code == 202:
            print("Отчет формируется в режиме офлайн")
            retryIn = int(req.headers.get("retryIn", 60))
            print("Повторная отправка запроса через {} секунд".format(retryIn))
            print("RequestId:  {}".format(req.headers.get("RequestId", False)))
            sleep(retryIn)
        elif req.status_code == 500:
            print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
            print("RequestId: {}".format(req.headers.get("RequestId", False)))
            print("JSON-код ответа сервера: \n{}".format(u(req.json())))
            break
        elif req.status_code == 502:
            print("Время формирования отчета превысило серверное ограничение.")
            print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
            print("JSON-код запроса: {}".format(body))
            print("RequestId: {}".format(req.headers.get("RequestId", False)))
            print("JSON-код ответа сервера: \n{}".format(u(req.json())))
            break
        else:
            print("Произошла непредвиденная ошибка")
            print("RequestId:  {}".format(req.headers.get("RequestId", False)))
            print("JSON-код запроса: {}".format(body))
            print("JSON-код ответа сервера: \n{}".format(u(req.json())))
            break

    # Обработка ошибки, если не удалось соединиться с сервером API Директа
    except ConnectionError:
        # В данном случае мы рекомендуем повторить запрос позднее
        print("Произошла ошибка соединения с сервером API")
        # Принудительный выход из цикла
        break

    # Если возникла какая-либо другая ошибка
    except:
        # В данном случае мы рекомендуем проанализировать действия приложения
        print("Произошла непредвиденная ошибка")
        # Принудительный выход из цикла
        break


# ___________________________
# 2. Готовим данные для GA

data_from_direct = pd.read_csv("my_file.txt", header=1, sep="	",dtype={"CampaignId": str, "AdGroupId": str, "AdId": str, "CriterionId": str})

PATH_TO_SERVICE_ACCOUNT_KEY = '##################'  # Указать привязанный сервисный аккаунт из консоли файл
GA_PROPERTY_ID = '#############'  # Указать ID представления
GA_DATA_SET_ID = '#############'  # Указать дата сет ID

GA_CSV_FILEPATH = "dataframe.csv"


# Функция, которая очищает приписки от placement1, и заменяет значения none на Яндекс
def clean_placement(row):
    list = ["src_", "|dt"]
    row = str(row["placement1"])
    for i in list:
        row = row.replace(i, "")
    if row == "none":  # Нулевые значения означают что это был поиск Яндекса
        row = "Яндекс"
    return row


# Функция, которая очищает приписки от Device1
def clean_device(row):
    list = ["dt_", "|"]
    row = str(row["Device1"])
    for i in list:
        row = row.replace(i, "")
    return str(row)


def clean_keyword(row):
    list = ["\"", "!", "+", "[", "]"]
    row = str(row["key1"])
    for i in list:
        row = row.replace(i, "")
    return str(row)

#формируем utm_campaign
def build_campaign(row):
    campaign_id = str(row["CampaignId"])
    campaign_name = str(row["CampaignName"])
    source_type = str(row["AdNetworkType"]).lower()
    if source_type == "ad_network":
        source_type = "network"
    return "cid_" + campaign_id + "|cname_" + campaign_name + "|srct_" + source_type

#формируем utm_content
def build_content(row):
    phrase_id = str(row["CriterionId"])
    gbid = str(row["AdGroupId"])
    ad_id = str(row["AdId"])
    check_type = str(row["CriterionType"])
    if check_type == "KEYWORD":
        retargeting_id = ""
    elif check_type == "FEED_FILTER" or check_type == "WEBPAGE_FILTER" or check_type == "RETARGETING":
        retargeting_id = row["Criterion"]
    adtarget_id = str(row["CriterionId"])
    check_placement = str(row["Placement"])
    if check_placement == "Яндекс":
        source = "none"
    else:
        source = str(row["Placement"])
    device = str(row["Device"]).lower()
    region_name = str(row["TargetingLocationName"])
    return "gid_" + gbid + "|ad_" + ad_id + "|ret_" + retargeting_id + "|tgt_" + adtarget_id + "|src_" + source + "|dt_" + device + "|gname_" + region_name

#формируем utm_keyword
def build_keyword(row):
    phrase_id = str(row["CriterionId"])
    check_type = str(row["CriterionType"])
    if check_type == "KEYWORD":
        keyword = str(row["key"])
        return "key_" + keyword + "|ph_" + phrase_id
    elif check_type == "FEED_FILTER" or check_type == "WEBPAGE_FILTER" or check_type == "RETARGETING":
        return "key_|ph_" + phrase_id


# Подготавливаем дата фрейм Директа
data_from_direct["Clicks"] = data_from_direct["Clicks"].fillna(0)
data_from_direct["key1"] = data_from_direct["Criterion"].str.split(" -", expand=True)[0].str.extract("(.*)")
data_from_direct["key"] = data_from_direct.apply(clean_keyword, axis=1)
data_from_direct["Cost"] = data_from_direct["Cost"].fillna(0)
data_from_direct = data_from_direct.dropna()
# Подготавливаем метрики Аналитикса
data_from_direct["ga:date"] = data_from_direct["Date"].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').strftime("%Y%m%d"))
data_from_direct["ga:source"] = "yandex"
data_from_direct["ga:medium"] = "cpc"
data_from_direct["ga:campaign"] = data_from_direct.apply(build_campaign, axis=1)
data_from_direct["ga:adGroup"] = data_from_direct.apply(lambda x: str(x["AdGroupId"]), axis=1)
data_from_direct["ga:adContent"] = data_from_direct.apply(build_content, axis=1)
data_from_direct["ga:keyword"] = data_from_direct.apply(build_keyword, axis=1)
data_from_direct["ga:adCost"] = data_from_direct.apply(lambda x: x["Cost"] / 1000000, axis=1)  # Делим расходы на 1 000 000, потому что так выгружается из АПИ Директа
data_from_direct["ga:adClicks"] = data_from_direct["Clicks"].astype("int64")
data_from_direct["ga:Impressions"] = data_from_direct["Impressions"].astype("int64")

df = data_from_direct[["ga:date", "ga:source", "ga:medium", "ga:campaign", "ga:adGroup", "ga:adContent", "ga:keyword", "ga:adCost",
     "ga:adClicks", "ga:Impressions"]]

df.to_csv("dataframe.csv", index=False, encoding="utf-8")

# ___________________________
# 3. Импортируем данные в GA

def get_ga_service(api_name, api_version, scopes, key_file_location):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        key_file_location, scopes=scopes)

    # Build the service object.
    service = build(api_name, api_version, credentials=credentials)

    return service


def upload_cost_data_to_ga(service, ga_property_id, ga_data_set_id, path_to_csv):
    media = MediaFileUpload(path_to_csv,mimetype='application/octet-stream',resumable=False)

    service.management().uploads().uploadData(
        accountId=ga_property_id.split('-')[1],
        webPropertyId=ga_property_id,
        customDataSourceId=ga_data_set_id,
        media_body=media).execute()


def main():
    try:
        scope = ['https://www.googleapis.com/auth/analytics.edit']
        service = get_ga_service('analytics', 'v3', scope, PATH_TO_SERVICE_ACCOUNT_KEY)
        upload_cost_data_to_ga(service, GA_PROPERTY_ID, GA_DATA_SET_ID, GA_CSV_FILEPATH)
        print('Cost data is uploaded to {}.'.format(GA_PROPERTY_ID))
    except Exception as e:
        print("Something wrong... {}".format(e))


if __name__ == '__main__':
    main()
