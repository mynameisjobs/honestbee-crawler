# coding: utf-8
import os
import re
import json
import datetime

from celery import Celery, group, chain
import requests
import models
import peewee

app = Celery('main', broker='pyamqp://guest@%s//'%(os.getenv('RBMQ_HOST')))
app.conf.task_routes = {
        'main.to_postgres': {'queue': 'to_postgres'},
        'main.to_es':       {'queue': 'to_es'}
        }
HOST = 'https://www.honestbee.tw'
ES_HOST = os.getenv('ES_HOST')

def get_stores():
    resp = requests.get(HOST + '/zh-tw/groceries/stores')
    window_data = json.loads(re.findall('window.__data=(.+?); window.__i18n', resp.text, re.DOTALL)[0])
    brands = window_data['groceries']['brands']['byId']
    print("got stores")
    return brands

@app.task
def get_products(storeId_and_dt, page=1):
    store_id, dt = storeId_and_dt
    url = "https://www.honestbee.tw/api/api/stores/{}?sort=price:desc&page={}".format(store_id, page) #&fields[]=departments"
    headers = json.loads(r'''{
        "Accept": "application/vnd.honestbee+json;version=2",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-TW",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Cookie": "connect.sid=s%3AMhmQ1nQHYL_69wnrjuJBgExp5Zv7zVqs.g6m01NNVv%2BOMh1K6KiTVPUOthSZQHjUxe%2F1Qvdt1hgU; ajs_user_id=null; ajs_group_id=null; ajs_anonymous_id=%22d9d3c3ec-2472-4fcf-9abb-df5fc9984c4b%22; _ga=GA1.2.1181326923.1525430384; appier_utmz=%7B%22csr%22%3A%22google%22%2C%22timestamp%22%3A1525430384%7D; _atrk_siteuid=Iv-rYwXkRnTLdhXM; ab.storage.deviceId.b0a7b43e-b927-41a2-8f56-8183f42abb09=%7B%22g%22%3A%22e3a19647-699c-e595-ba2f-d8a0654a2e14%22%2C%22c%22%3A1525430385018%2C%22l%22%3A1525430385018%7D; __ssid=35a0fbf6-9d19-4289-996c-0bfba46c026b; _omappvp=wWe6NRCEOaQW76xtU8lYAhNyQ83c86xtz3AufqEOoPbFWTLrGCAOMjcIyAIXudXtVhFwObcOGTbhrRbnSiBFk9Z1mxdan8uN; cto_lwid=54d4a608-82cb-4f31-9fd4-d52255987263; __zlcmid=mFhJbYVFxacoH1; appier_uid_2=yRlZz2OixSNUoKpocV5O6w; __zlcprivacy=1; _gid=GA1.2.925910841.1525969555; _atrk_ssid=l-QvgtIxWHyb3I7hxqS7jk; ab.storage.sessionId.b0a7b43e-b927-41a2-8f56-8183f42abb09=%7B%22g%22%3A%2205a932f9-e58c-abc1-8a26-7b5fcef60ffa%22%2C%22e%22%3A1526012004955%2C%22c%22%3A1526008764512%2C%22l%22%3A1526010204955%7D; appier_tp=; _atrk_sessidx=4; appier_pv_counternL1t5crDJLr4VSH=2; _uetsid=_uetf941f4d3",
        "Host": "www.honestbee.tw",
        "Pragma": "no-cache",
        "Referer": "https://www.honestbee.tw/zh-TW/groceries/stores/fresh-by-honestbee-tw?sort=price_desc",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
        "x-honestbee-cache": "enable"
    }''')

    resp = requests.get(url, headers=headers)
    data = resp.json()
    if data.get('products'):
        products = [{k.lower():v for k,v in row.items()} for row in data.get('products')]
        for row in products:
            row['product_id'] = row['id']
            row['id'] = "%s_%s"%(row['id'],store_id)
            row['brand_id'] = int(store_id)
            row['dt'] = dt
        to_postgres.delay(products)
        [to_es.delay(x) for x in products]
    #print(json.dumps(resp.json(), ensure_ascii=False))
    return (storeId_and_dt, data)

@app.task
def get_total_pages(storeId_and_dt_and_productData):
    storeId_and_dt, product_data = storeId_and_dt_and_productData
    return (storeId_and_dt, product_data['meta']['total_pages'])

@app.task
def dispatch_get_products(storeId_and_dt_and_totalPages):
    storeId_and_dt, total_pages = storeId_and_dt_and_totalPages
    if total_pages == 1:
        return
    for x in range(2, total_pages + 2):
        get_products.delay(storeId_and_dt, x)

@app.task
def to_es(data):
    d = {
        "title": data['title'],
        "price": data['price'],
        "brand_id": data['brand_id'],
        "product_id": data['product_id'],
        "imageurl": data['imageurl'],
        "size": data['size'],
        "updated_at": data['dt'],
        "currency": data['currency']
    }
    resp = requests.put('http://{}:9200/honestbee/main/{}'.format(ES_HOST, data['product_id']), headers={'content-type': 'application/json'},
            json=d)
    return resp.json()

@app.task
def to_postgres(data):
    #print(data)
    try:
        with models.database.atomic():
            models.Product.insert_many(data).execute()
    except peewee.IntegrityError:
        return "duplicate"

    # except Exception as e:
    #     print(e)
    #     print(data)


if __name__ == '__main__':
    dt = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=8))).isoformat()
    for store_id in get_stores():
        chain(get_products.s((store_id,dt)) |
                get_total_pages.s() |
                dispatch_get_products.s())()

