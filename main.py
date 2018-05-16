# coding: utf-8
import os
import re
import json
import datetime

from celery import Celery, group, chain
import requests
import models
import peewee
from playhouse.shortcuts import model_to_dict

app = Celery('main', broker='pyamqp://guest@%s//'%(os.getenv('RBMQ_HOST')))
app.conf.task_routes = {
        'main.to_postgres': {'queue': 'to_postgres'},
        'main.to_es':       {'queue': 'to_es'}
        }
#brands_dict = {row.id: model_to_dict(row) for row in models.Brands}
HOST = 'https://www.honestbee.tw'
ES_HOST = os.getenv('ES_HOST')


@app.task
def process_brands(brands):
    models.Brands.drop_table()
    models.Brands.create_table()
    (models.Brands
            .insert_many(brands)
            .execute())

def get_stores():
    resp = requests.get(HOST + '/zh-tw/groceries/stores')
    window_data = json.loads(re.findall('window.__data=(.+?); window.__i18n', resp.text, re.DOTALL)[0])
    brands = window_data['groceries']['brands']['byId']
    brands = [{k.lower():v for k,v in row.items()} for kk, row in brands.items()]
    stores = [brand['storeid'] for brand in brands]
    process_brands(brands)
    #print("got brands")
    return stores

def dump_page(url):
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
    return resp

def prase_products(data, store_id, department_id, category_id, dt):
    if data.get('products'):
        products = [{k.lower():v for k,v in row.items()} for row in data.get('products')]
        for row in products:
            # if brands_dict.get(int(brand_id),{}).get('slug'):
            #     row['url'] = "https://www.honestbee.tw/zh-TW/groceries/stores/{store_slug}/products/{product_id}".format(
            #             store_slug=brands_dict.get(int(brand_id)).get('slug'),
            #             product_id=row['id'])
            row['product_id'] = int(row['id'])
            row['id'] = row['id']
            row['store_id'] = int(store_id)
            row['category_id'] = category_id
            row['department_id'] = department_id
            row['dt'] = dt
        to_postgres.delay(products)
        [to_es.delay(x) for x in products]

@app.task
def get_products(payload):
    department_id = payload['department_id']
    category_id = payload['category_id']
    store_id = payload['store_id']
    page = payload.get('page', 1)
    # url = "https://www.honestbee.tw/api/api/stores/{}?sort=price:desc&page={}".format(store_id, page) #&fields[]=departments"
    url = "https://www.honestbee.tw/api/api/departments/{department_id}?\
            categoryIds%5B%5D={category_id}&\
            sort=ranking&\
            page={page}&\
            storeId={store_id}&\
            fields%5B%5D=categories".format(department_id=department_id,
                                            category_id=category_id,
                                            page=page,
                                            store_id=store_id)
    data = dump_page(url).json()
    prase_products(data, store_id, department_id, category_id, payload['dt'])
    #print(json.dumps(resp.json(), ensure_ascii=False))
    payload['data'] = data
    return payload

@app.task
def get_total_pages(payload):
    payload['total_pages'] = payload['data']['meta']['tatal_pages']
    return payload

@app.task
def dispatch_rest_of_get_products(payload):
    total_pages = payload['total_pages']
    if total_pages == 1:
        return
    for x in range(2, total_pages + 2):
        payload['page'] = x
        get_products.delay(payload)

@app.task
def dispatch_get_products(payload):
    process_department = payload['process_department']
    process_category = payload['process_category']
    for category_id,category in process_category.items():
        payload['department_id'] = int(category['department_id'])
        print(payload)
        payload['store_id']     = int(process_department[str(payload['department_id'])]['store_id'])
        payload['category_id'] = category_id
        if 'process_category' in payload:
            del payload['process_category']
            del payload['process_department']
        chain(get_products.s(payload) |
                get_total_pages.s() |
                dispatch_rest_of_get_products.s())()

@app.task
def to_es(data):
    d = {
        "title": data['title'],
        "price": data['price'],
        "store_id": data['store_id'],
        "product_id": data['product_id'],
        "imageurl": data['imageurl'],
        "size": data['size'],
        "url": data.get('url'),
        "updated_at": data['dt'],
        "status": data['status'],
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

@app.task
def store_department(department):
    del department['categories']
    models.Departments.create(**department)

@app.task
def process_department(payload):
    payload['process_department'] = {}
    for department in payload.get('departments'):
        department = {k.lower():v for k,v in department.items()}
        department['store_id'] = int(payload.get('store_id'))
        store_department.delay(department)
        department['id'] = int(department['id'])
        payload['process_department'][int(department['id'])] = department
    return payload

@app.task
def store_category(category):
    models.Categories.create(**category)

@app.task
def process_category(payload):
    payload['process_category'] = {}
    for department in payload.get('process_department').values():
        # print(department)
        for category in department.get('categories'):
            category = {k.lower():v for k,v in category.items()}
            category['department_id'] = int(department['id'])
            store_category.delay(category)
            category['id'] = int(category['id'])
            payload['process_category'][int(category['id'])] = category
    del payload['departments']
    return payload

@app.task
def get_directory(payload):
    store_id = int(payload['store_id'])
    dt       = payload['dt']

    url = "https://www.honestbee.tw/api/api/stores/{}/directory".format(store_id)
    headers = json.loads(r'''{
        "Accept": "application/vnd.honestbee+json;version=2",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-TW",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Cookie": "connect.sid=s%3A608o0mr0PaNmG0og-XBxSVwcYdlnbeht.Y5Ec5ICbb7NXCLmnODO1ASJc7JSNYQHg%2F3XXtZK1DY8; ajs_user_id=null; ajs_group_id=null; ajs_anonymous_id=%227c983032-927c-4214-8c4e-d306827ac44a%22; _ga=GA1.2.937310059.1526023442; ab.storage.deviceId.b0a7b43e-b927-41a2-8f56-8183f42abb09=%7B%22g%22%3A%222b83d6d3-2db7-27f2-4892-409d5139feae%22%2C%22c%22%3A1526023442611%2C%22l%22%3A1526023442611%7D; __ssid=34fc6072-d449-4851-aca0-a951b9e8bd7a; appier_utmz=%7B%7D; _atrk_siteuid=_nwI401QSL6fIj_O; _omappvp=E3IXL25nSz6PJX8mV07BQqTa2hXrMssF5zsp4CTEUnNFxeiN4HTnd3dVv1lsTFPoUS6JQ1xx3Zj5cmYOnf4Es3gzogQvSxI9; __zlcmid=mMhKeI2JX9LkJG; appier_uid_2=QA2V4hxPSE2lnYR44eXsv7; _gid=GA1.2.935251516.1526294893; cto_lwid=e5953c02-8ed8-4168-a931-f54031873972; _atrk_ssid=YpAsjIrqnZwwCSTMIRBmdj; appier_tp=; appier_pv_counternL1t5crDJLr4VSH=4; _uetsid=_uet85e08d3a; ab.storage.sessionId.b0a7b43e-b927-41a2-8f56-8183f42abb09=%7B%22g%22%3A%22ad61c68d-563e-54f5-2e47-c91611935c55%22%2C%22e%22%3A1526442516340%2C%22c%22%3A1526440025884%2C%22l%22%3A1526440716340%7D; _atrk_sessidx=15; _honestbees_session=S3oyaERudzN2SVhHOXBzandHWkhoOUZOd3F4YjFWbU85Nkx5RzZ3NTlnNkU3RTdVSzAzaE1oeFFqbmZJYllPREpBMUc1TFdNVGhWWjBXeTFpTkVlOTVOTUdjS2g4RnBGbGVOaEVMOTJRZnY3blJUNEdaUU4wbFNQaDJucS83MFFLTTIzOXVEbkkvU1ZaeEtGVW5MU29nPT0tLWZjMjExT09FaWVnN2xSVzNUQUl4Smc9PQ%3D%3D--ea719a7aa991c288bc691711fcba6ab95a4fb559",
        "Host": "www.honestbee.tw",
        "Pragma": "no-cache",
        "Referer": "https://www.honestbee.tw/zh-TW/groceries/stores/carrefour/departments/20272/categories/136156",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
        "x-honestbee-cache": "enable"
    }''')

    resp = requests.get(url, headers=headers)
    payload['departments'] = resp.json().get('departments')
    return payload

if __name__ == '__main__':
    payload = {}
    dt = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=8))).isoformat()
    for store_id in get_stores():
        # chain(get_products.s((store_id,dt)) |
        #         get_total_pages.s() |
        #         dispatch_get_products.s())()
        payload['store_id'] = int(store_id)
        payload['dt'] = dt
        chain(get_directory.s(payload) |
                process_department.s() |
                process_category.s() |
                dispatch_get_products.s()
                )()

