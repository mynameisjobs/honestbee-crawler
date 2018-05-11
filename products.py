# coding: utf-8
import re

import requests

def get_product(store_url):
    store_name = store_url.split('/')[-1]
    resp = requests.get(store_url)
    products = re.findall('href="(/en/groceries/stores/{}/products/.+?)"'.format(store_name), resp.text)
    print(products)

if __name__ == '__main__':
    get_product('https://www.honestbee.tw/en/groceries/stores/laoshen-zaizai')
