import sys
import requests
import lxml.html
import urllib.request
import time
import numpy as np
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pymongo import MongoClient


BASE_ULR = 'https://suumo.jp'

# 桜新町ので検索結果ページのURL
SAKULASHIMACHI_URL = 'https://suumo.jp/jj/chintai/ichiran/FR301FC005/?gclid=CjwKCAjwqIiFBhAHEiwANg9sziL7fKlU8DAVWubuiq52Fkns-Mm_mLmoJiWmfdYIXvQ3eb76GpC-HxoCNS0QAvD_BwE&rn=0230&bs=040&gclsrc=aw.ds&ipao9739=&ra=013&ipao9738=&ipao9731=&ipao9730=&ipao9733=&vos=op4014adwstw012000000zzz_01x0000000-xe_kwd-302722255670:cr-355846349919:sl-:adg-99960440636:dev-c:acc-4461499012:cam-9789266115&ipao9732=2259732283885045897&ipao9735=&ipao9734=&ipao9737=&ipao9736=&ipao9723=99960440636&ipao9724=1009307&ipao9725=1009307&ipao9726=&ipao9740=&ar=030&ipao9727=&ipao9728=&ipao9729=&ek=023016140&ipao9741=&et=15'






def main(page=1):
    
    # ページが155ページあるため事前にクエリパラメータを作っておく(デフォルトは1ページから)
    PAGE_RANGE = np.arange(int(page), 156, 1)
    QUERY = '&page='

    # mongodb接続
    client, collection = connect_mongodb()

    for j in PAGE_RANGE:

        response = requests.get(SAKULASHIMACHI_URL + QUERY + str(j))
        root = lxml.html.fromstring(response.content)
        
        # 詳細ページのリンクを取得
        elem_detail_url_array = root.xpath('//a[@class="js-cassetLinkHref"]')

        # 詳細ページをスクレイピング
        for i in range(0, len(elem_detail_url_array)):

            detail_data = {} 

            # 物件名
            building_name = elem_detail_url_array[i].text
            url = urljoin(BASE_ULR, elem_detail_url_array[i].get('href'))
            detail_data['building_name'] = building_name
            detail_data['url'] = url
            detail_data = detail_page_scraping(url, detail_data=detail_data)

            # mongodbへ格納
            collection.insert_one(detail_data)
            print(f'j:{j}, i:{i},| {building_name} | {url}')

            # スリープ
            time.sleep(1)

        

    client.close()



def connect_mongodb():
    client =  MongoClient("mongodb://127.0.0.1:27017", username='pyuser', password='ellegarden', authSource='mydb')
    db = client.mydb
    collection = db.suumo
    return client, collection


def detail_page_scraping(url, detail_data):

    # 詳細ページ取得
    response = requests.get(url)
    root = lxml.html.fromstring(response.content)

    # 賃料
    rant = root.xpath('//*[@class="property_view_main-emphasis"]')[0].text.split('\t')[-1]
    detail_data["賃料"] = rant
    
    # 管理費・共益費から築年数までをスクレイピング
    elem_key_array = root.xpath('//div[@class="property_data-title"]')
    elem_value_array = root.xpath('//div[@class="property_data-body"]')
    elem_len   = len(root.xpath('//div[@class="property_data-title"]'))
    for i in range(0, elem_len):
        key   = elem_key_array[i].text 
        value = elem_value_array[i].text.split('\t')[-1]
        detail_data[key] = value

    # 敷金/礼金のみHTMLのフォーマットが異なるため別途取得 
    key = root.xpath('//div[@class="property_data-title"]')[1].text
    elem_len = len(root.xpath('//div[@class="property_data-body"]/span'))
    tmp = []
    for i in range(0, elem_len):
        tmp.append(root.xpath('//div[@class="property_data-body"]/span')[i].text.split('\t')[-1])
        
    else:
        value = ''
        for j in range(0, len(tmp)):
            value += tmp[j]

    detail_data[key] = value


    # アクセス
    key = root.xpath('//*[@class="property_view_detail-header-title"]')[2].text
    elem_value = root.xpath('//*[@class="property_view_detail-body" and contains(.,"分")]/div')
    elem_len = len(root.xpath('//*[@class="property_view_detail-body" and contains(.,"分")]/div'))
    accsess_array = []
    for k in range(0, elem_len):
        accsess_array.append(elem_value[k].text.split('\t')[-1])
    else:
        detail_data[key] = accsess_array

    # 所在地
    key   = root.xpath('//*[@class="property_view_detail-header-title"]')[3].text
    value = root.xpath('//*[@class="property_view_detail-text"]')[-1].text.split('\t')[-1]
    detail_data[key] = value

    # 部屋の特徴・設備
    key = root.xpath('//*[@id="contents"]/h2/span')[0].text
    value = root.xpath('//*[@id="bkdt-option"]//*/li')[0].text
    detail_data[key] = value

    # 物件概要
    key = root.xpath('//*[@id="contents"]/h2/span')[1].text

    # 物件概要(htmlがtableなのでBeautifulSoupを使用する)
    html = urllib.request.urlopen(url)
    soup = BeautifulSoup(html, "lxml")


    # tableの要素をkey, valueの配列として作成
    table = soup.find("table", class_="data_table table_gaiyou")
    key_array = []
    value_array = []
    detail_data_tmp = {}
    for row in table.find_all("tr"):

        # tableにthタグがなかった箇所があるためNoneで暫定対応
        if row.find("th") == None:
            continue
        key_array.append(row.find("th").text)
        value_array.append(row.find("td").text.split('\t')[-1])

    else:
        # 物件概要の詳細データをdict型で作成
        for i in range(0, len(key_array)):
            detail_data_tmp[key_array[i]] = value_array[i]

    # 物件概要の詳細をリストとして作成し全体のdictに登録
    detail_data_tmp2 = []
    detail_data_tmp2.append(detail_data_tmp)

    detail_data[key] = detail_data_tmp2

    return detail_data


    



if __name__ == '__main__':
    if len(sys.argv) > 1:
        main(page=sys.argv[1])
    else:
        main()
