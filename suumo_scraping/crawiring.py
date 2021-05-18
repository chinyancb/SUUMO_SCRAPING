import requests
import lxml.html
from urllib.parse import urljoin
from pymongo import MongoClient


BASE_ULR = 'https://suumo.jp'

# 桜新町ので検索結果ページのURL
SAKULASHIMACHI_URL = 'https://suumo.jp/jj/chintai/ichiran/FR301FC005/?gclid=CjwKCAjwqIiFBhAHEiwANg9sziL7fKlU8DAVWubuiq52Fkns-Mm_mLmoJiWmfdYIXvQ3eb76GpC-HxoCNS0QAvD_BwE&rn=0230&bs=040&gclsrc=aw.ds&ipao9739=&ra=013&ipao9738=&ipao9731=&ipao9730=&ipao9733=&vos=op4014adwstw012000000zzz_01x0000000-xe_kwd-302722255670:cr-355846349919:sl-:adg-99960440636:dev-c:acc-4461499012:cam-9789266115&ipao9732=2259732283885045897&ipao9735=&ipao9734=&ipao9737=&ipao9736=&ipao9723=99960440636&ipao9724=1009307&ipao9725=1009307&ipao9726=&ipao9740=&ar=030&ipao9727=&ipao9728=&ipao9729=&ek=023016140&ipao9741=&et=15'




"""
client =  MongoClient("mongodb://127.0.0.1:27017", username='pyuser', password='ellegarden', authSource='mydb')
db = client.mydb
collection = db.suumo
collection.insert_one({'test': 'test2'})
client.close()
"""


def main():
    
    response = requests.get(SAKULASHIMACHI_URL)
    root = lxml.html.fromstring(response.content)
    
    # 詳細ページのリンクを取得
    elem_detail_url_array = root.xpath('//a[@class="js-cassetLinkHref"]')
    for i in range(0, len(elem_detail_url_array)):
        print(elem_detail_url_array[i].text)
        url = urljoin(BASE_ULR, elem_detail_url_array[i].get('href'))
        detail_data = detail_page_scraping(url)
        print(url)
        print(detail_data)
        



def detail_page_scraping(url):
    response = requests.get(url)
    root = lxml.html.fromstring(response.content)

    detail_data = {}
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


    return detail_data


    



if __name__ == '__main__':
    main()
