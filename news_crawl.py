#-*-encoding:utf=8-*-

import re
import html
import pymysql
import requests
from tqdm import tqdm
from time import sleep
from bs4 import BeautifulSoup

from dartdb import DartDb

header_api = {
    'accept': 'application/json',
    'X-Naver-Client-Id': 'aNf8wmhx9JDXDGEwqihl',
    'X-Naver-Client-Secret': 'zWTwoDIiLI'
}

major_urls = {
    'thebell': 'thebell.co.kr',
    'chosun': 'news.chosun.com',
    'jungang': 'news.joins.com',
    'yonhap': 'yonhapnews.co.kr',
    'junja': 'etnews.com',
    'hangook': 'hankookilbo.com'
}


# naver api 사용해서 데이터 가져오기.
def get_json(keyword, display=100, start=1):
    url = 'https://openapi.naver.com/v1/search/news.json?query={0}&display={1}&start={2}&sort=date'.format(keyword, str(display), str(start))
    req = requests.get(url, headers=header_api, allow_redirects=False)
    if req.status_code == 200:
        return req.json()
    else:
        print(req.text)
        raise Exception('Error on calling Naver API : request code {}'.format(req.status_code))


# 네이버 기사 내용 수집.
def get_content_naver(link):
    type = 'news'
    req = requests.get(link, allow_redirects=False)
    if req.status_code == 301:
        new_url = req.headers['location']
        if 'entertain' in new_url:
            type = 'entertain'
        req = requests.get(new_url)
    elif req.status_code == 302:
        new_url = 'https://news.naver.com' + req.headers['location']
        if 'entertain' in new_url:
            type = 'entertain'
        req = requests.get(new_url)
    elif req.status_code == 200:
        type = 'news'
    bs = BeautifulSoup(req.text, 'lxml')

    if type == 'news':
        content_div = bs.find('div', id='articleBodyContents')
    elif type == 'entertain':
        content_div = bs.find('div', id='articeBody')

    for script in content_div(['script']):
        script.decompose()
    content = content_div.get_text().strip()
    return content


# 더벨(thebell.co.kr) 수집
def get_content_thebell(link):
    header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'}
    req = requests.get(link, headers=header)
    bs = BeautifulSoup(req.content.decode('utf-8', 'replace'), 'lxml')
    try:
        content_div = bs.find('div', id='article_main')
        content = content_div.get_text().strip()
    except Exception as exc:
        try:
            pattern = "'https?://(\w*:\w*@)?[-\w.]+(:\d+)?(/([\w/_.]*(\?\S+)?)?)?'"
            new_url = re.search(pattern, str(bs)).group().replace("'", '')
            req = requests.get(new_url)
            bs = BeautifulSoup(req.content.decode('utf-8', 'replace'), 'lxml')
            content_div = bs.find('div', id='DivArticleContent')
            content = content_div.get_text().strip()
        except:
            content = ''
    return content


# 뉴스 1개에 대해 dict로 정리.
def make_dict(raw_json, keyword, content=''):
    result = dict()
    result['keyword'] = keyword
    result['title'] = html.unescape(raw_json['title']).replace('<b>', '').replace('</b>', '').replace('"', '\'').replace('`', "'")
    result['link'] = html.unescape(raw_json['link']).replace('"', '\'').replace('`', "'")
    result['id'] = '{0} :: {1}'.format(keyword, result['link'])
    result['originallink'] = raw_json['originallink']
    if content == '':
        result['content'] = raw_json['description'].replace('"', "'")
    else:
        result['content'] = content.replace('"', "'")
    result['date'] = raw_json['pubDate']
    return result


# 상세 내용 수집
def main(keyword, **kwargs):
    '''
    :param keyword: 조회할 검색어
    :param kwargs: count, func
    :return:
    '''
    length = get_json(keyword)['total']
    if length % 100 > 0:
        reqs = int(length / 100) + 1
    else:
        reqs = int(length / 100)
    if reqs > 9:
        reqs = 9

    cnt = 0
    temp_list = list()
    status = True
    for i in range(reqs + 1):
        start = i * 100 + 1
        if not status:
            break
        for item in tqdm(get_json(keyword, start=start)['items'], desc='{} 수집 {}/{}'.format(keyword, i, reqs)):
            if cnt > kwargs['count']:
                status = kwargs['func'](temp_list)
                if not status:
                    break
                temp_list.clear()
                cnt = 0
            if '//news.naver.com' in item['link']:
                content = get_content_naver(item['link'])
                temp_list.append(make_dict(item, keyword, content))
                cnt += 1
            elif major_urls['thebell'] in item['link']:
                content = get_content_thebell(item['link'])
                temp_list.append(make_dict(item, keyword, content))
                cnt += 1


def data_sql(data):
    dart.get_cursor()
    for iter in data:
        sql = 'INSERT INTO `naver_news` VALUES("{}", "{}", "{}", "{}", "{}", "{}", "{}");' \
            .format(iter['id'], iter['title'], iter['keyword'], iter['link'], iter['originallink'], iter['content'], iter['date'])
        try:
            dart.curs.execute(sql)
        except pymysql.err.IntegrityError as exc:
            if '1062' in str(exc):
                print(sql)
                return False
        except Exception as exc:
            pass
    dart.close_cursor()
    return True


dart = DartDb()
#dart._debug_clear_table('naver_news')
main('LG전자', count=50, func=data_sql)
dart.get_cursor()
