#-*-encoding:utf-8-*-

import re
import html
import json
import pymysql
import logging
import requests
import configparser
from tqdm import tqdm
from time import sleep
from bs4 import BeautifulSoup
from openpyxl import load_workbook

from dartdb import DartDb

header_api = {
    'accept': 'application/json',
    'X-Naver-Client-Id': '',
    'X-Naver-Client-Secret': ''
}

major_urls = {
    'thebell': 'thebell.co.kr',
    'chosun': 'news.chosun.com',
    'jungang': 'news.joins.com',
    'yonhap': 'yonhapnews.co.kr',
    'junja': 'etnews.com',
    'hangook': 'hankookilbo.com'
}
company_list = list()


# 기업명 로딩
def load_targets(filename):
    # Get company data
    company_file = load_workbook('./setting/{}'.format(filename))
    company_sheet = company_file.worksheets[0]
    for row in company_sheet.rows:
        if row[0].value:
            company_list.append(row[0].value.replace(u'\xa0', u' ').strip())
    company_list.pop(0)


# naver api 사용해서 데이터 가져오기.
def get_json(keyword, display=100, start=1, depth=1):
    url = 'https://openapi.naver.com/v1/search/news.json?query={0}&display={1}&start={2}&sort=date'.format(keyword, str(display), str(start))
    req = requests.get(url, headers=header_api, allow_redirects=False)

    if req.status_code == 200:
        try:
            json_data = req.json()
            return json_data
        except json.decoder.JSONDecodeError:
            if depth > 10:
                return {'items': list()}
            return get_json(keyword, display, start, depth + 1)
    else:
        try:
            if depth > 10:
                raise Exception('Error on calling Naver API : request code {}'.format(req.status_code))
            return get_json(keyword, display, start, depth + 1)
        except Exception:
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
    for i in range(reqs + 1):
        start = i * 100 + 1
        for item in tqdm(get_json(keyword, start=start)['items'], desc='\'{}\' 상세 수집 ({}/{})'.format(keyword, i, reqs), ncols=100, leave=False):
            try:
                if cnt > kwargs['count']:
                    status = kwargs['func'](temp_list)
                    if not status:
                        return
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
            except Exception as exc:
                logger.info('[ERROR] 수집 에러. ' + str(exc))
        if len(temp_list) > 0:
            kwargs['func'](temp_list)
            temp_list.clear()


def data_sql(data):
    dart.get_cursor()
    for iter in data:
        sql = 'INSERT INTO `{}` VALUES("{}", "{}", "{}", "{}", "{}", "{}", "{}");' \
            .format(table_name, iter['id'], iter['title'], iter['keyword'], iter['link'], iter['originallink'], iter['content'], iter['date'])
        try:
            dart.curs.execute(sql)
        except pymysql.err.IntegrityError as exc:
            if '1062' in str(exc):
                return False
        except Exception as exc:
            pass
    dart.close_cursor()
    return True


if __name__ == "__main__":
    # LOGGER
    logger = logging.getLogger('notice')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('[SYSTEM] %(asctime)s :: %(message)s')
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    # setting
    config = configparser.ConfigParser()
    config.read('./setting/news_config.ini')
    header_api['X-Naver-Client-Id'] = config['API']['X_NAVER_CLIENT_ID']
    header_api['X-Naver-Client-Secret'] = config['API']['X_NAVER_CLIENT_SECRET']
    file_name = config['COMPANY']['FILE_NAME']
    db_header = {
        'host': config['DATABASE']['HOST'],
        'port': int(config['DATABASE']['PORT']),
        'user': config['DATABASE']['USERNAME'],
        'password': config['DATABASE']['PASSWORD'],
        'db': config['DATABASE']['SCHEMA'],
        'charset': config['DATABASE']['CHARSET']
    }
    table_name = config['DATABASE']['TABLE_NAME']

    load_targets(file_name)

    dart = DartDb(db_header)
    idx = 1
    for company in tqdm(company_list, ncols=100, leave=True, desc='전체 수집'):
        main(company, count=30, func=data_sql, now=idx, length=len(company_list))
        idx += 1
