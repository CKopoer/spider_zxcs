import requests
from lxml.html import fromstring
from logger import logger
import re
import time
from functools import wraps
import pandas as pd
from multiprocessing import Pool
import glob
import os

review = ['仙草', '粮草', '干草', '枯草', '毒草']
# ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'
ua = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
re1 = re.compile(r'^《(.*?)》.*?作者[：:] ?(.*?)$')
re2 = re.compile(r'.*?(\d+)$')
re3 = re.compile(r'.*?(\d+[.]\d+)')

s = requests.session()

def get_wrapper(fun):
    @wraps(fun)
    def wrapper(*args, **kwargs):
        def inner():
            try:
                logger.info('[开始爬取]' + args[0])
                r = fun(*args, **kwargs)
            except:
                logger.error('[爬取异常]' + args[0])
                r = ''
            return r
        retry = 5
        r = inner()
        while not r and retry > 0:
            r = inner()
            retry -= 1
            time.sleep(2)
        if r and r.status_code == 200:
            logger.info('[爬取成功]' + args[0])
        # r.encoding = r.apparent_encoding
        return r
    return wrapper

s.get = get_wrapper(s.get)

def parse_tree(tree):
    all_items = []
    all_pl = tree.xpath('//*[@id="plist"]')
    for pl in all_pl:
        item = dict()
        dt = pl.xpath('dt/a')
        dd1 = pl.xpath('dd[1]')
        dd2 = pl.xpath('dd[2]')[0].getchildren()
        title = re1.match(dt[0].text).group(1)
        author = re1.match(dt[0].text).group(2)
        code = re2.match(dt[0].xpath('@href')[0]).group(1)

        abstract = dd1[0].text.strip()
        size = 0
        
        # abstract = abstract.split('正版订阅')[0].strip()
        cate1 = dd2[0].text
        if dd2[1].text:
            cate2 = dd2[1].text
        elif dd2[1].tail:
            cate2 = dd2[1].tail.split()[0]
        else:
            cate2 = None
        item['title'] = title
        item['author'] = author
        item['code'] = code
        item['size'] = size
        item['abstract'] = abstract
        item['cate1'] = cate1
        item['cate2'] = cate2
        all_items.append(item)
    return all_items


def get_review(item, code, host):
    head = 'https://' if len(host) > 7 else 'http://'
    url2 = head + host + '/content/plugins/cgz_xinqing/cgz_xinqing_action.php?action=mood&id={code}'
    logger.info('尝试获取' + item['title'] + '的评价数据')
    html = s.get(url2.format(code=code), timeout=30, headers={'User-Agent': ua, 'host':host})
    if not html:
        for i in range(len(review)):
            item[review[i]] = 0
        return
    temp = list(map(int, html.text.split(',')))
    for i in range(len(temp)):
        item[review[i]] = temp[i]

    item['voter'] = 0
    item['aggregate'] = 0
    for idx, vote in enumerate(review):
        num = item[vote]
        item['voter'] += num
        item['aggregate'] += (5 - idx) * num
    item['score'] = 0 if (not item['voter']) else round(item['aggregate'] / item['voter'], 4)

def get_downloadLink(item, code, host):
    head = 'https://' if len(host) > 7 else 'http://'

    url3 = head + host + '/download.php?id={code}'

    logger.info('尝试获取' + item['title'] + '的下载链接')
    html = s.get(url3.format(code=code), timeout=30, headers={'User-Agent': ua, 'host':host})
    try:
        html = fromstring(html.text)
        link = html.xpath('//span[@class="downfile"]/a')
        item['link1'] = link[0].xpath('@href')[0]
        item['link2'] = link[1].xpath('@href')[0]
    except:
        item['link1'] = ''
        item['link2'] = ''
    
    
    

def has_next(tree):
    try:
        next_node = tree.xpath('//div[@id="pagenavi"]/span/following-sibling::*[1]')
        if next_node and next_node[0].tag == 'a':
            logger.info('存在下一页: ' + next_node[0].text)
            return next_node[0].xpath('@href')[0]
    except:
        return ''




def main(url, host, round=1000):
    urls = [url]
    novels = []
    while urls and round > 0:
        u = urls.pop()
        logger.info('开始爬取列表页面：' + str(u))
        html = s.get(u, timeout=30, headers={'User-Agent': ua, 'host':host})
        if not html:
            continue
        tree = fromstring(html.text)
        items = parse_tree(tree)
        for item in items:
            get_review(item, item['code'], host)
            # get_downloadLink(item, item['code'], host)
        novels.append(items)
        # print(items)
        next_u = has_next(tree)
        round -= 1
        if next_u:
            time.sleep(2)
            urls.append(next_u)
    return novels

def to_csv(novels, filename):
    if not novels:
        return
    data = pd.concat(map(pd.DataFrame, novels))
    data.to_csv(filename, index=False, encoding="utf_8_sig")

def mkdir(path):
    folder = os.path.exists(path)
    if not folder:                   #判断是否存在文件夹如果不存在则创建为文件夹
        os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径
        print( "---  new folder...  ---")
        print( "---  OK  ---")
    else:
        print( "---  There is this folder!  ---")

def get_csv(url, filename, host):
    novels = main(url, host)
    to_csv(novels, filename)

if __name__ == '__main__':
    urlsForMe = {
            'http://zxcs.me/sort/23': '都市娱乐.csv', 
            'http://zxcs.me/sort/25': '武侠仙侠.csv', 
            'http://zxcs.me/sort/26': '奇幻玄幻.csv', 
            'http://zxcs.me/sort/27': '科幻灵异.csv', 
            'http://zxcs.me/sort/28': '历史军事.csv',
            'http://zxcs.me/sort/29': '竞技游戏.csv',
            'http://zxcs.me/sort/55': '二次元.csv'
            }
    urlsForInfo = {
            'https://www.zxcs.info/sort/3': '都市娱乐.csv', 
            'https://www.zxcs.info/sort/4': '武侠仙侠.csv', 
            'https://www.zxcs.info/sort/8': '奇幻玄幻.csv', 
            'https://www.zxcs.info/sort/11': '科幻灵异.csv', 
            'https://www.zxcs.info/sort/14': '历史军事.csv',
            'https://www.zxcs.info/sort/17': '竞技游戏.csv',
            'https://www.zxcs.info/sort/20': '二次元.csv'
            }

    Time = time.strftime("%Y-%m-%d", time.localtime())
    path1 = "./result/" + Time + "/Me/"
    path2 = "./result/" + Time + "/Info/"
    mkdir(path1)
    mkdir(path2)

    # MultiProcess
    # p = Pool(len(urlsForMe) + len(urlsForInfo))
    # for url, filename in urlsForMe.items():
    #     p.apply_async(get_csv, args=(url, path1 + filename, 'zxcs.me'))

    # for url, filename in urlsForInfo.items():
    #     p.apply_async(get_csv, args=(url, path2 + filename, 'www.zxcs.info'))

    # print('Waiting for all subprocesses done...')
    # p.close()
    # p.join()
    # print('All subprocesses done.')


    # Test Code
    for url, filename in urlsForInfo.items():
        get_csv(url, path2 + filename, 'www.zxcs.info')






    print('Start Merge zxcs.me.')
    all_filenames = [i for i in glob.glob(path1 + '*.csv')]
    #在列表中合并所有文件
    if not all_filenames:
        print("No Files in zxcs.me.")
    else:
        combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
        #导出 csv
        combined_csv.to_csv(path1 + "Book.csv", index=False, encoding='utf-8-sig')
        combined_csv.to_excel(path1 + 'Me.xlsx', index=False, sheet_name='data', encoding='utf-8-sig')


    print('Start Merge zxcs.info.')
    all_filenames = [i for i in glob.glob(path2 + '*.csv')]
    #在列表中合并所有文件
    if not all_filenames:
        print("No Files in zxcs.info.")
    else:
        combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
        #导出 csv
        combined_csv.to_csv(path2 + "Book.csv", index=False, encoding='utf-8-sig')
        combined_csv.to_excel(path2 + 'Info.xlsx', index=False, sheet_name='data', encoding='utf-8-sig')