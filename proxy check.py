import requests
from bs4 import BeautifulSoup
import urllib

proxy_list = ['http://20.81.106.180:8888', 'http://202.73.51.234:80', 'http://51.195.201.93:80',
              'http://103.152.35.245:80', 'http://104.233.204.77:82', 'http://104.233.204.73:82',
              'http://209.141.55.228:80', 'http://206.253.164.122:80', 'http://139.99.237.62:80',
              'http://51.195.201.93:80', 'http://157.119.234.50:80', 'http://202.73.51.234:80',
              'http://199.19.226.12:80', 'http://103.216.103.25:80', 'http://45.199.148.4:80']

HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
        }


for proxy in proxy_list[:]:
    try:
        result = requests.get('http://news.10jqka.com.cn/20211025/c633627264.shtml', headers=HEADERS,
                              proxies={'http': proxy, 'https': proxy})
        if result.status_code == 200:
            soup = BeautifulSoup(result.text, 'lxml')
            print(proxy, 'RESULT TEXT: ', result.text)
            print(proxy, '- working')

        else:
            print(proxy, '- not working')
    except Exception as e:
        print(e)
