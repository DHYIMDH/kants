#!/usr/bin/env python
# coding: utf-8

# ## Import

# In[ ]:


# 크롤링시 필요한 라이브러리
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

import urllib.request
from urllib.parse import quote

# postgresql 연결을 위해 필요한 라이브러리
from psycopg2 import connect


# ## 1. Constructing Functions

# ## 1.1. Get "TARGET" table

# In[ ]:


# DB의 Target table에서 data 불러오기
def get_target_table() -> list:
    
    with connect(database="kants", user='postgres', password='kants123!', host='34.64.47.191') as conn:
        
        with conn.cursor() as cur:
            
            cur.execute("SELECT * FROM target WHERE target=1")
            target_stocks = cur.fetchall()
            
            cur.execute("SELECT count(*) FROM news")
            last_news_id = cur.fetchone()
        
            cur.close
    
    stock_names = []
    for stock in target_stocks:
        stock_names.append(stock[1])
    
    last_news_id = last_news_id[0]
    
    return stock_names, last_news_id


# ## 1.2. DB INSERTION

# In[ ]:


# 크롤링한 daily news data를 DB에 넣기
def insert_news_table(df_news):
    
    with connect(database="kants", user='postgres', password='kants123!', host='34.64.47.191') as conn:
        
        with conn.cursor() as cur:
            
            for i in range(len(df_news)):
                
                row = df_news.iloc[i,:].values
                news_id = str(row[0])
                date = row[1]
                title = row[2]
                content = row[3]
                urls = row[4]
                
                cur.execute("INSERT INTO news(news_id, date, title, content, urls) VALUES(%s, %s, %s, %s, %s)",
                           (news_id, date, title, content, urls))
            
            conn.commit()
            
            cur.close


# ## 1.3. Crawler

# In[ ]:


def set_chrome_driver():

    # chrome 실행 옵션을 받아오는 변수
    chrome_options = webdriver.ChromeOptions()
    
    # headless: 크롭탭을 띄우지 않고 크롬을 사용: 다른 작업에 거슬리지 않도록 함
    # no-sandbox: 크롬 보안 기능을 비활성화: 접근한 페이지와 크롬의 호환성이 맞지 않거나, PC 내 어떤 프로세스와 충돌하는 상황
    # disable-dev-shm-usage: /dev/shm 디렉토리를 비활성화: 메모리 부족으로 인해 에러가 발생하는 것을 방지해줌
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    

    # webdriver_manager.chrome을 사용하면,
    # - 크롬 버전에 맞는 드라이버 다운로드 및 캐시 저장
    # - 캐시에 저장된 드라이버가 존재하면 다운로드하지 않고 재사용
    # - ref: https://velog.io/@sangyeon217/deprecation-warning-executablepath-has-been-deprecated

    
    #Selenium 버전이 v4.x 일 경우
    # driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    
    
    #Selenium 버전이 v3.x 일 경우
    # chromedriver가 다운로드된 경로 전달하여 chrome 실행
    driver = webdriver.Chrome(executable_path='/opt/google/chrome/chromedriver', options=chrome_options)

    return driver


# In[ ]:


# 뉴스 페이지의 url 형식에 맞게 페이지 숫자를 생성하는 함수
# 1, 11, 21, 31, 41, ...
def makePgNum(page) -> int:
    page_num = (page-1)*10 + 1
    return page_num


# In[ ]:


# 네이버 뉴스 페이지 url 리스트를 생성하는 함수: 검색어, 시작페이지, 종료페이지, 시작날짜, 종료날짜
# 뉴스 유형 5개 중에서 '지면기사'만 수집
def makeUrl(search_keyword, start_pg, end_pg, stdt, endt) -> list:
    
    # "yyyy.mm.dd" 형태의 string 값 생성
    stdt_dot = stdt[:4] + "." + stdt[4:6] + "." + stdt[-2:]
    endt_dot = endt[:4] + "." + endt[4:6] + "." + endt[-2:]
    
    # url을 저장할 리스트 생성
    urls = []
    
    # 원하는 페이지 수 만큼 url 생성
    for i in range(start_pg, end_pg+1):
        
        # 페이지 숫자 생성
        page_num = makePgNum(i)
        
        # url 생성 후 저장
        # url = 기본주소 + 검색어(query) + 검색옵션내역 \
        #       + 시작날짜(yyyy.mm.dd) + "&de" + 종료날짜(yyyy.mm.dd) \
        #       + 검색옵션내역 + 시작날짜(yyyymmdd) + "to" + 종료날짜(yyyymmdd) \
        #       + ",a:all&start=" + 페이지수
        url = 'https://search.naver.com/search.naver?where=news&sm=tab_pge&query=' + quote(search_keyword)         + '&sort=0&photo=3&field=0&pd=3&ds=' + stdt_dot + "&de=" + endt_dot         + "&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so:r,p:from" + stdt + "to" + endt         + ",a:all&start=" + str(page_num)
        urls.append(url)
    
    # url 리스트 반환
    return urls


# In[ ]:


def crawler(driver, start_news_id, stock_name, start_pg, end_pg, stdt, endt, time_sleep1, time_sleep2):
    
    # chrome driver 실행
    time.sleep(1)
    
    # 크롤링 결과값 저장용 containers
    dates_container = []
    titles_container = []
    contents_container = []
    naver_urls_container = []
    
    ########## 네이버 뉴스 url 수집 Part ##########
         
    # 네이버 뉴스 페이지 url 리스트 생성 (e.g. 삼성전자 검색 뉴스 페이지 url)
    news_page_urls = makeUrl(stock_name, start_pg, end_pg, stdt, endt)
        

    # 마지막 검색 페이지 도달 검증용 리스트
    prev_html = []
        
    # 네이버뉴스 url 크롤링
    for url in news_page_urls:
            
        req = urllib.request.Request(url)
        sourcecode = urllib.request.urlopen(url).read()
        soup = BeautifulSoup(sourcecode, "html.parser")

        # 과거에 없었던 종목이면 처음부터 크롤링이 안되면서 NoneType error 발생, 이럴 경우에는 empty list를 return 처리함
        if (soup.find("ul", class_="list_news") != None):
            curr_html = soup.find("ul", class_="list_news").find_all("li")
            # 마지막 검색 페이지 도달 시, 반복문을 break
            if prev_html == curr_html:
                break
            # 아닐 경우 keep going
            else:
                prev_html = curr_html
        # 아무런 뉴스도 없었을 경우
        else:
            return [], start_news_id - 1
            
        for href in soup.find("ul", class_="list_news").find_all("li"):
                
            # None 값이 읽힐 때는 따로 처리하지 않음
            if (href.find("div", class_="info_group")!=None):
                    
                for i,tag in enumerate(href.find("div", class_="info_group")):

                    # 작성일자 저장
                    if i == 3:
                        date = tag.get_text()
                        date = date[:4]+date[5:7]+date[8:10]
                        dates_container.append(date)

                    # url 저장
                    if i == 4:
                        naver_urls_container.append(str(tag["href"]))
        
    ########## 네이버 뉴스 기사 수집 Part ##########
        
    # ConnectionError 방지
    headers = {"User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"}
                            
    # 본문 기사 읽어오기
    for i in naver_urls_container:
        original_html = requests.get(i, headers=headers)
        html = BeautifulSoup(original_html.text, "html.parser")
            
        # 뉴스 제목 가져오기
        title = html.select("div#ct > div.media_end_head.go_trans > div.media_end_head_title > h2")
        # list합치기
        title = ''.join(str(title))
        # html태그제거
        pattern1 = '<[^>]*>'
        title = re.sub(pattern=pattern1, repl='', string=title)
        titles_container.append(title)

        # 뉴스 본문 가져오기
        content = html.select("div#dic_area")

        # 기사 텍스트만 가져오기
        # list합치기
        content = ''.join(str(content))

        # html태그제거 및 텍스트 다듬기
        content = re.sub(pattern=pattern1, repl='', string=content)
        pattern2 = """[\n\n\n\n\n// flash 오류를 우회하기 위한 함수 추가\nfunction _flash_removeCallback() {}"""
        content = content.replace(pattern2, '')

        contents_container.append(content)
        
    # DataFrame으로 변경
    # news_id 생성
    news_id_container = [str(start_news_id + i) for i in range(len(titles_container))]
    df_news = pd.DataFrame({'news_id': news_id_container,'date': dates_container, 'title': titles_container,'content': contents_container, 'urls': naver_urls_container})
    last_news_id = start_news_id + len(news_id_container) - 1
      
    return df_news, last_news_id


# # 2. Daily Batch

# In[ ]:


def daily_batch(stdt, endt, time_sleep1=1, time_sleep2=0.5, start_pg=1, end_pg=400):

    # 1. select * from target where target=1 -> name list 받아오기
    # 2. select count(*) from news => 현재 몇 개의 뉴스데이터가 있는지 받아오기 (news_id 생성용)
    stock_names, last_news_id = get_target_table()
        
    # 3. driver 실행
    daily_batch_driver = set_chrome_driver()

    print("==== INFO: stdt: "+stdt+", endt: "+endt+" ====")
    
    # 4. name list 돌면서 crawling
    for stock_name in stock_names:        
        
        print("crawling for "+stock_name +" is started.")
        start = time.time()
        df_news, last_news_id = crawler(daily_batch_driver, last_news_id+1 ,stock_name,
                                         start_pg, end_pg, stdt, endt, time_sleep1, time_sleep2)
        end = time.time()
        print("number of crawled news: "+str(len(df_news))+", time: "+str(end-start)+"sec")
        
        # 5. crawled news를 DB에 INSERT
        if (len(df_news)!=0):
            insert_news_table(df_news)
        print("crawling for "+stock_name +" is finished.")
        
    # 6. driver 종료
    daily_batch_driver.quit()

    print("============== END ==============")


# In[ ]:


if __name__ == "__main__":
    
    # 오늘 날짜 받아오기
    today = datetime.today().strftime("%Y%m%d")
    
    # 오늘 날짜 뉴스에 대한 크롤링 수행
    daily_batch(today, today, 1, 0.5, 1, 400)
    


# In[ ]:




