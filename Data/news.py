import requests
from bs4 import BeautifulSoup

import re
import datetime
from tqdm import tqdm
import sys
from datetime import datetime

import pandas as pd
import json
import csv

import multiprocessing
import time


# 변수 세팅
corp_file = '/home/yikyungkim/kpmg/fssdata/dart_corpCodes.csv'
output_path = '/home/yikyungkim/kpmg/newsdata/output/'

max_page = 100
years = 3
listed = True



def get_company_list(file_path, listed):
    #회사 목록 리스트에서 상장사, 비상장사 구분하여 가져옴

    with open(file_path,'r', encoding='utf-8') as f:
        mycsv=csv.reader(f)
        companies=[]
        for row in mycsv:
            company={}
            company['corp_code']=row[0]
            company['corp_name']=row[1]
            company['stock_code']=row[2]
            company['modify_date']=row[3]
            companies.append(company)
        
    cor_listed=[]
    cor_not_listed=[]
    for company in companies:
        if company['stock_code'] == ' ':
            cor_not_listed.append(company)
        else:
            cor_listed.append(company)
    
    if listed:
        return cor_listed
    else:
        return cor_not_listed        
       

def news_crawler(query, max_page):
    # 검색어를 바탕으로 크롤링 할 최대 페이지에 해당하는 뉴스 링크를 가져옴
        
#     start_date_2 = start_date.replace(".", "")
#     end_date_2 = end_date.replace(".", "")
    
    start_pg = 1
    end_pg = (int(max_page)-1)*10+1 

    naver_urls = []   
    
    while start_pg < end_pg:
        
        url = "https://search.naver.com/search.naver?where=news&sm=tab_pge&query=" + query + "&start=" + str(start_pg)
        # url = "https://search.naver.com/search.naver?where=news&sm=tab_pge&query=" + query + "&ds=" + start_date + "&de=" + end_date +  "&nso=so%3Ar%2Cp%3Afrom" + start_date_2 + "to" + end_date_2 + "%2Ca%3A&start=" + str(start_pg)
        # ua = UserAgent()
        # headers = {'User-Agent' : ua.random}

        raw = requests.get(url)
        cont = raw.content
        html = BeautifulSoup(cont, 'html.parser')
        
        for urls in html.select("a.info"):
            try:
                if "news.naver.com" in urls['href']:
                    naver_urls.append(urls['href'])              
            except Exception as e:
                continue
        
        start_pg += 10
        
    return naver_urls 


def newsdataset(query):
    
    total_news = []

    # url 가져오기
    total_urls = news_crawler(query, max_page)
    total_urls = list(set(total_urls))  #중복제거

    # ConnectionError방지
    headers = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/98.0.4758.102" }

    for url in (total_urls):
        raw = requests.get(url,headers=headers)
        html = BeautifulSoup(raw.text, "html.parser")
        news={}
        pattern1 = '<[^>]*>'

        ## 날짜
        try:
            html_date = html.select_one("div#ct> div.media_end_head.go_trans > div.media_end_head_info.nv_notrans > div.media_end_head_info_datestamp > div > span")
            news_date = html_date.attrs['data-date-time']
        except AttributeError:
            news_date = html.select_one("#content > div.end_ct > div > div.article_info > span > em")
            news_date = re.sub(pattern=pattern1,repl='',string=str(news_date))
       
        start_year = datetime.now().year - years
        try:
            news_year = int(news_date[:4])
            if news_year < start_year:
                continue
            else:
                news['dates']=news_date
        except ValueError:
            continue

        # url
        news['url']=url

        # 뉴스 제목
        title = html.select("div#ct > div.media_end_head.go_trans > div.media_end_head_title > h2")
        title = ''.join(str(title))

        # html태그제거
        title = re.sub(pattern=pattern1,repl='',string=title)
        news['titles']=title

        #뉴스 본문
        content = html.select("div#dic_area")
        content = ''.join(str(content))

        #html태그제거 및 텍스트 다듬기
        content = re.sub(pattern=pattern1,repl='',string=content)
        pattern2 = '\n'
        content = re.sub(pattern=pattern2,repl='',string=content)
        pattern3 = """[\n\n\n\n\n// flash 오류를 우회하기 위한 함수 추가\nfunction _flash_removeCallback() {}"""
        content = content.replace(pattern3,'')
        news['content']=content

        total_news.append(news)
            
    # return total_news
    path = output_path + query + '.json'       
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(total_news, f, ensure_ascii=False, indent='\t')



def main():

    start_time = time.time()

    # 회사 리스트 가져오기
    companies = get_company_list(corp_file, listed)
    queries=[companies[i]['corp_name'] for i in range(1,len(companies))]
    


    with multiprocessing.Pool(32) as pool:
        list(tqdm(pool.imap(newsdataset, queries), total=len(queries)))
    pool.close()
    pool.join()



    print("---%s seconds ---" % (time.time() - start_time))

    # final_output=[]  # 빈데이터 삭제
    # for out in output:
    #     if out != []:
    #         for o in out:
    #             final_output.append(o)

    # with open(output_path, 'w', encoding='utf-8') as f:
    #     json.dump(final_output, f, ensure_ascii=False, indent='\t')



if __name__ == '__main__':
    main()