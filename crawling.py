import bs4
from bs4 import BeautifulSoup
import requests
import conn
import numpy as np
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def investing(url_start):
    request_headers = {
        'User-Agent': 'Mozilla/5.0'
    }
    url_base = 'https://investing.com'
    url_sub = url_start
    url = url_base + url_sub

    href = []
    url_adds = []
    Press_soup = []
    press_soup = []

    print('URL, Press추출 시작')

    for page in range(1, 2):
        url_ = f'{url}{page}'
        response = requests.get(url=url_, headers=request_headers).text
        soup = BeautifulSoup(response, 'html.parser')

        list_soup = soup.find_all('a', class_='block text-base font-bold leading-5 hover:underline sm:text-base sm:leading-6 md:text-lg md:leading-7')
        press_soup = soup.find_all('li', class_='overflow-hidden text-ellipsis')
    
        for i, item in enumerate(list_soup):
            href.append(item['href'])
            url_adds.append(item['href']) 
        
            if i < len(press_soup):
                Press_soup.append(press_soup[i].get_text())
            else:
                Press_soup.append(None)

    data = pd.DataFrame({
        '주소': url_adds,
        '뉴스': Press_soup
    })
    data['주소'] = data['주소'].str.strip()
    Press_soup = [item.replace('By', '') for item in Press_soup]

    title = []
    content = []
    time = []
    ms = []
    print('추출 완료')
    print('나머지 추출')

    for idx in tqdm(data.index):
        url_str = data['주소'][idx]
    
        response = requests.get(url=url_str, headers=request_headers).text
        soup_tmp = BeautifulSoup(response, 'html.parser')
        # PK를 위한 밀리초
        try:
            ms.append(datetime.now().strftime('%f')[:3])
        except AttributeError as e:
            ms.append('N/A')
        
        title_element = soup_tmp.find('h1', id='articleTitle')
        time_element = soup_tmp.find('div', class_='flex flex-col gap-2 text-warren-gray-700 md:flex-row md:items-center md:gap-0')
        content_element = soup_tmp.find('div', class_='article_WYSIWYG__O0uhw article_articlePage__UMz3q text-[18px] leading-8')
    
        Title_tmp = title_element.get_text() if title_element else 'None'
        Time_tmp = time_element.get_text() if time_element else 'None'
    
        if Time_tmp and len(Time_tmp.split()) >= 3:
            Time_tmp = Time_tmp.split()[1:4]
        else:
            Time_tmp = ['None', '00:00', 'AM']

        Content_tmp = content_element.get_text() if content_element else 'None'
    
        title.append(Title_tmp)
        content.append(Content_tmp)
        time.append(Time_tmp)

    for idx in time:
        if len(idx) >= 3:
            idx[2] = idx[2].replace('Updated', ' ')

    time_result = []
    for idx in time:
        date_ = idx[0].replace(',', '').strip()
        time_ = idx[1].strip()
        period_ = idx[2].strip() if len(idx) >= 3 else 'AM'
    
        if period_ == 'PM' and not time_.startswith('12'):
            hour, minute = time_.split(':')
            time_ = f"{int(hour) + 12}:{minute}"
        elif period_ == 'AM' and time_.startswith('12'):
            hour, minute = time_.split(':')
            time_ = f"00:{minute}"
    
        result = f'{date_} {time_}'
        time_result.append(result)

    chtime = []
    for idx in time_result:
        if idx.split()[0] != 'None':
            try:
                date_org = datetime.strptime(idx, '%m/%d/%Y %H:%M')
                date_org += timedelta(hours=13)
                chtime.append(date_org)
            except ValueError:
                chtime.append(None)
        else:
            chtime.append(None)

    data1 = pd.DataFrame({
        'USNEWS_TITLE': title,
        'USNEWS_CONTENT': content,
        'USNEWS_DATE': chtime,
        'USNEWS_PRESS': Press_soup,
        'USNEWS_URL': url_adds,
        'Ms_': ms
    })
    
    data1 = data1[data1['USNEWS_CONTENT'] != 'None']
    
    # 오늘 날짜 필터링
    today = datetime.today().date()
    today_df = data1[data1['USNEWS_DATE'].apply(lambda x: x.date() == today if x else False)]
    

    return today_df


