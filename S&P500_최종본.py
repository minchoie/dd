import bs4
from bs4 import BeautifulSoup
from urllib.request import urlopen
import requests
import numpy as np
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import conn

def investing(search_word, url_start):

    conn.connect_to_database()
    # GINDEX_CODE = 'SPX'
    # INVEST_CODE = '03'

    try:
        conn.global_cursor.execute("SELECT a.GINDEX_CODE FROM TB_INDEXCLASSIFY a WHERE a.GINDEX_NAME = %s", (search_word,))
        GINDEX_CODE = conn.global_cursor.fetchone()[0]
        print("첫번째 SELECT문", GINDEX_CODE)
        conn.global_cursor.execute("SELECT a.INVEST_CODE FROM TB_INDEXCLASSIFY a WHERE a.GINDEX_NAME = %s", (search_word,))
        INVEST_CODE = conn.global_cursor.fetchone()[0]
        print("두번째 SELECT문", INVEST_CODE)
    except conn.mariadb.Error as e:
        print(f"GINDEX_CODE, INVEST_CODE를 SELECT하다가 오류 발생: {e}")
        conn.rollback_changes()

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

    for page in range(1, 100):
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
                chtime.append(date_org)
            except ValueError:
                chtime.append(None)
        else:
            chtime.append(None)

    data1 = pd.DataFrame({
        'Title_': title,
        'Content_': content,
        'Time_': chtime,
        'Press_': Press_soup,
        'URL_': url_adds,
        'Ms_':ms
    })

    data1 = data1[data1['Content_'] != 'None']

    df = data1

    print('유사도 검사 시작')

    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(df['Content_'])

    cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

    similarity_df = pd.DataFrame(cosine_sim, index=df['Title_'], columns=df['Title_'])

    rows_to_drop = set()

    for i in range(similarity_df.shape[0]):
        for j in range(i + 1, similarity_df.shape[1]):
            if 0.9 < similarity_df.iat[i, j] <= 1.0:  # 유사도가 0.9 초과 1.0 이하인 경우
                rows_to_drop.add(similarity_df.index[i])
                rows_to_drop.add(similarity_df.columns[j])

    similarity_df = similarity_df.drop(index=rows_to_drop, columns=rows_to_drop, errors='ignore')

    df = df.drop(rows_to_drop, errors='ignore')

    df.to_excel(f'S&P500.xlsx', index=False)
    similarity_df.to_excel(f'S&P500_similarity.xlsx', index=True)

    # 데이터 삽입 쿼리
    insert_query = """
    INSERT INTO TB_USINDEXNEWS (USIDXN_CODE, GINDEX_CODE, INVEST_CODE, USNEWS_TITLE, USNEWS_CONTENT, USNEWS_DATE, USNEWS_PRESS, USNEWS_URL)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    count = 0
    # 데이터 삽입 실행
    for idx in range(len(df)):
        time_str = df.iloc[idx]['Time_'].strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(df.iloc[idx]['Time_']) else None
        data = ( 
            'USN'+ time_str.replace('-', '').replace(':', '').replace(' ', '') + df.iloc[idx]['Ms_'] if time_str else 'USN00000000000000', 
            GINDEX_CODE, 
            INVEST_CODE, 
            df.iloc[idx]['Title_'], 
            df.iloc[idx]['Content_'],
            time_str,
            df.iloc[idx]['Press_'], 
            df.iloc[idx]['URL_']
        )
        conn.global_cursor.execute(insert_query, data)
        conn.commit_changes()
        print(f"데이터 {df.shape[0]}건이 성공적으로 MariaDB에 삽입되었습니다.")
    count += df.shape[0]
# 데이터베이스 연결 종료
    conn.close_database_connection()
    print("프로그램 완전 종료")
    

investing('SNP500' , '/indices/us-spx-500-news/')
