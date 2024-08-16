import bs4
from bs4 import BeautifulSoup
from urllib.request import urlopen
import requests
import numpy as np
import pandas as pd
from tqdm import tqdm
import cx_Oracle
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Oracle 데이터베이스 연결 설정
conn = cx_Oracle.connect('ora_user', '1234', 'localhost:1521/XE')

request_headers = {
    'User-Agent': 'Mozilla/5.0'
}
url_base = 'https://investing.com'
url_sub = '/news/cryptocurrency-news/'
url = url_base + url_sub

href = []
url_adds = []
Press_soup = []
press_soup = []

print('확인1')

for page in range(1, 30):
    full_url = f'{url}{page}'
    response = requests.get(url=full_url, headers=request_headers).text
    soup = BeautifulSoup(response, 'html.parser')

    list_soup = soup.find_all('a', class_='text-inv-blue-500 hover:text-inv-blue-500 hover:underline focus:text-inv-blue-500 focus:underline whitespace-normal text-sm font-bold leading-5 !text-[#181C21] sm:text-base sm:leading-6 lg:text-lg lg:leading-7')
    press_soup = soup.find_all('span', class_='shrink-0 text-xs leading-4')
    
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

title = []
content = []
time = []

print('확인2')

for idx in tqdm(data.index):
    url_str = data['주소'][idx]
    
    response = requests.get(url=url_str, headers=request_headers).text
    soup_tmp = BeautifulSoup(response, 'html.parser')
    
    title_element = soup_tmp.find('h1', id='articleTitle')
    time_element = soup_tmp.find('div', class_='flex flex-col gap-2 text-warren-gray-700 md:flex-row md:items-center md:gap-0')
    content_element = soup_tmp.find('div', class_='article_WYSIWYG__O0uhw article_articlePage__UMz3q text-[18px] leading-8')
    
    Title_tmp = title_element.get_text() if title_element else 'None'
    Time_tmp = time_element.get_text() if time_element else 'None'
    
    # 시간 데이터가 제대로 파싱되지 않는 경우 처리
    if Time_tmp and len(Time_tmp.split()) >= 3:
        Time_tmp = Time_tmp.split()[1:4]
    else:
        Time_tmp = ['None', '00:00', 'AM']  # 기본 값으로 초기화

    Content_tmp = content_element.get_text() if content_element else 'None'
    
    title.append(Title_tmp)
    content.append(Content_tmp)
    time.append(Time_tmp)

# 시간 처리 부분
for idx in time:
    idx[2] = idx[2].replace('Updated', ' ')

time_result = []
for idx in time:
    date_ = idx[0].replace(',', '').strip()
    time_ = idx[1].strip()
    period_ = idx[2].strip()
    
    if period_ == 'PM' and not time_.startswith('12'):
        hour, minute = time_.split(':')
        time_ = f"{int(hour) + 12}:{minute}"
    elif period_ == 'AM' and time_.startswith('12'):
        hour, minute = time_.split(':')
        time_ = f"00:{minute}"
    
    result = f'{date_} {time_}'
    time_result.append(result)

# 날짜 포맷 변환 - datetime 객체로 변환
chtime = []
for idx in time_result:
    if idx.split()[0] != 'None':  # 'None 00:00'이 아닌 경우에만 변환
        try:
            date_org = datetime.strptime(idx, '%m/%d/%Y %H:%M')
            chtime.append(date_org)  # datetime 객체로 추가
        except ValueError:
            chtime.append(None)
    else:
        chtime.append(None)

data1 = pd.DataFrame({
    'Title_': title,
    'Content_': content,
    'Time_': chtime,  # datetime 객체가 들어있음
    'Press_': Press_soup
})

# 'None' 텍스트가 포함된 행을 삭제
data2 = data1[(data1['Title_'] != 'None') & (data1['Content_'] != 'None')]


df = data2

# 유사도 검사
vectorizer = TfidfVectorizer()
tfidf_matrix = vectorizer.fit_transform(df['Content_'])

# 코사인 유사도 계산
cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

# 유사도 결과를 DataFrame으로 저장
# similarity_df = pd.DataFrame(cosine_sim, index=df['Title_'], columns=df['Title_'])

# 결과 출력
# print(similarity_df)

indices_to_remove = set()
similar_articles_found = False  # 유사한 기사가 발견되었는지 여부를 추적하는 변수

for i in range(len(cosine_sim)):
    for j in range(i + 1, len(cosine_sim)):
        if cosine_sim[i, j] >= 0.9:
            indices_to_remove.add(j)
            similar_articles_found = True  # 유사한 기사가 발견되었음을 표시

# 유사한 기사가 있었음을 알려줌
if similar_articles_found:
    print("유사한 기사 발견.") 
else: 
    print('없음')

# 데이터베이스에 데이터 삽입 함수
def insert_investing_data(conn, title, content, time, press):
    cursor = conn.cursor()
    
    sql_query = '''
    INSERT INTO investing (TITLE, CONTENT, TIME, PRESS)
    VALUES (:1, :2, :3, :4)
    '''
    cursor.execute(sql_query, (title, content, time, press))
    conn.commit()

# 데이터 삽입 실행
for idx in range(len(df)):
    insert_investing_data(conn, df.loc[idx, 'Title_'], df.loc[idx, 'Content_'], df.loc[idx, 'Time_'], df.loc[idx, 'Press_'])

print("데이터 삽입 완료")

# 커서 및 연결 종료
conn.close()
