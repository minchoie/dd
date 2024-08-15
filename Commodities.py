import bs4
from bs4 import BeautifulSoup
from urllib.request import urlopen
import requests
import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

request_headers = {
    'User-Agent': 'Mozilla/5.0'
}
url_base = 'https://investing.com'
url_sub = '/news/commodities-news/'
url = url_base + url_sub

href = []
url_adds = []
Press_soup = []
press_soup = []

print('확인1')


for page in range(1, 4):
    url = f'{url}{page}'
    response = requests.get(url=url, headers=request_headers).text
    soup = BeautifulSoup(response, 'html.parser')

    list_soup = soup.find_all('a', class_='text-inv-blue-500 hover:text-inv-blue-500 hover:underline focus:text-inv-blue-500 focus:underline whitespace-normal text-sm font-bold leading-5 !text-[#181C21] sm:text-base sm:leading-6 lg:text-lg lg:leading-7')
    press_soup = soup.find_all('span', class_='shrink-0 text-xs leading-4')
    
    for i,item in enumerate(list_soup):
        href.append(item['href'])
        url_adds.append(item['href']) # url_base + 
        
                # press_soup에서 크롤링한 데이터가 있는지 확인하고, 없으면 null로 처리
        if i < len(press_soup):
            Press_soup.append(press_soup[i].get_text())
        else:
            Press_soup.append(None)

data = pd.DataFrame({
    '주소': url_adds
    ,'뉴스': Press_soup
})

title = []
content = []
time = []
press = []

for idx in tqdm(data.index):
    url_str = data['주소'][idx]
    
    response = requests.get(url=url_str, headers=request_headers).text
    soup_tmp = BeautifulSoup(response, 'html.parser')
    
    title_element = soup_tmp.find('h1', id='articleTitle')
    time_element = soup_tmp.find('div', class_='flex flex-col gap-2 text-warren-gray-700 md:flex-row md:items-center md:gap-0')
    content_element = soup_tmp.find('div', class_='article_WYSIWYG__O0uhw article_articlePage__UMz3q text-[18px] leading-8')
    # press_element = soup_tmp.find_all('img', src='https://i-invdn-com.investing.com//news/providers/Reuters.png')
    
    Title_tmp = title_element.get_text() if title_element else 'None'
    Time_tmp = time_element.get_text() if time_element else 'None'
    Content_tmp = content_element.get_text() if content_element else 'None'
    
    # if press_element:
    #     Press_tmp = press_element[0].get('alt') if press_element[0].get('alt') else 'None'
    # else:
    #     Press_tmp = 'None'
    
    title.append(Title_tmp)
    content.append(Content_tmp)
    time.append(' '.join(Time_tmp.split()[1:]))
    # press.append(Press_tmp)

data1 = pd.DataFrame({
    'Title_': title,
    'Content_': content,
    'Time_': time,
    'Press_': Press_soup
})

df = data1

# 유사도 검사
vectorizer = TfidfVectorizer()
tfidf_matrix = vectorizer.fit_transform(df['Content_'])

# 코사인 유사도 계산
cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

# # 유사도 결과를 DataFrame으로 저장
# similarity_df = pd.DataFrame(cosine_sim, index=df['Title_'], columns=df['Title_'])

# # 결과 출력
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
else : 
    print('없음')


# 엑셀 파일로 저장
today = datetime.datetime.today()
dayNo = today.strftime('%Y%m%d')
df.to_excel(f'commodities-news{dayNo}.xlsx', sheet_name=dayNo, index=False)
# similarity_df.to_excel(f'commodities-news_similarity{dayNo}.xlsx', sheet_name=dayNo, index=True)
