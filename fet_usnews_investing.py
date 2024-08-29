import conn
from datetime import datetime, timedelta
import pandas as pd
from crawling import investing  # 크롤링 모듈에서 investing 함수 가져오기
from datetime import datetime
# 유사도 검사 함수
import pandas as pd
from connection import insert_usnews_to_db
from similarity import consim
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from connection import get_old_news_from_db, search_GINDEX_CODE

def get_old_news_from_db(GINDEX_CODE):
    conn.connect_to_database()
    today = datetime.now().date()
    oneday_ago = today - timedelta(days=1)
    twoday_ago = today -timedelta(days=2)
    
    try:
        if not GINDEX_CODE:
            print("유효하지 않은 GINDEX_CODE입니다.")
            return pd.DataFrame()
        
        oldnews_query = """
        SELECT * 
        FROM TB_USINDEXNEWS 
        WHERE GINDEX_CODE = %s 
        AND (USNEWS_DATE LIKE %s OR USNEWS_DATE LIKE %s OR USNEWS_DATE LIKE %s)
        """
        query_params = (GINDEX_CODE, f'{today}%', f'{oneday_ago}%', f'{twoday_ago}')
        print("실행할 쿼리:", oldnews_query % query_params)
        
        conn.global_cursor.execute(oldnews_query, query_params)
        
        results = conn.global_cursor.fetchall()
        columns_names = [desc[0] for desc in conn.global_cursor.description]
        return pd.DataFrame(results, columns=columns_names)
    except conn.pymysql.Error as e:
        print(f"데이터 불러오던 중 오류 발생: {e}")
        conn.rollback_changes()
        return pd.DataFrame()

# def search_GINDEX_CODE(search_word):
#     conn.connect_to_database()
#     try:
#         conn.global_cursor.execute("SELECT GINDEX_CODE FROM TB_INDEXCLASSIFY WHERE GINDEX_NAME = %s", (search_word,))
#         GINDEX_CODE = conn.global_cursor.fetchone()
        
#         if GINDEX_CODE:
#             GINDEX_CODE = GINDEX_CODE[0]
#             print("첫번째 SELECT문 결과:", GINDEX_CODE)
#         else:
#             print(f"{search_word}에 해당하는 GINDEX_CODE를 찾을 수 없습니다.")
#             return None
#         return GINDEX_CODE
#     except conn.pymysql.Error as e:
#         print(f"DB로부터 GINDEX_CODE 가져오는 중 오류 발생: {e}")
#         return None

def consim(data, db_df):
    combined_df = pd.concat([data, db_df], axis=0, ignore_index=True)
    # 'USNEWS_CONTENT'와 'USNEWS_TITLE'이 존재하는지 확인
    if 'USNEWS_CONTENT' not in combined_df.columns or 'USNEWS_TITLE' not in combined_df.columns:
        print("경고: 'USNEWS_CONTENT' 또는 'USNEWS_TITLE' 열이 데이터프레임에 없습니다.")
        return pd.DataFrame()
    # NaN 값, 빈 row 제거
    combined_df = combined_df.dropna(subset=['USNEWS_CONTENT', 'USNEWS_TITLE'])
    combined_df = combined_df[(combined_df['USNEWS_CONTENT'].str.strip() != '') & (combined_df['USNEWS_TITLE'].str.strip() != '')]
    # 기사가 없으면 종료
    if combined_df.empty:
        print("경고: 'USNEWS_CONTENT' 또는 'USNEWS_TITLE' 열이 비어있거나 유효한 데이터가 없습니다.")
        pass
    else:
        # 제목과 내용을 결합하여 유사도 계산
        combined_df['combined_text'] = combined_df['USNEWS_TITLE'] + " " + combined_df['USNEWS_CONTENT']

        # TF-IDF 행렬 생성
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(combined_df['combined_text'])

        # 코사인 유사도 계산
        cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

        # 유사도 결과를 DataFrame으로 저장
        similarity_df = pd.DataFrame(cosine_sim, index=combined_df.index, columns=combined_df.index)

        # 유사한 기사 인덱스를 저장할 집합
        rows_to_drop = set()

        # 유사도 검사: 유사도가 0.9 이상이거나 1.0인 경우 제거 대상에 추가
        for i in range(similarity_df.shape[0]):
            for j in range(i + 1, similarity_df.shape[1]):
                if 0.9 <= cosine_sim[i, j] < 1.0 or cosine_sim[i, j] == 1.0:
                    rows_to_drop.add(similarity_df.index[j])

        # 유사한 기사 삭제
        if rows_to_drop:
            print("유사한 기사 있음. 해당 기사는 삭제됩니다.")
            combined_df = combined_df.drop(rows_to_drop, errors='ignore')
        else:
            print("유사한 기사 없음.")

        # 최종 결과를 JSON으로 변환하여 데이터베이스에 삽입
        if not combined_df.empty:
            news_json = combined_df.to_json(orient='records', date_format='iso')
            insert_usnews_to_db(news_json)
        else:
            print('기사 없어서 json변환 건너뜀')


def insert_db(search_word, url_start):
    today = datetime.today().strftime('%Y-%m-%d')
    gindex_code = search_GINDEX_CODE(search_word)
    db_df = get_old_news_from_db(gindex_code)
    db_df.to_excel(f'{search_word}_{today}_DB.xlsx', index=False)
    print(f"DB 데이터가 '{search_word}_{today}_DB.xlsx' 파일로 저장되었습니다.")

    news_df = investing(url_start)
    news_df.to_excel(f'{search_word}_{today}_news.xlsx', index=False)
    print(f"크롤링 데이터가 '{search_word}_{today}_news.xlsx' 파일로 저장되었습니다.")
    
    conn.connect_to_database()
    try:
        conn.global_cursor.execute("SELECT a.GINDEX_CODE FROM TB_INDEXCLASSIFY a WHERE a.GINDEX_NAME = %s", (search_word,))
        GINDEX_CODE = conn.global_cursor.fetchone()[0]
        print("첫번째 SELECT문", GINDEX_CODE)
        conn.global_cursor.execute("SELECT a.INVEST_CODE FROM TB_INDEXCLASSIFY a WHERE a.GINDEX_NAME = %s", (search_word,))
        INVEST_CODE = conn.global_cursor.fetchone()[0]
        print("두번째 SELECT문", INVEST_CODE)
    except conn.pymysql.Error as e:
        print(f"GINDEX_CODE, INVEST_CODE를 SELECT하다가 오류 발생: {e}")
        conn.rollback_changes()
    
    # data 변수를 미리 빈 DataFrame으로 초기화
    data = pd.DataFrame()

    for idx, row in news_df.iterrows():
        time_str = row['USNEWS_DATE'].strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row['USNEWS_DATE']) else None
        row_data = pd.DataFrame({ 
            'USIDXN_CODE' : ['USN'+ time_str.replace('-', '').replace(':', '').replace(' ', '') + row['Ms_'] if time_str else 'USN00000000000000'], 
            'GINDEX_CODE' : [GINDEX_CODE], 
            'INVEST_CODE' : [INVEST_CODE], 
            'USNEWS_TITLE' : [row['USNEWS_TITLE']], 
            'USNEWS_CONTENT' : [row['USNEWS_CONTENT']],
            'USNEWS_DATE' : [time_str],
            'USNEWS_PRESS' : [row['USNEWS_PRESS']], 
            'USNEWS_URL' : [row['USNEWS_URL']]
        })
        
        # 루프에서 각 row_data를 누적하여 data에 추가
        data = pd.concat([data, row_data], ignore_index=True)

    if not data.empty:
        data.to_excel(f'{search_word}_{today}_final.xlsx', index=False)
        print(f"최종본 데이터가 '{search_word}_{today}_final.xlsx' 파일로 저장되었습니다.")
    
    return data, db_df

# 데이터를 가져오고 유사성 검사를 하는 함수
def process_and_compare_news():
    source = [
        ('SNP500', '/indices/us-spx-500-news/'), 
        ('코스피200', '/indices/kospi-news/'), 
        ('나스닥100', '/indices/nq-100-news/'), 
        ('미국_국채_10년물', '/rates-bonds/u.s.-10-year-bond-yield-news/')
    ]

    # 데이터프레임 생성
    insert = pd.DataFrame(source, columns=['search_word', 'url_start'])

    # 각 행을 순회하면서 insert_db 함수 호출
    for idx, row in insert.iterrows():
        data, db_df = insert_db(row['search_word'], row['url_start'])
        combined_df = consim(data, db_df)  # 유사도 검사 및 데이터 병합
        # combined_df는 처리된 후 데이터베이스에 삽입되거나 다른 작업을 수행할 수 있음

        # 필요시 combined_df를 추가 작업 가능
        # combined_df.to_excel(f'{row["search_word"]}_{today}_processed.xlsx', index=False)

# 실행
process_and_compare_news()  # insert_db 호출