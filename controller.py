from crawling import investing
from similarity import consim
import pandas as pd
from datetime import datetime
from connection import get_old_news_from_db, search_GINDEX_CODE
import conn




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
