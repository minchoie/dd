import conn
from datetime import datetime, timedelta
import pandas as pd
import logging
import json
from crawling import investing

def fetch_stock_names():
    conn.connect_to_database()
    search_list = []
    if conn.global_cursor is None:
        print("데이터베이스에 연결되지 않았습니다.")
        return search_list
    
    try:
        conn.global_cursor.execute("SELECT GINDEX_NAME FROM TB_INDEXCLASSIFY a")
        search_list = [row[0] for row in conn.global_cursor.fetchall()]  
        print('DB로부터 가져온 검색종목 목록:', search_list)
    except conn.pymysql.Error as e:
        print(f'DB로부터 검색종목 목록 가져오다가 오류 발생: {e}')
    return search_list

def search_GINDEX_CODE(search_word):
    conn.connect_to_database()
    try:
        conn.global_cursor.execute("SELECT GINDEX_CODE FROM TB_INDEXCLASSIFY WHERE GINDEX_NAME = %s", (search_word,))
        GINDEX_CODE = conn.global_cursor.fetchone()
        
        if GINDEX_CODE:
            GINDEX_CODE = GINDEX_CODE[0]
            print("첫번째 SELECT문 결과:", GINDEX_CODE)
        else:
            print(f"{search_word}에 해당하는 GINDEX_CODE를 찾을 수 없습니다.")
            return None
        return GINDEX_CODE
    except conn.pymysql.Error as e:
        print(f"DB로부터 GINDEX_CODE 가져오는 중 오류 발생: {e}")
        return None

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

def insert_usnews_to_db(news_json):
    try:
        data = json.loads(news_json)
        
        insert_query = """
        INSERT INTO TB_USINDEXNEWS (USIDXN_CODE, GINDEX_CODE, INVEST_CODE, USNEWS_TITLE, USNEWS_CONTENT, USNEWS_DATE, USNEWS_PRESS, USNEWS_URL)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = [
            (
                item['USIDXN_CODE'],
                item['GINDEX_CODE'],
                item['INVEST_CODE'],
                item['USNEWS_TITLE'],
                item['USNEWS_CONTENT'],
                item['USNEWS_DATE'],
                item['USNEWS_PRESS'],
                item['USNEWS_URL']
            )
            for item in data
        ]
        conn.global_cursor.executemany(insert_query, values)
        conn.commit_changes()
        logging.info(f"데이터 {len(values)}건이 성공적으로 MariaDB에 삽입되었습니다.")
    except json.JSONDecodeError as je:
        logging.error(f"Error decoding JSON: {je}")
    except Exception as e:
        logging.error(f"데이터 적재 중 오류 발생: {e}")
        conn.rollback_changes()
    finally:
        conn.close_database_connection()
        logging.error('프로그램 완전 종료!')


        