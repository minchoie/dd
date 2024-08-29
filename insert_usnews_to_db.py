import logging
import json
import conn

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