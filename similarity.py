# 크롤링 모듈에서 data1을 반환하는 함수가 있다고 가정
from crawling import investing  # 크롤링 모듈에서 investing 함수 가져오기
from datetime import datetime
# 유사도 검사 함수
import pandas as pd
from connection import insert_usnews_to_db
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def similarity(df, threshold=0.9):
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(df['USNEWS_CONTENT'])

    # 코사인 유사도 계산
    cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

    similarity_df = pd.DataFrame(cosine_sim, index=df.index, columns=df.index)

    indices_to_remove = set()

    for i in range(len(cosine_sim)):
        for j in range(i+1, len(cosine_sim)):  # j는 i 이후의 인덱스만 비교
            if threshold <= cosine_sim[i, j] < 1.0:  # 유사도가 threshold 이상 1.0 미만인 경우
                indices_to_remove.add(j)

    # 유사한 기사가 있으면 해당 크롤링된 기사 삭제
    if indices_to_remove:
        print("유사한 기사 발견. 해당 기사는 삭제됩니다.")
        df = df.drop(list(indices_to_remove))  # 유사한 기사는 제거
    else:
        print("유사한 기사 없음. 오늘 기사를 저장합니다.")
    
    return df



# def consim(data, db_df):
#     # 데이터를 행 방향으로 결합
#     combined_df = pd.concat([data, db_df], axis=0, ignore_index=True)

#     # 'USNEWS_CONTENT' 열이 존재하는지 확인
#     if 'USNEWS_CONTENT' not in combined_df.columns:
#         print("경고: 'USNEWS_CONTENT' 열이 데이터프레임에 없습니다.")
#         return pd.DataFrame()

#     # NaN 값이 있거나 빈 문자열을 제거
#     combined_df = combined_df.dropna(subset=['USNEWS_CONTENT'])
#     combined_df = combined_df[combined_df['USNEWS_CONTENT'].str.strip() != '']

#     # 유효한 데이터가 없으면 종료
#     if combined_df.empty:
#         print("경고: 'USNEWS_CONTENT' 열이 비어있거나 유효한 데이터가 없습니다.")
#         # return pd.DataFrame()
#         pass
#     else:# TF-IDF 행렬 생성
#         vectorizer = TfidfVectorizer()
#         tfidf_matrix = vectorizer.fit_transform(combined_df['USNEWS_CONTENT'])

#     # 코사인 유사도 계산
#         cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

#     # 유사도 결과를 DataFrame으로 저장
#         similarity_df = pd.DataFrame(cosine_sim, index=combined_df.index, columns=combined_df.index)

#     # 유사한 기사 인덱스를 저장할 집합
#         rows_to_drop = set()

#     # 유사도 검사: 유사도가 0.9 이상이거나 1.0인 경우 제거 대상에 추가
#         for i in range(similarity_df.shape[0]):
#             for j in range(i + 1, similarity_df.shape[1]):
#                 if 0.9 <= cosine_sim[i, j] < 1.0 or cosine_sim[i, j] == 1.0:
#                     rows_to_drop.add(similarity_df.index[j])

#     # 유사한 기사 삭제
#         if rows_to_drop:
#             print("유사한 기사 있음. 해당 기사는 삭제됩니다.")
#             combined_df = combined_df.drop(rows_to_drop, errors='ignore')
#         else:
#             print("유사한 기사 없음.")

#     # 최종 결과를 JSON으로 변환하여 데이터베이스에 삽입
#         if not combined_df.empty:
#             news_json = combined_df.to_json(orient='records', date_format='iso')
#             insert_usnews_to_db(news_json)
#         else:
#             print('기사 없어서 json변환 건너뜀')


def consim(data, db_df):
    # 데이터를 행 방향으로 결합
    combined_df = pd.concat([data, db_df], axis=0, ignore_index=True)

    # 'USNEWS_CONTENT'와 'USNEWS_TITLE' 열이 존재하는지 확인
    if 'USNEWS_CONTENT' not in combined_df.columns or 'USNEWS_TITLE' not in combined_df.columns:
        print("경고: 'USNEWS_CONTENT' 또는 'USNEWS_TITLE' 열이 데이터프레임에 없습니다.")
        return pd.DataFrame()

    # NaN 값이 있거나 빈 문자열을 제거
    combined_df = combined_df.dropna(subset=['USNEWS_CONTENT', 'USNEWS_TITLE'])
    combined_df = combined_df[(combined_df['USNEWS_CONTENT'].str.strip() != '') & (combined_df['USNEWS_TITLE'].str.strip() != '')]

    # 유효한 데이터가 없으면 종료
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