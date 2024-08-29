import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# 샘플 데이터 생성
data = pd.DataFrame({
    'USNEWS_TITLE': ['News about AI', 'AI technology advances', 'New AI product released', 'AI product release', 'Brand new AI product launch'],
    'USNEWS_CONTENT': ['This is a news article about AI.', 'The technology behind AI is advancing rapidly.',
                        'Today, a new AI product was released.', 'A new AI product has been released today.', 'A brand new AI product has just been launched.']
})

db_df = pd.DataFrame({
    'USNEWS_TITLE': ['News about AI', 'Latest AI news', 'AI technology advancements', 'Brand new AI product launch'],
    'USNEWS_CONTENT': ['This is a news article about AI.','Find out the latest news about AI.', 'AI technology is progressing at a rapid pace.',
                        'A brand new AI product has just been launched.']
})

# 데이터프레임 병합
combined_df = pd.concat([data, db_df], axis=0, ignore_index=True)

# NaN 값, 빈 row 제거
combined_df = combined_df.dropna(subset=['USNEWS_CONTENT', 'USNEWS_TITLE'])
combined_df = combined_df[(combined_df['USNEWS_CONTENT'].str.strip() != '') & (combined_df['USNEWS_TITLE'].str.strip() != '')]

# 기사가 없으면 종료
if combined_df.empty:
    print("경고: 'USNEWS_CONTENT' 또는 'USNEWS_TITLE' 열이 비어있거나 유효한 데이터가 없습니다.")
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

    # 유사한 기사 인덱스를 저장할 리스트
    rows_to_drop = []

    # 유사도 검사: 유사도가 0.9 이상이거나 1.0인 경우 제거 대상에 추가
    for i in range(similarity_df.shape[0]):
        for j in range(i + 1, similarity_df.shape[1]):
            if cosine_sim[i, j] >= 0.5:
                print(f"유사한 기사 발견: {i}와 {j} (유사도: {cosine_sim[i, j]})")
                rows_to_drop.append(j) # append
                rows_to_drop.append(i)

    # 유사한 기사 삭제
    if rows_to_drop:
        print(f"유사한 기사 있음. 해당 기사는 삭제됩니다. 삭제 인덱스: {rows_to_drop}")
        combined_df = combined_df.drop(rows_to_drop, axis=0)
    else:
        print("유사한 기사 없음.")

    # 최종 결과 출력
    print(combined_df[['USNEWS_TITLE', 'USNEWS_CONTENT']])
