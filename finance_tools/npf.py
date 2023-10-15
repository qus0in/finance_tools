import requests, ast
import pandas as pd

def get_etf_list() -> pd.DataFrame:
    # Naver Finance API의 도메인과 경로 설정
    DOMAIN = 'https://finance.naver.com'
    PATH = '/api/sise/etfItemList.nhn'
    URL = f'{DOMAIN}{PATH}'
    
    # API 요청 후 응답 받기
    response = requests.get(URL)
    
    # JSON 형태의 응답을 파싱하여 필요한 정보만 추출
    data = response.json().get('result').get('etfItemList')
    
    # 추출한 데이터를 DataFrame으로 변환
    df = pd.DataFrame(data)
    
    # 'itemcode' 컬럼을 인덱스로 설정 후 DataFrame 반환
    return df.set_index('itemcode')


def get_exclude_query(*kwds:list, column_name:str='itemname') -> str:
    # 주어진 키워드들을 사용하여 "not column_name.str.contains('kwd')" 형태의 쿼리 문자열을 리스트로 생성
    # 예를 들어, kwds가 ['apple', 'banana']이고 column_name이 'itemname'이라면,
    # 결과 리스트는 ['not itemname.str.contains("apple")', 'not itemname.str.contains("banana")']
    query_list = [f'not {column_name}.str.contains("{kwd}")' for kwd in kwds]
    
    # 생성된 쿼리 문자열 리스트를 'and'로 연결하여 하나의 문자열로 만듦
    return ' and '.join(query_list)


def get_filtered_etf_list(
    exclude_kwds:list,
    market_cap:int) -> pd.DataFrame:
    # get_etf_list() 함수로부터 ETF 목록을 가져옴 (이 부분은 예시이며 실제로는 해당 함수의 정의가 필요합니다.)
    ko_etf = get_etf_list()
    
    # 결측값을 제거
    ko_etf.dropna(inplace=True)
    
    # 제외 키워드가 있으면, 해당 키워드를 'itemname' 컬럼에서 제거
    if exclude_kwds:
        query_str = get_exclude_query(*exclude_kwds, column_name='itemname')  # get_exclude_query 함수를 사용하여 쿼리 문자열 생성
        ko_etf.query(query_str, inplace=True)
    
    # 시가총액이 일정 금액 이상인 ETF만 선택
    ko_etf.query(f'marketSum >= {market_cap}', inplace=True)
    
    # 'group' 컬럼 생성: 'itemname' 컬럼의 첫 단어를 제거한 나머지 문자열
    ko_etf['group'] = ko_etf['itemname']\
        .str.replace('^\S* ', '', regex=True)\
        .str.replace('(H)', '', regex=False)\
        .str.replace('선물', '', regex=False)\
        .str.replace('TR$', '', regex=True)
    gb = ko_etf.reset_index().groupby('group')
    # 'group'으로 그룹화한 후 첫 번째 데이터를 선택하고, 'itemcode'를 인덱스로 설정하여 반환
    fst = gb.first()
    fst['groupMarketSum'] = gb.marketSum.sum()
    return fst\
        .set_index('itemcode')\
        .sort_values('marketSum', ascending=False)


def get_prices(symbol: str) -> pd.DataFrame:
    # Naver Finance API의 도메인과 경로 설정
    DOMAIN = 'https://api.finance.naver.com'
    PATH = 'siseJson.naver'
    URL = f'{DOMAIN}/{PATH}'
    
    # API에 전달할 파라미터 설정
    params = dict(
        symbol=symbol,                # 주식 심볼
        requestType=1,                # 요청 유형 (1은 일반적으로 시세 정보 요청)
        startTime='19000101',         # 조회 시작 날짜
        endTime='20991231',           # 조회 종료 날짜
        timeframe='day'                # 시간 단위 (여기서는 'day'로 설정)
    )
    
    # API 요청 후 응답 받기
    response = requests.get(URL, params)
    
    # 응답 데이터를 이중 리스트로 변환
    # '\n'을 제거하여 문자열을 정리한 후, ast.literal_eval로 파싱
    data = ast.literal_eval(response.text.replace('\n', ''))
    
    # 데이터를 DataFrame으로 변환
    df = pd.DataFrame(data[1:], columns=data[0])
    
    # '날짜' 컬럼을 datetime 형식으로 변환
    df.날짜 = pd.to_datetime(df.날짜, format='%Y%m%d')
    
    # '날짜' 컬럼을 인덱스로 설정 후 DataFrame 반환
    return df.set_index('날짜')