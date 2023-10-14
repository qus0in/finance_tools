import requests, ast
import pandas as pd


def get_etf_list() -> pd.DataFrame:
    """
    Naver Finance API를 이용하여 ETF 목록 정보를 DataFrame 형태로 반환합니다.
    
    Parameters:
    없음
    
    Returns:
    DataFrame: 'itemcode'를 인덱스로 가지며, ETF 정보가 컬럼에 담겨 있습니다.
    
    """
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


def get_exclude_query(*kwds, column_name:str='itemname') -> str:
    """
    주어진 키워드를 이용하여 Pandas DataFrame에서 특정 컬럼에서 해당 키워드를 제외하는 쿼리 문자열을 생성합니다.
    
    Parameters:
    *kwds: 가변 인자, 제외하려는 키워드들입니다. 문자열 형태의 여러 키워드를 입력할 수 있습니다.
    column_name (str, optional): 키워드를 제외할 컬럼의 이름입니다. 기본값은 'itemname'.
    
    Returns:
    str: 키워드를 제외하는데 사용할 수 있는 Pandas DataFrame 쿼리 문자열입니다.
    
    """
    # 주어진 키워드들을 사용하여 "not column_name.str.contains('kwd')" 형태의 쿼리 문자열을 리스트로 생성
    # 예를 들어, kwds가 ['apple', 'banana']이고 column_name이 'itemname'이라면,
    # 결과 리스트는 ['not itemname.str.contains("apple")', 'not itemname.str.contains("banana")']
    query_list = [f'not {column_name}.str.contains("{kwd}")' for kwd in kwds]
    
    # 생성된 쿼리 문자열 리스트를 'and'로 연결하여 하나의 문자열로 만듦
    return ' and '.join(query_list)


def get_filtered_etf_list(
    exclude_kwds=('합성', '액티브', '레버리지', '2X',
                '혼합', '금리', '단기', '3년', '배당', 'TR'),
    market_cap:int=1000) -> pd.DataFrame:
    """
    특정 키워드를 제외하고 시가총액이 일정 이상인 ETF 목록을 필터링하여 DataFrame으로 반환합니다.
    
    Parameters:
    exclude_kwds (tuple, optional): 제외할 키워드들의 튜플입니다. 기본값은 ('합성', '액티브', '레버리지', '2X', '혼합', '금리', '단기', '3년', '배당', 'TR').
    market_cap (int, optional): 필터링할 시가총액의 최소 값입니다. 기본값은 1000.
    
    Returns:
    DataFrame: 필터링된 ETF 정보를 담고 있는 DataFrame입니다. 'itemcode'를 인덱스로 가집니다.
    
    """
    # ko_etf_list() 함수로부터 ETF 목록을 가져옴 (이 부분은 예시이며 실제로는 해당 함수의 정의가 필요합니다.)
    ko_etf = ko_etf_list()
    
    # 결측값을 제거
    ko_etf.dropna(inplace=True)
    
    # 제외 키워드가 있으면, 해당 키워드를 'itemname' 컬럼에서 제거
    if exclude_kwds:
        query_str = get_exclude_query(*exclude_kwds, column_name='itemname')  # get_exclude_query 함수를 사용하여 쿼리 문자열 생성
        ko_etf.query(query_str, inplace=True)
    
    # 시가총액이 일정 금액 이상인 ETF만 선택
    ko_etf.query(f'marketSum >= {market_cap}', inplace=True)
    
    # 'group' 컬럼 생성: 'itemname' 컬럼의 첫 단어를 제거한 나머지 문자열
    ko_etf['group'] = ko_etf['itemname'].str.replace('^\S* ', '', regex=True)
    
    # 'group'으로 그룹화한 후 첫 번째 데이터를 선택하고, 'itemcode'를 인덱스로 설정하여 반환
    return ko_etf.reset_index()\
        .groupby('group').first()\
        .set_index('itemcode')\
        .sort_values('marketSum', ascending=False)


def get_prices(symbol: str) -> pd.DataFrame:
    """
    Naver Finance API를 이용하여 주식 시세 정보를 DataFrame 형태로 반환합니다.
    
    Parameters:
    symbol (str): 조회할 주식의 심볼 혹은 코드. 예를 들어, '005930'이면 삼성전자.
    
    Returns:
    DataFrame: 날짜를 인덱스로 가지고, 각 컬럼에는 '시가', '고가', '저가', '종가', '거래량', '외국인소진율' 등의 정보를 담고 있습니다.
    
    """
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