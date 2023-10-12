import requests
import pandas as pd

def ko_etf_list() -> pd.DataFrame:
    '''
    [네이버페이 증권 ETF](https://finance.naver.com/sise/etf.naver) 목록을 데이터프레임으로 받아옴
    '''
    DOMAIN = 'https://finance.naver.com'
    PATH = '/api/sise/etfItemList.nhn'
    URL = f'{DOMAIN}{PATH}'
    response = requests.get(URL)
    data = response.json().get('result').get('etfItemList')
    df = pd.DataFrame(data)
    return df.set_index('itemcode')

def not_contain(*kwds, column_name='itemname') -> str:
    '''
    Column에 특정 키워드들이 포함되지 않은 것만 검색하는 쿼리 작성
    '''
    return ' and '.join([f'not {column_name}.str.contains("{kwd}")'
            for kwd in kwds])

def filtered_ko_etf(
    exclude_kwds=('합성', '액티브', '레버리지', '2X',
                '혼합', '금리', '단기', '3년', '배당', 'TR'),
    market_cap=1000) -> pd.DataFrame:
    '''
    특정 키워드를 제외하고, 시가총액으로 필터링한 ETF 데이터
    '''
    ko_etf = ko_etf_list()
    ko_etf.dropna(inplace=True) # 상장 후 3개월 미만 제외
    if exclude_kwds:
        ko_etf.query(
            not_contain(*exclude_kwds, 'itemname'), inplace=True)
    # 종목명 특정 키워드 포함 종목 제외
    ko_etf.query(f'marketSum >= {market_cap}', inplace=True)
    # 시총 {market_cap}억 미만 제외
    ko_etf['group'] = ko_etf['itemname'].str.replace('^\S* ', '', regex=True)
    # 중복 종목 제거 (시총 큰 것만 남김)
    return ko_etf.reset_index()\
        .groupby('group').first()\
        .set_index('itemcode')\
        .sort_values('marketSum', ascending=False)