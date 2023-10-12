import requests
import pandas as pd

def ko_etf_list() -> pd.DataFrame:
    DOMAIN = 'https://finance.naver.com'
    PATH = '/api/sise/etfItemList.nhn'
    URL = f'{DOMAIN}{PATH}'
    response = requests.get(URL)
    data = response.json().get('result').get('etfItemList')
    df = pd.DataFrame(data)
    return df