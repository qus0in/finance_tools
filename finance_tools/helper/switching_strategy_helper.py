from multiprocessing import Pool, cpu_count
import pandas as pd
from finance_tools.dynamo import DynamoDB
from finance_tools.decorator import stopwatch

class SwitchingStrategyHelper:
    @classmethod
    @stopwatch
    def put_etf(cls, client: DynamoDB, etf: pd.DataFrame, multiprocessing: bool = False):
        """
        ETF 데이터를 DynamoDB 테이블에 추가합니다.

        Parameters:
        - client (DynamoDB): DynamoDB 클라이언트 객체
        - etf (pd.DataFrame): 추가할 ETF 정보 (Pandas DataFrame 형태)
        - multiprocessing (bool): 멀티프로세싱을 사용할지 여부 (기본값: False)

        Returns:
        - None
        """
        
        # etf DataFrame에서 itemname 칼럼의 아이템을 추출
        data = etf.itemname.items()
        
        # 멀티프로세싱을 사용하지 않을 경우
        if not multiprocessing:
            for row in data:
                client.handle_data(row)
            return
        
        # 멀티프로세싱을 사용할 경우
        with Pool(cpu_count() * 2) as p:
            p.map(client.handle_data, data)