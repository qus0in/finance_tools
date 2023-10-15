from multiprocessing import Pool, cpu_count
import pandas as pd

from finance_tools.npf import get_filtered_etf_list
from finance_tools.dynamo import DynamoDB
from finance_tools.decorator import stopwatch
import plotly.express as px

class SwitchingStrategyHelper:
    def __init__(self,
                 db_client: DynamoDB,
                 exclude_kwds,
                 market_cap: int,
                 use_multiprocessing: bool = False) -> None:
        self.db_client:DynamoDB = db_client
        self.universe:pd.DataFrame = get_filtered_etf_list(exclude_kwds, market_cap)
        self.multiprocessing:bool = use_multiprocessing

    @stopwatch
    def put_etf(self):
        # etf DataFrame에서 itemname 칼럼의 아이템을 추출
        data = list(self.universe.reset_index().iterrows())
        
        # 멀티프로세싱을 사용하지 않을 경우
        if not self.multiprocessing:
            for row in data:
                self.handle_data(row)
            return
        
        # 멀티프로세싱을 사용할 경우
        with Pool(cpu_count() * 2) as p:
            p.map(self.handle_data, data)

    def handle_data(self, row):
        now_str = datetime.datetime.utcnow().strftime('%Y%m%d')
        i, v = row
        item = {
            'itemcode': {'S': v['itemcode']},
            'itemname': {'S': v['itemname']},
            'marketSum': {'N': str(v['marketSum'])},
            'groupMarketSum': {'N': str(v['groupMarketSum'])},
            'updatedAt': {'S': now_str},
        }
        self.db_client.put_item(item)
    
    def draw_treemap(self):
        u = self.universe.copy()
        u['group_name'] = u.itemname.replace('^\S+ ', '', regex=True)
        fig = px.treemap(
            u,
            path=['category', 'group_name'],
            values='groupMarketSum'
        )
        return fig