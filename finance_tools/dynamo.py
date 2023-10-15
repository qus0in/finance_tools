import datetime
import requests
from finance_tools.decorator import aws_error_handler

class DynamoDB:
    def __init__(self, URL: str, API_KEY: str, TABLE_NAME: str):
        """
        DynamoDB 객체를 초기화합니다.

        Parameters:
        - URL (str): API endpoint URL
        - API_KEY (str): API 키
        - TABLE_NAME (str): DynamoDB 테이블 이름
        """
        self.URL = URL
        self.API_KEY = API_KEY
        self.TABLE_NAME = TABLE_NAME

    @aws_error_handler
    def put_item(self, item: dict):
        """
        테이블에 아이템을 추가합니다.

        Parameters:
        - item (dict): 추가할 아이템의 정보 (Python dictionary 형태)

        Returns:
        - Response object: API 응답 객체
        """
        data = {'TableName': self.TABLE_NAME, 'Item': item}
        headers = {'x-api-key': self.API_KEY}
        response = requests.post(self.URL, json=data, headers=headers)
        return response

    def handle_data(self, row):
        """
        개별 데이터를 처리하여 테이블에 추가합니다.

        Parameters:
        - row (tuple): 처리할 데이터 (itemcode, itemname)
        """
        now_str = datetime.datetime.utcnow().strftime('%Y%m%d')
        itemcode, itemname = row
        item = {
            'itemcode': {'S': itemcode},
            'itemname': {'S': itemname},
            'updatedAt': {'S': now_str}
        }
        self.put_item(item)