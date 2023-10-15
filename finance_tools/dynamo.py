import datetime
import requests
from finance_tools.decorator import aws_error_handler

class DynamoDB:
    def __init__(self, URL: str, API_KEY: str, TABLE_NAME: str):
        self.URL = URL
        self.API_KEY = API_KEY
        self.TABLE_NAME = TABLE_NAME

    @aws_error_handler
    def put_item(self, item: dict):
        data = {'TableName': self.TABLE_NAME, 'Item': item}
        headers = {'x-api-key': self.API_KEY}
        response = requests.post(self.URL, json=data, headers=headers)
        return response
    
    @aws_error_handler
    def query(self, expr:str, attr_val:dict):
        data = {'TableName': self.TABLE_NAME,
                'KeyConditionExpression': expr,
                'ExpressionAttributeValues': attr_val}
        headers = {'x-api-key': self.API_KEY}
        response = requests.post(self.URL + '/query', json=data, headers=headers)
        return response