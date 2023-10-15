import time

def aws_error_handler(func):
    """
    AWS API 응답을 확인하여 에러 발생시 정보를 출력하고 예외를 발생시킵니다.

    Parameters:
    func (callable): AWS API를 호출하는 함수입니다.

    Returns:
    callable: 에러 처리 기능이 추가된 래퍼 함수입니다.
    """
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        
        # 응답 코드가 200이 아닌 경우 에러 정보 출력
        if response.status_code != 200:
            print('---')
            print(f'status_code : {response.status_code}')
            print(f'x-amzn-ErrorType : {response.headers.get("x-amzn-ErrorType")}')
            print(f'message : {response.json().get("message")}')
            print('---')
            raise Exception('API ERROR')
        
        return response
    return wrapper


def stopwatch(func):
    """
    함수의 실행 시간을 측정하고 출력합니다.

    Parameters:
    func (callable): 실행 시간을 측정할 함수입니다.

    Returns:
    callable: 실행 시간 측정 기능이 추가된 래퍼 함수입니다.
    """
    def wrapper(*args, **kwargs):
        print(f'[{func.__name__} 실행 시작]')
        
        start_time = time.time()
        result = func(*args, **kwargs)  # 함수의 리턴 값을 받을 수 있도록 수정
        
        print(f'[{func.__name__} 실행 완료]')
        print(f'- 수행시간 : {(time.time() - start_time):.4f}초')  # 소수점 4자리까지 출력
        
        return result  # 함수의 리턴 값을 반환하도록 수정
    return wrapper
