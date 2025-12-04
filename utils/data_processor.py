"""
데이터 처리 유틸리티
- 데이터 변환, 계산 등의 공통 함수를 제공합니다
"""

import decimal


def convert_decimal_to_float(obj):
    """
    Decimal 타입을 float로 변환 (JSON 직렬화용)
    
    Args:
        obj: 변환할 객체 (dict, list, Decimal 등)
    
    Returns:
        변환된 객체
    
    사용 예시:
        records = convert_decimal_to_float(df.to_dicts())
    """
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: convert_decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_decimal_to_float(item) for item in obj]
    return obj


def calculate_percentage_change(current, previous):
    """
    전년대비 증감률 계산
    
    Args:
        current: 당해 값
        previous: 전년 값
    
    Returns:
        float: 증감률 (%)
    """
    if previous == 0:
        return 100.0 if current > 0 else 0.0
    return ((current - previous) / previous) * 100


def format_amount_million(amount):
    """
    금액을 백만원 단위로 변환하고 포맷팅
    
    Args:
        amount: 원 단위 금액
    
    Returns:
        float: 백만원 단위 금액 (소수점 2자리)
    """
    return round(float(amount) / 1000000, 2)






