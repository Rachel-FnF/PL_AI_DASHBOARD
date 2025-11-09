# PL AI Dashboard - 간단 버전

`simple_analyzer.py` 하나로 모든 분석을 수행합니다.

## 설치

```bash
pip install -r requirements.txt
```

## 설정

프로젝트 루트에 `.env` 파일을 생성하고 다음 변수들을 설정하세요:

```
# Snowflake DB 연결
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role

# Claude API
CLAUDE_API_KEY=your_api_key
```

## 사용 방법

1. `simple_analyzer.py` 파일을 열어서 맨 아래 설정을 변경:
```python
yyyymm = '202509'  # 분석할 년월
brd_cd = 'M'       # 브랜드 코드 (M=MLB, X=DISCOVERY, I=MLB KIDS 등)
```

2. 실행:
```bash
python simple_analyzer.py
```

3. 결과 확인:
- Markdown: `output/md/` 폴더
- JSON: `output/json/` 폴더

## 새로운 분석 추가하기

`simple_analyzer.py` 파일에 다음을 추가하면 됩니다:

1. **SQL 쿼리 함수 추가**:
```python
def get_my_query(yyyymm, brd_cd):
    return f"""
    SELECT ...
    FROM ...
    WHERE yyyymm = '{yyyymm}' AND brd_cd = '{brd_cd}'
    """
```

2. **분석 함수 추가**:
```python
def analyze_my_data(yyyymm, brd_cd):
    engine = get_db_engine()
    try:
        sql = get_my_query(yyyymm, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        # LLM 호출
        prompt = f"데이터 분석해줘: {records}"
        analysis = call_llm(prompt)
        
        # 저장
        filename = f"01.{brd_cd}_내분석"
        save_markdown(analysis, filename)
        save_json({'data': records, 'analysis': analysis}, filename)
    finally:
        engine.dispose()
```

3. **메인에서 호출**:
```python
if __name__ == '__main__':
    yyyymm = '202509'
    brd_cd = 'M'
    analyze_my_data(yyyymm, brd_cd)
```

## 주요 함수

- `get_db_engine()`: Snowflake DB 연결
- `run_query(sql, engine)`: SQL 쿼리 실행
- `call_llm(prompt)`: Claude API 호출
- `save_markdown(content, filename)`: Markdown 파일 저장
- `save_json(data, filename)`: JSON 파일 저장

