"""
중국용 분석 도구 - 모든 기능이 하나의 파일에 통합됨
- SQL 쿼리 실행
- LLM 호출 (Claude)
- Markdown/JSON 파일 저장
"""

import os
import json
import polars as pl
import anthropic
from datetime import datetime, timedelta
from decimal import Decimal
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# ============================================================================
# 설정
# ============================================================================
BRAND_CODE_MAP = {
    # 중국용 브랜드 코드 매핑 (필요에 따라 수정)
    'M': 'MLB',
    'I': 'MLB KIDS',
    'X': 'DISCOVERY',
    'V': 'DUVETICA',
    'ST': 'SERGIO TACCHINI',
    'W': 'SUPRA',
}

OUTPUT_JSON_PATH = './cn_output/json'
OUTPUT_MD_PATH = './cn_output/md'

# 출력 폴더 생성
os.makedirs(OUTPUT_JSON_PATH, exist_ok=True)
os.makedirs(OUTPUT_MD_PATH, exist_ok=True)

# 채널 순서 정의 (JSON/MD 추출 시 사용)
CHANNEL_ORDER = [
    '(EC)티몰',
    '(EC)틱톡/JD',
    '(EC)할인몰',
    '(OFF)플래그쉽',
    '(OFF)쇼핑몰',
    '(OFF)아울렛',
    '(EC)대리상',
    '(OFF)대리상',
]

# ============================================================================
# DB 연결
# ============================================================================
def get_db_engine():
    """Snowflake DB 연결 엔진 생성"""
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    role = os.getenv('SNOWFLAKE_ROLE')
    
    if not all([account, user, password, database, schema, warehouse, role]):
        raise ValueError("Snowflake 환경 변수가 설정되지 않았습니다. .env 파일을 확인하세요.")
    
    return create_engine(
        URL(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role,
        )
    )

# ============================================================================
# SQL 쿼리 실행
# ============================================================================
def run_query(sql, engine):
    """SQL 쿼리 실행하고 DataFrame 반환"""
    print(f"[SQL] 쿼리 실행 중...")
    df = pl.read_database(sql, engine)
    print(f"[OK] {len(df)}개 행 조회 완료")
    return df

# ============================================================================
# LLM 호출
# ============================================================================
# 전역 토큰 사용량 추적
_total_tokens_used = {'input': 0, 'output': 0}

def call_llm(prompt, max_tokens=4000, temperature=0.7):
    """Claude API 호출"""
    api_key = os.getenv('CLAUDE_API_KEY')
    if not api_key:
        raise ValueError("CLAUDE_API_KEY가 설정되지 않았습니다. .env 파일을 확인하세요.")
    
    client = anthropic.Anthropic(api_key=api_key, timeout=120.0)
    
    system_prompt = """
당신은 F&F 그룹의 최고 전략 분석가입니다. 다음 원칙을 반드시 준수하세요:

📊 **분석 원칙**
- 숫자는 절대 변형하지 말고 원본 그대로 사용
- 모든 금액은 k(천) 단위로 표시 (원본 데이터를 1,000으로 나누어 표기)
- 단위는 k, 3자리마다 쉼표 표기
- ⚠️ **중요: 천 단위 표시 시 반드시 정수로 표기하고 소수점을 사용하지 말 것**
  - 올바른 예: 1,234k, 588k, 1,378k
  - 잘못된 예: 1,234.56k, 588.67k, 1,378.0k (절대 사용 금지)
  - 소수점이 있는 경우 반올림하여 정수로 표기 (예: 588.67 → 589k, 1,378.0 → 1,378k)
- 비중(%)은 소수점 첫째자리까지 표현
- 매출액은 act_sale_amt 컬럼 사용할것 매출액(v+)라고 표현하기
- 할인율 계산은 act_sale_amt / tag_sale_amt 사용
- 직접이익률 계산 시 직업이익 / (act_sale_amt/1.1) 사용
- 영업이익률 계산 시 영업이익 / (act_sale_amt/1.1) 사용

🎯 **보고 스타일**
- 경영관리팀 대상의 전략적 관점
- 즉시 실행 가능한 구체적 액션플랜 제시
- 리스크와 기회를 명확히 구분
- 근거 기반의 객관적 분석
- 이상징후나 특이사항 언급
"""
    
    full_prompt = system_prompt + "\n\n" + prompt
    
    print(f"[LLM] Claude API 호출 중...")
    message = client.messages.create(
        model='claude-sonnet-4-20250514',
        max_tokens=max_tokens,
        temperature=temperature,
        messages=[{"role": "user", "content": full_prompt}]
    )
    
    # 토큰 사용량 추적
    if hasattr(message, 'usage') and message.usage:
        input_tokens = message.usage.input_tokens if hasattr(message.usage, 'input_tokens') else 0
        output_tokens = message.usage.output_tokens if hasattr(message.usage, 'output_tokens') else 0
        _total_tokens_used['input'] += input_tokens
        _total_tokens_used['output'] += output_tokens
        print(f"[OK] LLM 응답 완료 (입력: {input_tokens:,} 토큰, 출력: {output_tokens:,} 토큰, 총: {input_tokens + output_tokens:,} 토큰)")
    else:
        print(f"[OK] LLM 응답 완료")
    
    return message.content[0].text

def get_total_tokens():
    """전체 토큰 사용량 반환"""
    return _total_tokens_used.copy()

def reset_token_counter():
    """토큰 카운터 초기화"""
    global _total_tokens_used
    _total_tokens_used = {'input': 0, 'output': 0}

# ============================================================================
# 파일 저장
# ============================================================================
def save_markdown(content, filename):
    """Markdown 파일 저장"""
    # KEY, sub_key, country 추출
    key, sub_key, country = extract_key_from_filename(filename)
    
    # YAML frontmatter 추가
    frontmatter_lines = ["---"]
    if key:
        frontmatter_lines.append(f"key: {key}")
    if sub_key:
        frontmatter_lines.append(f"sub_key: {sub_key}")
    frontmatter_lines.append(f"country: {country}")
    frontmatter_lines.append("---")
    frontmatter = "\n".join(frontmatter_lines) + "\n\n"
    
    # content 앞에 frontmatter 추가
    full_content = frontmatter + content
    
    file_path = os.path.join(OUTPUT_MD_PATH, f"{filename}.md")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(full_content)
    print(f"[OK] Markdown 저장: {file_path}")
    return file_path

class DecimalEncoder(json.JSONEncoder):
    """Decimal 타입을 float로 변환하는 JSON 인코더"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def json_dumps_safe(obj, **kwargs):
    """Decimal 타입을 안전하게 처리하는 json.dumps 래퍼"""
    return json.dumps(obj, cls=DecimalEncoder, **kwargs)

def extract_json_from_response(text):
    """
    AI 응답에서 JSON 코드 블록을 추출하고 파싱
    
    Args:
        text: AI 응답 텍스트 (JSON 코드 블록이 포함될 수 있음)
    
    Returns:
        dict: 파싱된 JSON 데이터, 실패 시 None
    """
    import re
    
    if not text:
        return None
    
    # 1. JSON 코드 블록 찾기 (```json ... ```)
    json_block_pattern = r'```json\s*(.*?)\s*```'
    match = re.search(json_block_pattern, text, re.DOTALL)
    
    if match:
        json_str = match.group(1).strip()
    else:
        # 2. 코드 블록 없으면 ``` ... ``` 찾기
        code_block_pattern = r'```\s*(.*?)\s*```'
        match = re.search(code_block_pattern, text, re.DOTALL)
        if match:
            json_str = match.group(1).strip()
            # json 마커 제거
            if json_str.startswith('json'):
                json_str = json_str[4:].strip()
        else:
            # 3. 코드 블록이 없으면 전체 텍스트에서 JSON 객체 찾기
            # { 로 시작하고 } 로 끝나는 부분 찾기 (중첩된 중괄호 처리)
            json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
            match = re.search(json_pattern, text, re.DOTALL)
            if match:
                json_str = match.group(0)
            else:
                json_str = text.strip()
    
    # JSON 파싱 시도
    try:
        parsed = json.loads(json_str)
        print(f"[OK] JSON 파싱 성공: {len(parsed.get('sections', []))}개 섹션 추출")
        return parsed
    except json.JSONDecodeError as e:
        print(f"[WARNING] JSON 파싱 실패: {str(e)[:100]}")
        # 마지막 시도: 첫 번째 { 부터 마지막 } 까지 추출
        try:
            start_idx = text.find('{')
            end_idx = text.rfind('}')
            if start_idx >= 0 and end_idx > start_idx:
                json_str = text[start_idx:end_idx+1]
                parsed = json.loads(json_str)
                print(f"[OK] JSON 파싱 성공 (재시도): {len(parsed.get('sections', []))}개 섹션 추출")
                return parsed
        except:
            pass
        return None

def sort_channels_by_order(channel_dict):
    """
    채널 딕셔너리를 정의된 순서로 정렬
    
    Args:
        channel_dict: 채널명을 키로 하는 딕셔너리
    
    Returns:
        OrderedDict: 정렬된 채널 딕셔너리
    """
    from collections import OrderedDict
    
    ordered_dict = OrderedDict()
    
    # 정의된 순서대로 채널 추가
    for channel in CHANNEL_ORDER:
        if channel in channel_dict:
            ordered_dict[channel] = channel_dict[channel]
    
    # 정의된 순서에 없는 채널들은 뒤에 추가 (알파벳 순서)
    remaining_channels = sorted([
        (k, v) for k, v in channel_dict.items() 
        if k not in CHANNEL_ORDER
    ])
    for channel, data in remaining_channels:
        ordered_dict[channel] = data
    
    return ordered_dict

def get_channel_list_sorted(channel_dict):
    """
    채널 리스트를 정의된 순서로 정렬
    
    Args:
        channel_dict: 채널명을 키로 하는 딕셔너리
    
    Returns:
        list: 정렬된 채널명 리스트
    """
    sorted_list = []
    
    # 정의된 순서대로 채널 추가
    for channel in CHANNEL_ORDER:
        if channel in channel_dict:
            sorted_list.append(channel)
    
    # 정의된 순서에 없는 채널들은 뒤에 추가 (알파벳 순서)
    remaining_channels = sorted([
        k for k in channel_dict.keys() 
        if k not in CHANNEL_ORDER
    ])
    sorted_list.extend(remaining_channels)
    
    return sorted_list

def extract_key_from_filename(filename):
    """
    파일명에서 KEY와 sub_key를 추출 (브랜드 코드 제외)
    
    파일명 형식: CN_{yyyymm_short}_{brd_cd}_{분석타입}_{세부분석}
    예시: CN_2509_M_리테일매출_채널별매출분석
    
    Returns:
        tuple: (key, sub_key, country)
        - key: 분석타입만 (예: 리테일)
        - sub_key: 세부분석만 (예: 채널별매출분석)
        - country: CN
    """
    # CN_2509_M_리테일매출_채널별매출분석 형식
    parts = filename.split('_')
    if len(parts) < 4:
        return None, None, 'CN'
    
    # CN 제거하고 나머지 부분 사용
    if parts[0] == 'CN':
        parts = parts[1:]  # ['2509', 'M', '리테일', '채널별매출분석']
    
    if len(parts) < 3:
        return None, None, 'CN'
    
    # yyyymm_short, brd_cd, 나머지
    analysis_parts = parts[2:]  # ['리테일', '채널별매출분석']
    
    if len(analysis_parts) == 0:
        return None, None, 'CN'
    
    # KEY: 첫번째 분석타입만 (브랜드 코드 제외)
    key = analysis_parts[0]  # '리테일'
    
    # sub_key: 두번째부터 끝까지 (브랜드 코드 제외)
    if len(analysis_parts) > 1:
        sub_key = '_'.join(analysis_parts[1:])  # '채널별매출분석'
    else:
        sub_key = None
    
    return key, sub_key, 'CN'

def save_json(data, filename):
    """JSON 파일 저장 - 필드 순서: country, brand_cd, brand_name, yyyymm, yyyymm_py, key, sub_key, analysis_data, ..."""
    # KEY, sub_key, country 추출
    key, sub_key, country = extract_key_from_filename(filename)
    
    # JSON 데이터에 KEY, sub_key, country 추가 (지정된 순서로)
    if isinstance(data, dict):
        # 순서를 보장하기 위해 OrderedDict 사용
        from collections import OrderedDict
        new_data = OrderedDict()
        
        # 1. country (항상 첫 번째)
        if 'country' in data:
            new_data['country'] = data['country']
        elif country:
            new_data['country'] = country
        
        # 2. brand_cd
        if 'brand_cd' in data:
            new_data['brand_cd'] = data['brand_cd']
        
        # 3. brand_name
        if 'brand_name' in data:
            new_data['brand_name'] = data['brand_name']
        
        # 4. yyyymm
        if 'yyyymm' in data:
            new_data['yyyymm'] = data['yyyymm']
        
        # 5. yyyymm_py
        if 'yyyymm_py' in data:
            new_data['yyyymm_py'] = data['yyyymm_py']
        
        # 6. key
        if 'key' in data:
            new_data['key'] = data['key']
        elif key:
            new_data['key'] = key
        
        # 7. sub_key
        if 'sub_key' in data:
            new_data['sub_key'] = data['sub_key']
        elif sub_key:
            new_data['sub_key'] = sub_key
        
        # 8. analysis_data
        if 'analysis_data' in data:
            new_data['analysis_data'] = data['analysis_data']
        
        # 나머지 필드들 (summary, channel_summary, raw_data 등)
        for k, v in data.items():
            if k not in ['country', 'brand_cd', 'brand_name', 'yyyymm', 'yyyymm_py', 'key', 'sub_key', 'analysis_data']:
                new_data[k] = v
        
        data = dict(new_data)  # OrderedDict를 일반 dict로 변환 (Python 3.7+에서는 순서 보장)
    
    file_path = os.path.join(OUTPUT_JSON_PATH, f"{filename}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, cls=DecimalEncoder)
    print(f"[OK] JSON 저장: {file_path}")
    return file_path

# ============================================================================
# SQL 쿼리 함수들 (사용자가 채워넣을 부분)
# ============================================================================
def get_retail_channel_sales_query(yyyymm, yyyymm_py, brd_cd):
    """
    리테일 채널별 매출분석 쿼리 (당해/전년 동월 비교)
    
    Args:
        yyyymm: 당해 년월 (예: '202510')
        yyyymm_py: 전년 동월 (예: '202410')
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리 문자열
    """
    # TODO: SQL 쿼리 작성 필요
    sql = f"""
    -- 리테일 채널별 매출분석 쿼리
    -- 당해: {yyyymm}, 전년: {yyyymm_py}, 브랜드: {brd_cd}
    with param as (
  select 'CY' as div
     , '{yyyymm}' as std_yyyymm
  union all
  select 'PY' as div
     , '{yyyymm_py}' as std_yyyymm
)
, chnl_std as (
    select map_shop_agnt_cd  -- 매장, 대라상 코드
         , mgmt_chnl_nm as chnl_std  -- 채널명
    from sap_fnf.mst_shop
    group by 1,2
)
, raw as (
    select a.yymm
        , a.brd_cd
        , b.chnl_std
        , c.prdt_hrrc3_nm as class3 -- 아이템
        , a.prdt_cd -- 품번
        , max(c.prdt_nm) as prdt_nm -- 제품명
        , sum(a.sale_amt) sale_amt -- 실판가 v+
    from chn.dm_sh_s_m a -- bos 매출
    join chnl_std b
      on a.map_shop_agnt_cd = b.map_shop_agnt_cd
    join sap_fnf.mst_prdt c
      on a.prdt_cd = c.prdt_cd
    join param p
      on a.yymm = p.std_yyyymm
    where 1=1
      and a.brd_cd = '{brd_cd}'
    group by a.yymm
        , a.brd_cd
        , b.chnl_std
        , c.prdt_hrrc3_nm
        , a.prdt_cd
)
select *
from raw
    """
    return sql

def get_outbound_category_sales_query(yyyymm, yyyymm_py, brd_cd):
    """
    출고카테고리별 매출분석 쿼리 (당해/전년 동월 비교)
    
    Args:
        yyyymm: 당해 년월 (예: '202510')
        yyyymm_py: 전년 동월 (예: '202410')
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리 문자열
    """
    # TODO: SQL 쿼리 작성 필요
    sql = f"""
    -- 출고카테고리별 매출분석 쿼리
    -- 당해: {yyyymm}, 전년: {yyyymm_py}, 브랜드: {brd_cd}
    SELECT 
        -- 여기에 SQL 쿼리 작성
        1 as placeholder
    """
    return sql

def get_agent_store_sales_query(yyyymm, yyyymm_py, brd_cd):
    """
    대리상 점당매출 종합분석 쿼리 (당해/전년 동월 비교)
    
    Args:
        yyyymm: 당해 년월 (예: '202510')
        yyyymm_py: 전년 동월 (예: '202410')
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리 문자열
    """
    # TODO: SQL 쿼리 작성 필요
    sql = f"""
    -- 대리상 점당매출 종합분석 쿼리
    -- 당해: {yyyymm}, 전년: {yyyymm_py}, 브랜드: {brd_cd}
    SELECT 
        -- 여기에 SQL 쿼리 작성
        1 as placeholder
    """
    return sql

def get_discount_rate_query(yyyymm, yyyymm_py, brd_cd):
    """
    할인율 종합분석 쿼리 (당해/전년 동월 비교)
    
    Args:
        yyyymm: 당해 년월 (예: '202510')
        yyyymm_py: 전년 동월 (예: '202410')
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리 문자열
    """
    # TODO: SQL 쿼리 작성 필요
    sql = f"""
    -- 할인율 종합분석 쿼리
    -- 당해: {yyyymm}, 전년: {yyyymm_py}, 브랜드: {brd_cd}
    SELECT 
        -- 여기에 SQL 쿼리 작성
        1 as placeholder
    """
    return sql

def get_operating_expense_query(yyyymm, yyyymm_py, brd_cd):
    """
    영업비 종합분석 쿼리 (당해/전년 동월 비교)
    
    Args:
        yyyymm: 당해 년월 (예: '202510')
        yyyymm_py: 전년 동월 (예: '202410')
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리 문자열
    """
    # TODO: SQL 쿼리 작성 필요
    sql = f"""
    -- 영업비 종합분석 쿼리
    -- 당해: {yyyymm}, 전년: {yyyymm_py}, 브랜드: {brd_cd}
    SELECT 
        -- 여기에 SQL 쿼리 작성
        1 as placeholder
    """
    return sql

# ============================================================================
# 분석 함수들
# ============================================================================
def analyze_retail_channel_sales(yyyymm, brd_cd):
    """리테일 채널별 매출분석"""
    print(f"\n{'='*60}")
    print(f"리테일 채널별 매출분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (당해/전년 동월)
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # SQL 쿼리 실행
        sql = get_retail_channel_sales_query(yyyymm, yyyymm_py, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('SALE_AMT', 0)) for r in records if 'SALE_AMT' in r)
        unique_channels = len(set(r.get('CHNL_STD', '') for r in records if r.get('CHNL_STD')))
        unique_items = len(set(r.get('CLASS3', '') for r in records if r.get('CLASS3')))
        
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)")
        print(f"채널 수: {unique_channels}개")
        print(f"아이템 수: {unique_items}개")
        
        # 채널별 요약 데이터 생성
        channel_summary = {}
        for record in records:
            chnl_std = record.get('CHNL_STD', '기타')
            yymm = record.get('YYMM', '')
            sale_amt = float(record.get('SALE_AMT', 0))
            
            if chnl_std not in channel_summary:
                channel_summary[chnl_std] = {
                    'total_sales': 0,
                    'months': {},
                    'top_items': []
                }
            
            channel_summary[chnl_std]['total_sales'] += sale_amt
            
            if yymm not in channel_summary[chnl_std]['months']:
                channel_summary[chnl_std]['months'][yymm] = 0
            channel_summary[chnl_std]['months'][yymm] += sale_amt
        
        # 채널별 상위 아이템 추출
        item_sales_by_channel = {}
        for record in records:
            chnl_std = record.get('CHNL_STD', '기타')
            class3 = record.get('CLASS3', '기타')
            sale_amt = float(record.get('SALE_AMT', 0))
            
            key = f"{chnl_std}|{class3}"
            if key not in item_sales_by_channel:
                item_sales_by_channel[key] = {
                    'chnl_std': chnl_std,
                    'class3': class3,
                    'total_sales': 0
                }
            item_sales_by_channel[key]['total_sales'] += sale_amt
        
        # 채널별로 상위 5개 아이템 추출
        for chnl_std in channel_summary:
            items = [
                {'class3': v['class3'], 'total_sales': v['total_sales']}
                for k, v in item_sales_by_channel.items()
                if v['chnl_std'] == chnl_std
            ]
            items.sort(key=lambda x: x['total_sales'], reverse=True)
            channel_summary[chnl_std]['top_items'] = items[:5]
        
        # 당해/전년 비교 데이터 생성
        total_sales_cy = sum(
            float(r.get('SALE_AMT', 0)) for r in records 
            if r.get('YYMM') == yyyymm and 'SALE_AMT' in r
        )
        total_sales_py = sum(
            float(r.get('SALE_AMT', 0)) for r in records 
            if r.get('YYMM') == yyyymm_py and 'SALE_AMT' in r
        )
        change_pct = ((total_sales_cy - total_sales_py) / total_sales_py * 100) if total_sales_py > 0 else 0
        
        # 채널별 당해/전년 비교
        channel_comparison = {}
        for chnl_std in channel_summary:
            sales_cy = channel_summary[chnl_std]['months'].get(yyyymm, 0)
            sales_py = channel_summary[chnl_std]['months'].get(yyyymm_py, 0)
            change = ((sales_cy - sales_py) / sales_py * 100) if sales_py > 0 else 0
            channel_comparison[chnl_std] = {
                'sales_cy': sales_cy,
                'sales_py': sales_py,
                'change_pct': round(change, 1)
            }
        
        # 채널 순서대로 정렬
        channel_summary_sorted = sort_channels_by_order(channel_summary)
        channel_comparison_sorted = sort_channels_by_order(channel_comparison)
        sorted_channel_list = get_channel_list_sorted(channel_summary)
        
        # AI 분석 요청
        prompt = f"""
다음은 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 리테일 채널별 매출분석 데이터입니다.

**분석 기간**: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월

**전체 요약**:
- 총 매출액 (당해): {total_sales_cy/1000000:.2f}백만원
- 총 매출액 (전년): {total_sales_py/1000000:.2f}백만원
- 전년 대비 변화: {change_pct:.1f}%
- 채널 수: {unique_channels}개
- 아이템 수: {unique_items}개

**채널 분석 순서** (반드시 이 순서대로 분석하세요):
{', '.join(sorted_channel_list)}

**채널별 요약 데이터** (정렬된 순서):
{json_dumps_safe(dict(channel_summary_sorted), ensure_ascii=False, indent=2)}

**채널별 당해/전년 비교** (정렬된 순서):
{json_dumps_safe(dict(channel_comparison_sorted), ensure_ascii=False, indent=2)}

위 데이터를 바탕으로 다음을 분석해주세요:

1. **각 채널별 분석** (채널별로 DIV를 생성):
   - 각 채널의 매출 현황 및 전년 대비 변화율
   - 각 채널의 TOP 3 아이템 분석
   - 각 채널의 성과 요약 및 전략적 시사점

2. **종합분석** (종합분석-1, 종합분석-2, 종합분석-3으로 DIV 생성):
   - 종합분석-1: 최고 성과 채널 분석 (매출액, 성장률, 주요 성공 요인)
   - 종합분석-2: 개선 필요 채널 분석 (하락 채널, 원인 분석, 개선 방안)
   - 종합분석-3: 핵심 제안 (전체 채널 전략, 우선순위 액션플랜)

분석 결과는 다음 JSON 형식으로 제공해주세요:
{{
    "title": "리테일 채널별 매출분석",
    "sections": [
        {{
            "div": "채널명1",
            "sub_title": "채널명1 전년대비 주요 변화",
            "ai_text": "채널별 상세 분석 내용..."
        }},
        {{
            "div": "채널명2",
            "sub_title": "채널명2 전년대비 주요 변화",
            "ai_text": "채널별 상세 분석 내용..."
        }},
        ... (모든 채널에 대해 반복) ...
        {{
            "div": "종합분석-1",
            "sub_title": "최고 성과 채널",
            "ai_text": "최고 성과 채널 분석 내용..."
        }},
        {{
            "div": "종합분석-2",
            "sub_title": "개선 필요 채널",
            "ai_text": "개선 필요 채널 분석 내용..."
        }},
        {{
            "div": "종합분석-3",
            "sub_title": "핵심 제안",
            "ai_text": "핵심 제안 내용..."
        }}
    ]
}}

**중요**: 
- 각 채널별로 "div" 필드에 채널명을 정확히 입력하세요
- 채널별 분석은 반드시 위에 명시된 "채널 분석 순서"대로 작성하세요
- 종합분석은 반드시 "종합분석-1", "종합분석-2", "종합분석-3"으로 div 필드를 설정하세요
- 채널별 분석을 먼저 작성하고, 그 다음 종합분석을 작성하세요
"""
        
        ai_response = call_llm(prompt)
        
        # AI 응답 파싱 (JSON 코드 블록에서 추출)
        analysis_data = extract_json_from_response(ai_response)
        
        # 파싱 결과 검증 및 정리
        if analysis_data is None or not isinstance(analysis_data, dict):
            print(f"[WARNING] JSON 파싱 실패, 텍스트로 저장")
            analysis_data = {
                'title': '리테일 채널별 매출분석',
                'sections': [
                    {
                        'sub_title': '분석 결과',
                        'ai_text': ai_response
                    }
                ]
            }
        else:
            # sections 배열 검증
            if 'sections' not in analysis_data or not isinstance(analysis_data['sections'], list):
                print(f"[WARNING] sections 배열이 올바르지 않음, 재구성")
                analysis_data = {
                    'title': analysis_data.get('title', '리테일 채널별 매출분석'),
                    'sections': [
                        {
                            'sub_title': '분석 결과',
                            'ai_text': ai_response
                        }
                    ]
                }
            else:
                # sections의 각 항목이 올바른 구조인지 확인
                valid_sections = []
                for section in analysis_data['sections']:
                    if isinstance(section, dict) and 'ai_text' in section:
                        valid_sections.append(section)
                    else:
                        print(f"[WARNING] 잘못된 section 구조 발견, 건너뜀: {section}")
                
                if valid_sections:
                    analysis_data['sections'] = valid_sections
                    print(f"[OK] {len(valid_sections)}개 섹션이 올바르게 파싱됨")
                else:
                    print(f"[WARNING] 유효한 섹션이 없음, 텍스트로 저장")
                    analysis_data = {
                        'title': analysis_data.get('title', '리테일 채널별 매출분석'),
                        'sections': [
                            {
                                'sub_title': '분석 결과',
                                'ai_text': ai_response
                            }
                        ]
                    }
        
        # JSON 데이터 구성
        # channel_summary를 백만원 단위로 변환하고 정렬된 순서로 저장
        from collections import OrderedDict
        channel_summary_formatted = OrderedDict()
        for chnl_std, data in channel_summary_sorted.items():
            channel_summary_formatted[chnl_std] = {
                'total_sales': round(data['total_sales'] / 1000000, 2),
                'months': {
                    k: round(v / 1000000, 2) for k, v in data['months'].items()
                },
                'top_items': [
                    {
                        'class3': item['class3'],
                        'total_sales': round(item['total_sales'] / 1000000, 2)
                    }
                    for item in data['top_items']
                ]
            }
        
        json_data = {
            'country': 'CN',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '리테일',
            'sub_key': '채널별매출분석',
            'analysis_data': analysis_data,
            'summary': {
                'total_sales': round(total_sales / 1000000, 2),
                'total_sales_cy': round(total_sales_cy / 1000000, 2),
                'total_sales_py': round(total_sales_py / 1000000, 2),
                'change_pct': round(change_pct, 1),
                'unique_channels': unique_channels,
                'unique_items': unique_items,
                'unique_months': 2,
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'channel_summary': channel_summary_formatted,
            'raw_data': {
                'sample_records': [dict(r) for r in records[:50]],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]  # 202510 -> 2510
        filename = f"CN_{yyyymm_short}_{brd_cd}_리테일매출_채널별매출분석"
        save_json(json_data, filename)
        
        # Markdown도 저장 (채널 순서대로 정렬)
        markdown_content = f"# {analysis_data.get('title', '리테일 채널별 매출분석')}\n\n"
        
        # sections를 채널 순서대로 정렬
        sections = analysis_data.get('sections', [])
        
        # 채널별 sections와 종합분석 sections 분리
        channel_sections = []
        overall_sections = []
        other_sections = []
        
        for section in sections:
            div = section.get('div', '')
            if div.startswith('종합분석'):
                overall_sections.append(section)
            elif div in sorted_channel_list:
                channel_sections.append((sorted_channel_list.index(div), section))
            else:
                other_sections.append(section)
        
        # 채널별 sections를 순서대로 정렬
        channel_sections.sort(key=lambda x: x[0])
        
        # Markdown 생성: 채널별 → 종합분석 → 기타
        for _, section in channel_sections:
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        
        for section in overall_sections:
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        
        for section in other_sections:
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 리테일 채널별 매출분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_outbound_category_sales(yyyymm, brd_cd):
    """출고카테고리별 매출분석"""
    print(f"\n{'='*60}")
    print(f"출고카테고리별 매출분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (당해/전년 동월)
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # SQL 쿼리 실행
        sql = get_outbound_category_sales_query(yyyymm, yyyymm_py, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('SALE_AMT', 0)) for r in records if 'SALE_AMT' in r)
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)")
        
        # TODO: 데이터 가공 및 분석 로직 작성
        
        # AI 분석 요청
        prompt = f"""
다음은 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 출고카테고리별 매출분석 데이터입니다.

**분석 기간**: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월

**데이터 요약**:
- 총 매출액: {total_sales/1000000:.2f}백만원
- 총 레코드 수: {len(records)}개

**원본 데이터 샘플** (최대 50개):
{json_dumps_safe([dict(r) for r in records[:50]], ensure_ascii=False, indent=2)}

위 데이터를 바탕으로 다음을 분석해주세요:
1. 카테고리별 매출 현황 및 전년 대비 변화
2. 주요 카테고리의 성과 분석
3. 전략적 시사점 및 액션플랜

분석 결과는 다음 JSON 형식으로 제공해주세요:
{{
    "title": "출고카테고리별 매출분석",
    "sections": [
        {{
            "sub_title": "카테고리별 매출 현황",
            "ai_text": "분석 내용..."
        }},
        {{
            "sub_title": "전년 대비 주요 변화",
            "ai_text": "분석 내용..."
        }},
        {{
            "sub_title": "전략적 시사점",
            "ai_text": "분석 내용..."
        }}
    ]
}}
"""
        
        ai_response = call_llm(prompt)
        
        # AI 응답 파싱 (JSON 코드 블록에서 추출)
        analysis_data = extract_json_from_response(ai_response)
        
        if analysis_data is None:
            analysis_data = {
                'title': '출고카테고리별 매출분석',
                'sections': [
                    {
                        'sub_title': '분석 결과',
                        'ai_text': ai_response
                    }
                ]
            }
        
        # JSON 데이터 구성
        json_data = {
            'country': 'CN',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '출고',
            'sub_key': '카테고리별매출분석',
            'analysis_data': analysis_data,
            'summary': {
                'total_sales': round(total_sales / 1000000, 2),
                'total_records': len(records),
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'raw_data': {
                'sample_records': [dict(r) for r in records[:50]],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]  # 202510 -> 2510
        filename = f"CN_{yyyymm_short}_{brd_cd}_출고_카테고리별매출분석"
        save_json(json_data, filename)
        
        # Markdown도 저장
        markdown_content = f"# {analysis_data.get('title', '출고카테고리별 매출분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 출고카테고리별 매출분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_agent_store_sales(yyyymm, brd_cd):
    """대리상 점당매출 종합분석"""
    print(f"\n{'='*60}")
    print(f"대리상 점당매출 종합분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (당해/전년 동월)
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # SQL 쿼리 실행
        sql = get_agent_store_sales_query(yyyymm, yyyymm_py, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('SALE_AMT', 0)) for r in records if 'SALE_AMT' in r)
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)")
        
        # TODO: 데이터 가공 및 분석 로직 작성
        
        # AI 분석 요청
        prompt = f"""
다음은 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 대리상 점당매출 종합분석 데이터입니다.

**분석 기간**: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월

**데이터 요약**:
- 총 매출액: {total_sales/1000000:.2f}백만원
- 총 레코드 수: {len(records)}개

**원본 데이터 샘플** (최대 50개):
{json_dumps_safe([dict(r) for r in records[:50]], ensure_ascii=False, indent=2)}

위 데이터를 바탕으로 다음을 분석해주세요:
1. 대리상별 점당매출 현황 및 전년 대비 변화
2. 주요 대리상의 성과 분석
3. 전략적 시사점 및 액션플랜

분석 결과는 다음 JSON 형식으로 제공해주세요:
{{
    "title": "대리상 점당매출 종합분석",
    "sections": [
        {{
            "sub_title": "대리상별 점당매출 현황",
            "ai_text": "분석 내용..."
        }},
        {{
            "sub_title": "전년 대비 주요 변화",
            "ai_text": "분석 내용..."
        }},
        {{
            "sub_title": "전략적 시사점",
            "ai_text": "분석 내용..."
        }}
    ]
}}
"""
        
        ai_response = call_llm(prompt)
        
        # AI 응답 파싱 (JSON 코드 블록에서 추출)
        analysis_data = extract_json_from_response(ai_response)
        
        if analysis_data is None:
            analysis_data = {
                'title': '대리상 점당매출 종합분석',
                'sections': [
                    {
                        'sub_title': '분석 결과',
                        'ai_text': ai_response
                    }
                ]
            }
        
        # JSON 데이터 구성
        json_data = {
            'country': 'CN',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '대리상',
            'sub_key': '점당매출종합분석',
            'analysis_data': analysis_data,
            'summary': {
                'total_sales': round(total_sales / 1000000, 2),
                'total_records': len(records),
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'raw_data': {
                'sample_records': [dict(r) for r in records[:50]],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]  # 202510 -> 2510
        filename = f"CN_{yyyymm_short}_{brd_cd}_대리상_점당매출종합분석"
        save_json(json_data, filename)
        
        # Markdown도 저장
        markdown_content = f"# {analysis_data.get('title', '대리상 점당매출 종합분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 대리상 점당매출 종합분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_discount_rate(yyyymm, brd_cd):
    """할인율 종합분석"""
    print(f"\n{'='*60}")
    print(f"할인율 종합분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (당해/전년 동월)
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # SQL 쿼리 실행
        sql = get_discount_rate_query(yyyymm, yyyymm_py, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('SALE_AMT', 0)) for r in records if 'SALE_AMT' in r)
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)")
        
        # TODO: 데이터 가공 및 분석 로직 작성
        
        # AI 분석 요청
        prompt = f"""
다음은 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 할인율 종합분석 데이터입니다.

**분석 기간**: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월

**데이터 요약**:
- 총 매출액: {total_sales/1000000:.2f}백만원
- 총 레코드 수: {len(records)}개

**원본 데이터 샘플** (최대 50개):
{json_dumps_safe([dict(r) for r in records[:50]], ensure_ascii=False, indent=2)}

위 데이터를 바탕으로 다음을 분석해주세요:
1. 할인율 현황 및 전년 대비 변화
2. 채널/카테고리별 할인율 분석
3. 전략적 시사점 및 액션플랜

분석 결과는 다음 JSON 형식으로 제공해주세요:
{{
    "title": "할인율 종합분석",
    "sections": [
        {{
            "sub_title": "할인율 현황",
            "ai_text": "분석 내용..."
        }},
        {{
            "sub_title": "전년 대비 주요 변화",
            "ai_text": "분석 내용..."
        }},
        {{
            "sub_title": "전략적 시사점",
            "ai_text": "분석 내용..."
        }}
    ]
}}
"""
        
        ai_response = call_llm(prompt)
        
        # AI 응답 파싱 (JSON 코드 블록에서 추출)
        analysis_data = extract_json_from_response(ai_response)
        
        if analysis_data is None:
            analysis_data = {
                'title': '할인율 종합분석',
                'sections': [
                    {
                        'sub_title': '분석 결과',
                        'ai_text': ai_response
                    }
                ]
            }
        
        # JSON 데이터 구성
        json_data = {
            'country': 'CN',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '할인율',
            'sub_key': '종합분석',
            'analysis_data': analysis_data,
            'summary': {
                'total_sales': round(total_sales / 1000000, 2),
                'total_records': len(records),
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'raw_data': {
                'sample_records': [dict(r) for r in records[:50]],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]  # 202510 -> 2510
        filename = f"CN_{yyyymm_short}_{brd_cd}_할인율_종합분석"
        save_json(json_data, filename)
        
        # Markdown도 저장
        markdown_content = f"# {analysis_data.get('title', '할인율 종합분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 할인율 종합분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_operating_expense(yyyymm, brd_cd):
    """영업비 종합분석"""
    print(f"\n{'='*60}")
    print(f"영업비 종합분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (당해/전년 동월)
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # SQL 쿼리 실행
        sql = get_operating_expense_query(yyyymm, yyyymm_py, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_expense = sum(float(r.get('EXPENSE_AMT', 0)) for r in records if 'EXPENSE_AMT' in r)
        print(f"총 영업비: {total_expense:,.0f}원 ({total_expense/1000000:.2f}백만원)")
        
        # TODO: 데이터 가공 및 분석 로직 작성
        
        # AI 분석 요청
        prompt = f"""
다음은 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 영업비 종합분석 데이터입니다.

**분석 기간**: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월

**데이터 요약**:
- 총 영업비: {total_expense/1000000:.2f}백만원
- 총 레코드 수: {len(records)}개

**원본 데이터 샘플** (최대 50개):
{json_dumps_safe([dict(r) for r in records[:50]], ensure_ascii=False, indent=2)}

위 데이터를 바탕으로 다음을 분석해주세요:
1. 영업비 현황 및 전년 대비 변화
2. 계정별 영업비 분석
3. 전략적 시사점 및 액션플랜

분석 결과는 다음 JSON 형식으로 제공해주세요:
{{
    "title": "영업비 종합분석",
    "sections": [
        {{
            "sub_title": "영업비 현황",
            "ai_text": "분석 내용..."
        }},
        {{
            "sub_title": "전년 대비 주요 변화",
            "ai_text": "분석 내용..."
        }},
        {{
            "sub_title": "전략적 시사점",
            "ai_text": "분석 내용..."
        }}
    ]
}}
"""
        
        ai_response = call_llm(prompt)
        
        # AI 응답 파싱 (JSON 코드 블록에서 추출)
        analysis_data = extract_json_from_response(ai_response)
        
        if analysis_data is None:
            analysis_data = {
                'title': '영업비 종합분석',
                'sections': [
                    {
                        'sub_title': '분석 결과',
                        'ai_text': ai_response
                    }
                ]
            }
        
        # JSON 데이터 구성
        json_data = {
            'country': 'CN',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '영업비',
            'sub_key': '종합분석',
            'analysis_data': analysis_data,
            'summary': {
                'total_expense': round(total_expense / 1000000, 2),
                'total_records': len(records),
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'raw_data': {
                'sample_records': [dict(r) for r in records[:50]],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]  # 202510 -> 2510
        filename = f"CN_{yyyymm_short}_{brd_cd}_영업비_종합분석"
        save_json(json_data, filename)
        
        # Markdown도 저장
        markdown_content = f"# {analysis_data.get('title', '영업비 종합분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 영업비 종합분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

# ============================================================================
# 유틸리티 함수
# ============================================================================
def generate_yyyymm_list(start_yyyymm, end_yyyymm=None):
    """
    년월 리스트 생성
    
    Args:
        start_yyyymm: 시작 년월 (예: '202401')
        end_yyyymm: 종료 년월 (예: '202412'). None이면 start_yyyymm만 반환
    
    Returns:
        list: 년월 문자열 리스트 (예: ['202401', '202402', ...])
    """
    if end_yyyymm is None:
        return [start_yyyymm]
    
    start_date = datetime(int(start_yyyymm[:4]), int(start_yyyymm[4:6]), 1)
    end_date = datetime(int(end_yyyymm[:4]), int(end_yyyymm[4:6]), 1)
    
    yyyymm_list = []
    current_date = start_date
    
    while current_date <= end_date:
        yyyymm = current_date.strftime('%Y%m')
        yyyymm_list.append(yyyymm)
        
        # 다음 달로 이동
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)
    
    return yyyymm_list

# ============================================================================
# 메인 실행
# ============================================================================
if __name__ == '__main__':
    # 시작 시간 기록
    start_time = datetime.now()
    print(f"\n{'='*60}")
    print(f"분석 시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # 토큰 카운터 초기화
    reset_token_counter()
    
    # ========================================================================
    # 분석 기간 설정
    # ========================================================================
    # 방법 1: 한 달만 분석
    yyyymm_list = generate_yyyymm_list('202511')
    
    # 방법 2: 여러 달 분석 (2024년 1월 ~ 2025년 10월)
    # yyyymm_list = generate_yyyymm_list('202407', '202508')
    
    # 방법 3: 직접 리스트 지정
    # yyyymm_list = ['202509', '202510', '202511']
    
    if len(yyyymm_list) == 1:
        print(f"분석할 기간: {len(yyyymm_list)}개월 ({yyyymm_list[0]})")
    else:
        print(f"분석할 기간: {len(yyyymm_list)}개월 ({yyyymm_list[0]} ~ {yyyymm_list[-1]})")
    
    # 브랜드 선택 (원하는 브랜드만 주석 해제)
    brands_to_analyze = [
        'M',   # MLB
        # 'I',   # MLB KIDS
        # 'X',   # DISCOVERY
        # 'V',   # DUVETICA
        # 'ST',  # SERGIO TACCHINI
        # 'W',   # SUPRA
    ]
    
    # 기간별, 브랜드별 분석 실행
    for yyyymm in yyyymm_list:
        print(f"\n{'='*60}")
        print(f"기간 분석 시작: {yyyymm} ({yyyymm[:4]}년 {yyyymm[4:6]}월)")
        print(f"{'='*60}\n")
        
        for brd_cd in brands_to_analyze:
            print(f"\n{'='*60}")
            print(f"브랜드 분석 시작: {brd_cd} ({BRAND_CODE_MAP.get(brd_cd, brd_cd)})")
            print(f"{'='*60}\n")
            
            try:
                # 분석 실행 (원하는 분석만 주석 해제)
                analyze_retail_channel_sales(yyyymm, brd_cd)  # 리테일 채널별 매출분석
                # analyze_outbound_category_sales(yyyymm, brd_cd)  # 출고카테고리별 매출분석
                # analyze_agent_store_sales(yyyymm, brd_cd)  # 대리상 점당매출 종합분석
                # analyze_discount_rate(yyyymm, brd_cd)  # 할인율 종합분석
                # analyze_operating_expense(yyyymm, brd_cd)  # 영업비 종합분석
            except Exception as e:
                print(f"[ERROR] 브랜드 {brd_cd} 분석 중 오류 발생: {e}")
                print(f"[ERROR] 다음 브랜드로 계속 진행합니다...\n")
                continue
    
    # 종료 시간 기록
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    
    # 토큰 사용량 조회
    total_tokens = get_total_tokens()
    total_token_count = total_tokens['input'] + total_tokens['output']
    
    print(f"\n{'='*60}")
    print(f"전체 브랜드 분석 완료!")
    print(f"소요 시간: {elapsed_time}")
    print(f"총 토큰 사용량: {total_token_count:,} 토큰 (입력: {total_tokens['input']:,}, 출력: {total_tokens['output']:,})")
    print(f"{'='*60}\n")

