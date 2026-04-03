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


def format_channel_name(chnl_nm):
    """
    채널명에 (EC) 또는 (OFF) 접두사 추가
    
    Args:
        chnl_nm: 원본 채널명
    
    Returns:
        str: 포맷된 채널명
    """
    if not chnl_nm:
        return chnl_nm
    
    # 이미 접두사가 있는 경우 그대로 반환
    if chnl_nm.startswith('(EC)') or chnl_nm.startswith('(OFF)'):
        return chnl_nm
    
    # 매핑 딕셔너리에서 찾기
    if chnl_nm in CHANNEL_NAME_MAPPING:
        return CHANNEL_NAME_MAPPING[chnl_nm]
    
    # 부분 매칭 시도
    for key, value in CHANNEL_NAME_MAPPING.items():
        if key in chnl_nm:
            # 대리상의 경우 CHNL_CD로 구분 필요할 수 있음
            if '대리상' in chnl_nm:
                # 기본값은 (EC)대리상, 필요시 (OFF)대리상으로 변경 가능
                return '(EC)대리상'
            return value.replace(')', f'){chnl_nm}')
    
    # 매핑되지 않은 경우 원본 반환
    return chnl_nm

# ============================================================================
# DB 연결
# ============================================================================
def get_db_engine():
    """Snowflake DB 연결 엔진 생성"""
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    authenticator = os.getenv('SNOWFLAKE_AUTHENTICATOR')
    # password = os.getenv('SNOWFLAKE_PASSWORD')
    database = os.getenv('SNOWFLAKE_DATABASE')
    # schema = os.getenv('SNOWFLAKE_SCHEMA')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    # role = os.getenv('SNOWFLAKE_ROLE')
    
    # if not all([account, user, password, database, schema, warehouse, role]):
    if not all([account, user, database, warehouse, authenticator]):
        raise ValueError("Snowflake 환경 변수가 설정되지 않았습니다. .env 파일을 확인하세요.")
    
    return create_engine(
        URL(
            account=account,
            user=user,
            # password=password,
            authenticator=authenticator,
            database=database,
            # schema=schema,
            warehouse=warehouse,
            # role=role,
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
    
    print(f"[LLM] Claude API 호출 중... (timeout: 120초)")
    try:
        message = client.messages.create(
            model='claude-sonnet-4-20250514',
            max_tokens=max_tokens,
            temperature=temperature,
            messages=[{"role": "user", "content": full_prompt}],
            timeout=120.0
        )
    except Exception as e:
        print(f"[ERROR] LLM API 호출 실패: {e}")
        raise
    
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
# 공통 프롬프트 템플릿
# ============================================================================
def get_common_prompt_guidelines():
    """공통 프롬프트 가이드라인 텍스트 반환"""
    return """- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 천 단위(k)로 표시하고 절대 변형하지 말 것
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시 (예: "첫 번째 줄\\n두 번째 줄")
- ai_text 내에서 여러 문단이나 항목을 나눌 때는 \\n\\n을 사용
- 불릿 포인트나 리스트 항목 사이에는 \\n을 사용
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)"""

def get_common_json_requirement():
    """공통 JSON 요구사항 텍스트 반환"""
    return """아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘."""

def get_common_prompt_footer():
    """공통 프롬프트 푸터 텍스트 반환"""
    return """위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:"""

def parse_llm_json_response(response_text, default_title="분석 결과"):
    """
    LLM 응답에서 JSON을 파싱하는 공통 함수
    
    Args:
        response_text: LLM 응답 텍스트
        default_title: 파싱 실패 시 사용할 기본 제목
    
    Returns:
        dict: 파싱된 JSON 데이터
    """
    # JSON 파싱 (마크다운 코드 블록 제거)
    response_text = response_text.strip()
    if response_text.startswith('```json'):
        response_text = response_text[7:]
    if response_text.startswith('```'):
        response_text = response_text[3:]
    if response_text.endswith('```'):
        response_text = response_text[:-3]
    response_text = response_text.strip()
    
    try:
        analysis_data = json.loads(response_text)
    except json.JSONDecodeError as e:
        print(f"[WARNING] JSON 파싱 실패: {e}")
        print(f"[WARNING] 응답 내용: {response_text[:500]}")
        # 기본 구조로 대체
        analysis_data = {
            "title": default_title,
            "sections": [
                {"div": "종합분석-1", "sub_title": "분석 결과", "ai_text": response_text}
            ]
        }
    
    return analysis_data

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
    
    json_str = None
    
    # 1. JSON 코드 블록 찾기 (```json ... ```)
    json_block_pattern = r'```json\s*(.*?)\s*```'
    match = re.search(json_block_pattern, text, re.DOTALL)
    
    if match:
        json_str = match.group(1).strip()
        print(f"[DEBUG] JSON 코드 블록에서 추출: {len(json_str)}자")
    else:
        # 2. 코드 블록 없으면 ``` ... ``` 찾기
        code_block_pattern = r'```\s*(.*?)\s*```'
        match = re.search(code_block_pattern, text, re.DOTALL)
        if match:
            json_str = match.group(1).strip()
            # json 마커 제거
            if json_str.startswith('json'):
                json_str = json_str[4:].strip()
            print(f"[DEBUG] 코드 블록에서 추출: {len(json_str)}자")
        else:
            # 3. 코드 블록이 없으면 전체 텍스트에서 JSON 객체 찾기
            # 첫 번째 { 부터 마지막 } 까지 추출 (더 정확한 방법)
            start_idx = text.find('{')
            if start_idx >= 0:
                # 중괄호 균형을 맞춰서 끝 찾기
                brace_count = 0
                end_idx = start_idx
                for i in range(start_idx, len(text)):
                    if text[i] == '{':
                        brace_count += 1
                    elif text[i] == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            end_idx = i
                            break
                
                if end_idx > start_idx:
                    json_str = text[start_idx:end_idx+1]
                    print(f"[DEBUG] 텍스트에서 JSON 추출: {len(json_str)}자")
    
    if not json_str:
        print(f"[WARNING] JSON 문자열을 찾을 수 없음")
        return None
    
    # JSON 파싱 시도 (여러 방법으로)
    parsed = None
    
    # 방법 1: 직접 파싱
    try:
        parsed = json.loads(json_str)
        sections_count = len(parsed.get('sections', []))
        print(f"[OK] JSON 파싱 성공: {sections_count}개 섹션 추출")
        return parsed
    except json.JSONDecodeError as e:
        error_msg = str(e)
        error_pos = getattr(e, 'pos', None)
        print(f"[WARNING] JSON 파싱 실패 (방법1): {error_msg[:200]}")
        if error_pos:
            print(f"[DEBUG] 오류 위치: {error_pos}, 주변 텍스트: {json_str[max(0, error_pos-50):error_pos+50]}")
    
    # 방법 2: 이스케이프 문자 처리 후 파싱
    try:
        # 이스케이프된 따옴표 처리
        cleaned_json = json_str.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t')
        parsed = json.loads(cleaned_json)
        sections_count = len(parsed.get('sections', []))
        print(f"[OK] JSON 파싱 성공 (방법2 - 이스케이프 처리): {sections_count}개 섹션 추출")
        return parsed
    except (json.JSONDecodeError, Exception) as e:
        print(f"[DEBUG] 방법2 실패: {str(e)[:100]}")
    
    # 방법 3: 원본 텍스트에서 다시 추출
    try:
        if '```json' in text:
            json_block_pattern = r'```json\s*(.*?)\s*```'
            match = re.search(json_block_pattern, text, re.DOTALL)
            if match:
                raw_json = match.group(1).strip()
                print(f"[DEBUG] 원본 재추출: JSON 문자열 길이 {len(raw_json)}자")
                parsed = json.loads(raw_json)
                sections_count = len(parsed.get('sections', []))
                print(f"[OK] JSON 파싱 성공 (방법3 - 원본 재추출): {sections_count}개 섹션 추출")
                return parsed
    except json.JSONDecodeError as e1:
        error_pos1 = getattr(e1, 'pos', None)
        print(f"[DEBUG] 방법3 JSON 파싱 실패: {str(e1)[:200]}")
        if error_pos1 and 'raw_json' in locals():
            print(f"[DEBUG] 오류 위치: {error_pos1}, 주변:\n{raw_json[max(0, error_pos1-100):error_pos1+100]}")
    except Exception as e1:
        print(f"[DEBUG] 방법3 예외: {str(e1)[:100]}")
    
    # 방법 4: 중괄호 균형 맞춰서 추출 (더 정확한 방법)
    try:
        start_idx = text.find('{')
        if start_idx >= 0:
            brace_count = 0
            end_idx = start_idx
            in_string = False
            escape_next = False
            
            for i in range(start_idx, len(text)):
                char = text[i]
                
                if escape_next:
                    escape_next = False
                    continue
                
                if char == '\\':
                    escape_next = True
                    continue
                
                if char == '"' and not escape_next:
                    in_string = not in_string
                    continue
                
                if not in_string:
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            end_idx = i
                            break
            
            if end_idx > start_idx:
                extracted_json = text[start_idx:end_idx+1]
                # 코드 블록 마커 제거
                extracted_json = extracted_json.replace('```json', '').replace('```', '').strip()
                print(f"[DEBUG] 방법4: JSON 문자열 길이 {len(extracted_json)}자")
                parsed = json.loads(extracted_json)
                sections_count = len(parsed.get('sections', []))
                print(f"[OK] JSON 파싱 성공 (방법4 - 중괄호 균형): {sections_count}개 섹션 추출")
                return parsed
    except (json.JSONDecodeError, Exception) as e:
        print(f"[DEBUG] 방법4 실패: {str(e)[:100]}")
    
    print(f"[ERROR] 모든 JSON 파싱 방법 실패")
    print(f"[DEBUG] 추출된 JSON 문자열 앞 500자: {json_str[:500]}")
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
    
def get_outbound_category_sales_query(yyyymm, yyyymm_py, brd_cd):
    """
    출고카테고리별 매출분석 쿼리 (당해/전년 동월 비교)
    직영 매출 + 대리상 매출 통합
    
    Args:
        yyyymm: 당해 년월 (예: '202510')
        yyyymm_py: 전년 동월 (예: '202410')
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리 문자열
    """
    sql = f"""
WITH
    -- SHOP : BOS 매핑용 매장
    -- SAP 매장코드가 기준인 SAP_FNF.MST_SHOP에는 ERP 기준인 SHOP_CD 중복이 있을 수 있어 1건만 처리하는 로직 추가
    SHOP AS ( SELECT *
              FROM SAP_FNF.MST_SHOP
              QUALIFY
                  ROW_NUMBER() OVER ( PARTITION BY BRD_CD, CNTRY_CD, SHOP_CD, AGNT_CD, MAP_SHOP_AGNT_CD ORDER BY SAP_SHOP_CD ) =
                  1 )
    -- OR_SALE : 직영 매출
  , OR_SALE AS ( SELECT A.YYMM            AS YYYYMM
                      , A.BRD_CD          AS BRD_CD
                      , B.LARGE_CLASS_NM  AS LARGE_CLASS_NM
                      , B.MIDDLE_CLASS_NM AS MIDDLE_CLASS_NM
                      , B.ITEM_NM         AS ITEM_NM
                      , A.PRDT_CD         AS PRDT_CD
                      , SUM(A.SALE_AMT)   AS SALE_AMT
                 FROM CHN.DM_SH_S_M A
                     LEFT JOIN SAP_FNF.MST_PRDT B
                             ON A.PRDT_CD = B.PRDT_CD
                     LEFT JOIN SHOP C
                             ON A.MAP_SHOP_AGNT_CD = C.MAP_SHOP_AGNT_CD
                 WHERE C.CHNL_CD <> '84' -- 대리상 제외 (직영만)
                   AND A.YYMM IN ('{yyyymm}', '{yyyymm_py}')
                   AND A.BRD_CD = '{brd_cd}'
                 GROUP BY A.YYMM
                        , A.BRD_CD
                        , B.LARGE_CLASS_NM
                        , B.MIDDLE_CLASS_NM
                        , B.ITEM_NM
                        , A.PRDT_CD )
    -- FR_SALE : 대리상 매출
  , FR_SALE AS ( SELECT A.PST_YYYYMM        AS YYYYMM
                      , A.BRD_CD            AS BRD_CD
                      , B.LARGE_CLASS_NM    AS LARGE_CLASS_NM
                      , B.MIDDLE_CLASS_NM   AS MIDDLE_CLASS_NM
                      , B.ITEM_NM           AS ITEM_NM
                      , A.PRDT_CD           AS PRDT_CD
                      , SUM(A.ACT_SALE_AMT) AS SALE_AMT
                 FROM SAP_FNF.DM_CN_PL_SHOP_PRDT_M A
                     LEFT JOIN SAP_FNF.MST_PRDT B
                             ON A.PRDT_CD = B.PRDT_CD
                     LEFT JOIN SHOP C
                             ON A.SHOP_CD = C.SAP_SHOP_CD
                 WHERE C.CHNL_CD = '84' -- 대리상만
                   AND A.PST_YYYYMM IN ('{yyyymm}', '{yyyymm_py}')
                   AND A.BRD_CD = '{brd_cd}'
                 GROUP BY A.PST_YYYYMM
                        , A.BRD_CD
                        , B.LARGE_CLASS_NM
                        , B.MIDDLE_CLASS_NM
                        , B.ITEM_NM
                        , A.PRDT_CD )
-- 최종조회쿼리
SELECT A.YYYYMM, A.BRD_CD, A.LARGE_CLASS_NM, A.MIDDLE_CLASS_NM, A.ITEM_NM, A.PRDT_CD, B.PRDT_NM, SUM(A.SALE_AMT) AS SALE_AMT
FROM ( SELECT YYYYMM, BRD_CD, LARGE_CLASS_NM, MIDDLE_CLASS_NM, ITEM_NM, PRDT_CD, SALE_AMT
       FROM OR_SALE
       UNION ALL
       SELECT YYYYMM, BRD_CD, LARGE_CLASS_NM, MIDDLE_CLASS_NM, ITEM_NM, PRDT_CD, SALE_AMT
       FROM FR_SALE ) A
LEFT JOIN SAP_FNF.MST_PRDT B
		ON A.PRDT_CD = B.PRDT_CD
GROUP BY a.YYYYMM
       , a.BRD_CD
       , a.LARGE_CLASS_NM
       , a.MIDDLE_CLASS_NM
       , a.ITEM_NM
       , a.PRDT_CD
       , B.PRDT_NM
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
    할인율 종합분석 쿼리 (전년 1월부터 당해 월까지 추세 분석)
    
    Args:
        yyyymm: 당해 년월 (예: '202511')
        yyyymm_py: 전년 동월 (예: '202411')
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리 문자열
    """
    # 전년도 1월 계산 (추세 분석용: 전년도 1월 ~ 당해당월)
    current_year = int(yyyymm[:4])
    previous_year = current_year - 1
    start_yyyymm = f"{previous_year}01"  # 전년도 1월
    
    sql = f"""
WITH
    -- PARAM : 날짜조건
    PARAM AS ( SELECT 'CY' AS DIV, '{start_yyyymm}' AS STD_START_YYYYMM, '{yyyymm}' AS STD_END_YYYYMM
               )
    -- SHOP : BOS 매핑용 매장
    -- SAP 매장코드가 기준인 SAP_FNF.MST_SHOP에는 ERP 기준인 SHOP_CD 중복이 있을 수 있어 1건만 처리하는 로직 추가
    , SHOP AS ( SELECT *
              FROM SAP_FNF.MST_SHOP
              QUALIFY
                  ROW_NUMBER() OVER ( PARTITION BY BRD_CD, CNTRY_CD, SHOP_CD, AGNT_CD, MAP_SHOP_AGNT_CD ORDER BY SAP_SHOP_CD ) =
                  1 )
-- 최종조회쿼리
SELECT A.YYMM AS YYYYMM
     , A.BRD_CD AS BRD_CD
     , C.MGMT_CHNL_NM AS CHNL_NM
     , SUM(A.SALE_TAG_AMT) AS TAG_SALE_AMT
     , SUM(A.SALE_AMT) AS ACT_SALE_AMT
     , CASE 
         WHEN SUM(A.SALE_TAG_AMT) = 0 OR SUM(A.SALE_AMT) = 0 THEN 0 
         ELSE ROUND((1 - SUM(A.SALE_AMT) / SUM(A.SALE_TAG_AMT))*100, 1) 
       END AS DISCOUNT_PCT
FROM CHN.DM_SH_S_M A
    JOIN PARAM
            ON PARAM.DIV = 'CY'
                   AND A.YYMM BETWEEN PARAM.STD_START_YYYYMM AND PARAM.STD_END_YYYYMM
    LEFT JOIN SHOP C
            ON A.MAP_SHOP_AGNT_CD = C.MAP_SHOP_AGNT_CD
WHERE A.BRD_CD = '{brd_cd}' -- 브랜드조건 필터링 필요
GROUP BY A.YYMM
       , A.BRD_CD
       , C.MGMT_CHNL_NM
HAVING SUM(A.SALE_AMT) <> 0
ORDER BY A.YYMM DESC, A.BRD_CD, C.MGMT_CHNL_NM
    """
    return sql

def get_operating_expense_query(yyyymm, yyyymm_py, brd_cd):
    """
    영업비 종합분석 쿼리 (전년도 1월 ~ 당해당월)
    - 추세 분석: 전년도 1월 ~ 당해당월
    - 전년 누적: 전년도 1월 ~ 전년 동월
    - 당해 누적: 당해 1월 ~ 당해당월
    
    Args:
        yyyymm: 당해 년월 (예: '202511')
        yyyymm_py: 전년 동월 (예: '202411')
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리 문자열
    """
    # 전년도 1월 계산 (추세 분석용: 전년도 1월 ~ 당해당월)
    current_year = int(yyyymm[:4])
    previous_year = current_year - 1
    start_yyyymm = f"{previous_year}01"  # 전년도 1월
    
    sql = f"""
    SELECT PST_YYYYMM
         , BRD_CD
         , MGMT_CHNL_NM
         , SUM(
               CASE
                 WHEN MGMT_CHNL_CD IN ('CN7', 'CN8')
                   THEN ACT_SALE_AMT
                 ELSE ERP_ACT_SALE_AMT
               END
           ) AS SALE_AMT -- 출고매출
         , SUM(ERP_ACT_SALE_AMT/1.13)  AS SALE_AMT_VAT
         , sum(AD_CST_OPRT)       as ad_cst_oprt --광고비
         , sum(SLRY_CSY_OPRT)     as SLRY_CSY_OPRT --인건비
         , sum(EMP_BNFT_CST_OPRT) as EMP_BNFT_CST_OPRT --복리후생비
         , sum(PMT_CMS_OPRT)      as PMT_CMS_OPRT -- 지급수수료
         , sum(SHOP_RNT_OPRT)     as SHOP_RNT_OPRT --임차료
         , sum(EVNT_CST_OPRT)     as EVNT_CST_OPRT --수주회
         , sum(TAX_CST_OPRT)      as TAX_CST_OPRT --세금과공과
         , sum(DEPRC_CST_OPRT)    as DEPRC_CST_OPRT --감가상각비
         , sum(ETC_CST_OPRT)      as ETC_CST_OPRT --기타
    FROM SAP_FNF.VW_CN_PL_SHOP_M
    WHERE PST_YYYYMM BETWEEN '{start_yyyymm}' AND '{yyyymm}'
      AND BRD_CD = '{brd_cd}'
    GROUP BY PST_YYYYMM, BRD_CD, MGMT_CHNL_NM
    """
    return sql

def get_operating_expense_all_brands_query(yyyymm, yyyymm_py):
    """
    법인 전체 영업비 쿼리 (모든 브랜드 합계)
    - 추세 분석: 전년도 1월 ~ 당해당월
    - 전년 누적: 전년도 1월 ~ 전년 동월
    - 당해 누적: 당해 1월 ~ 당해당월
    
    Args:
        yyyymm: 당해 년월 (예: '202511')
        yyyymm_py: 전년 동월 (예: '202411')
    
    Returns:
        str: SQL 쿼리 문자열
    """
    # 전년도 1월 계산 (추세 분석용: 전년도 1월 ~ 당해당월)
    current_year = int(yyyymm[:4])
    previous_year = current_year - 1
    start_yyyymm = f"{previous_year}01"  # 전년도 1월
    
    # 법인 전체 브랜드 코드 리스트
    all_brand_codes = list(BRAND_CODE_MAP.keys())  # ['M', 'I', 'X', 'V', 'ST', 'W']
    brand_codes_str = "', '".join(all_brand_codes)
    
    sql = f"""
    SELECT PST_YYYYMM
         , MGMT_CHNL_NM
         , SUM(
               CASE
                 WHEN MGMT_CHNL_CD IN ('CN7', 'CN8')
                   THEN ACT_SALE_AMT
                 ELSE ERP_ACT_SALE_AMT
               END
           ) AS SALE_AMT -- 출고매출
         , SUM(ERP_ACT_SALE_AMT/1.13)  AS SALE_AMT_VAT
         , sum(AD_CST_OPRT)       as ad_cst_oprt --광고비
         , sum(SLRY_CSY_OPRT)     as SLRY_CSY_OPRT --인건비
         , sum(EMP_BNFT_CST_OPRT) as EMP_BNFT_CST_OPRT --복리후생비
         , sum(PMT_CMS_OPRT)      as PMT_CMS_OPRT -- 지급수수료
         , sum(SHOP_RNT_OPRT)     as SHOP_RNT_OPRT --임차료
         , sum(EVNT_CST_OPRT)     as EVNT_CST_OPRT --수주회
         , sum(TAX_CST_OPRT)      as TAX_CST_OPRT --세금과공과
         , sum(DEPRC_CST_OPRT)    as DEPRC_CST_OPRT --감가상각비
         , sum(ETC_CST_OPRT)      as ETC_CST_OPRT --기타
    FROM SAP_FNF.VW_CN_PL_SHOP_M
    WHERE PST_YYYYMM BETWEEN '{start_yyyymm}' AND '{yyyymm}'
      AND BRD_CD IN ('{brand_codes_str}')
    GROUP BY PST_YYYYMM, MGMT_CHNL_NM
    """
    return sql

# ============================================================================
# 분석 함수들
# ============================================================================

def analyze_retail_channel_top3_sales(yyyymm, brd_cd):
    """리테일 채널별 TOP3 분석 - 전년 VS 당해 채널별 매출이 높은 ITEM 분석"""
    print(f"\n{'='*60}")
    print(f"리테일 채널별 TOP3 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
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
        sql = f"""
WITH
-- SHOP : BOS 매핑용 매장
-- SAP 매장코드가 기준인 SAP_FNF.MST_SHOP에는 ERP 기준인 SHOP_CD 중복이 있을 수 있어 1건만 처리하는 로직 추가
SHOP AS (SELECT *
         FROM SAP_FNF.MST_SHOP
         QUALIFY
             ROW_NUMBER() OVER ( PARTITION BY BRD_CD, CNTRY_CD, SHOP_CD, AGNT_CD, MAP_SHOP_AGNT_CD ORDER BY SAP_SHOP_CD ) =
             1)
-- 최종조회쿼리
SELECT A.YYMM          AS YYYYMM
     , A.BRD_CD        AS BRD_CD
     , C.MGMT_CHNL_CD  as MGMT_CHNL_CD
     , C.MGMT_CHNL_NM  AS MGMT_CHNL_NM
     , B.ITEM_NM
     , SUM(A.SALE_AMT) AS SALE_AMT
FROM CHN.DM_SH_S_M A
         LEFT JOIN SAP_FNF.MST_PRDT B
                   ON A.PRDT_CD = B.PRDT_CD
         LEFT JOIN SHOP C
                   ON A.MAP_SHOP_AGNT_CD = C.MAP_SHOP_AGNT_CD
WHERE A.YYMM IN ('{yyyymm}', '{yyyymm_py}')
  AND A.BRD_CD = '{brd_cd}'
  AND ITEM_NM IS NOT NULL
  AND SALE_AMT <> 0
GROUP BY A.YYMM
       , A.BRD_CD
       , c.MGMT_CHNL_CD
       , c.MGMT_CHNL_NM
       , B.ITEM_NM
ORDER BY A.YYMM DESC, MGMT_CHNL_NM,ITEM_NM, SALE_AMT DESC
        """
        df = run_query(sql, engine)
        records = df.to_dicts() if df is not None else []
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('SALE_AMT', 0) or 0) for r in records)
        unique_channels = len(set(r.get('MGMT_CHNL_NM', '') for r in records if r.get('MGMT_CHNL_NM')))
        unique_items = len(set(r.get('ITEM_NM', '') for r in records if r.get('ITEM_NM')))
        unique_months = len(set(r.get('YYYYMM', '') for r in records if r.get('YYYYMM')))
        
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)")
        print(f"채널 수: {unique_channels}개")
        print(f"아이템 수: {unique_items}개")
        print(f"분석 월 수: {unique_months}개월")
        
        # 채널별 요약 데이터 생성
        channel_summary = {}
        for record in records:
            chnl_nm = record.get('MGMT_CHNL_NM', '기타') or '기타'
            month = record.get('YYYYMM', '')
            sale_amt = float(record.get('SALE_AMT', 0) or 0)
            
            if chnl_nm not in channel_summary:
                channel_summary[chnl_nm] = {
                    'total_sales': 0,
                    'months': {},
                    'top_items': []
                }
            
            channel_summary[chnl_nm]['total_sales'] += sale_amt
            
            if month not in channel_summary[chnl_nm]['months']:
                channel_summary[chnl_nm]['months'][month] = 0
            channel_summary[chnl_nm]['months'][month] += sale_amt
        
        # 채널별 상위 아이템 추출
        item_sales_by_channel = {}
        for record in records:
            chnl_nm = record.get('MGMT_CHNL_NM', '기타') or '기타'
            item_nm = record.get('ITEM_NM', '기타') or '기타'
            sale_amt = float(record.get('SALE_AMT', 0) or 0)
            
            key = f"{chnl_nm}|{item_nm}"
            if key not in item_sales_by_channel:
                item_sales_by_channel[key] = {
                    'chnl_nm': chnl_nm,
                    'item_nm': item_nm,
                    'total_sales': 0
                }
            item_sales_by_channel[key]['total_sales'] += sale_amt
        
        # 채널별로 상위 5개 아이템 추출
        for chnl_nm in channel_summary.keys():
            channel_items = [
                item for key, item in item_sales_by_channel.items()
                if item['chnl_nm'] == chnl_nm
            ]
            channel_items.sort(key=lambda x: x['total_sales'], reverse=True)
            channel_summary[chnl_nm]['top_items'] = [
                {
                    'item_nm': item['item_nm'],
                    'total_sales': round(item['total_sales'] / 1000000, 2)
                }
                for item in channel_items[:5]
            ]
            channel_summary[chnl_nm]['total_sales'] = round(
                channel_summary[chnl_nm]['total_sales'] / 1000000, 2
            )
        
        # 월별 합계 계산
        monthly_totals = {}
        for record in records:
            month = record.get('YYYYMM', '')
            sale_amt = float(record.get('SALE_AMT', 0) or 0)
            if month not in monthly_totals:
                monthly_totals[month] = 0
            monthly_totals[month] += sale_amt
        
        monthly_totals_list = [
            {'yyyymm': month, 'total_amount': round(amount / 1000000, 2)}
            for month, amount in sorted(monthly_totals.items())
        ]
        
        # 채널별로 당해/전년 데이터 존재 여부 확인
        channel_data_check = {}
        for record in records:
            chnl_nm = record.get('MGMT_CHNL_NM', '기타') or '기타'
            month = record.get('YYYYMM', '')
            
            if chnl_nm not in channel_data_check:
                channel_data_check[chnl_nm] = {
                    'has_current': False,
                    'has_previous': False
                }
            
            if month == yyyymm:
                channel_data_check[chnl_nm]['has_current'] = True
            elif month == yyyymm_py:
                channel_data_check[chnl_nm]['has_previous'] = True
        
        # 당해/전년 데이터가 모두 있는 채널만 필터링
        valid_channels = [
            chnl for chnl, check in channel_data_check.items()
            if check['has_current'] and check['has_previous']
        ]
        
        # 채널별 데이터 요약 (당해/전년 비교용)
        channel_comparison = {}
        for chnl_nm in valid_channels:
            current_data = [r for r in records if r.get('MGMT_CHNL_NM') == chnl_nm and r.get('YYYYMM') == yyyymm]
            previous_data = [r for r in records if r.get('MGMT_CHNL_NM') == chnl_nm and r.get('YYYYMM') == yyyymm_py]
            
            # 채널별 TOP 3 아이템 (당해 기준)
            current_items = sorted(current_data, key=lambda x: float(x.get('SALE_AMT', 0) or 0), reverse=True)[:3]
            
            # 채널 총 매출 계산
            current_total = sum(float(r.get('SALE_AMT', 0) or 0) for r in current_data)
            previous_total = sum(float(r.get('SALE_AMT', 0) or 0) for r in previous_data)
            
            channel_comparison[chnl_nm] = {
                'current_top3': [
                    {
                        'item_nm': item.get('ITEM_NM', ''),
                        'sale_amt': round(float(item.get('SALE_AMT', 0) or 0) / 1000000, 2)
                    }
                    for item in current_items
                ],
                'current_total': round(current_total / 1000000, 2),
                'previous_total': round(previous_total / 1000000, 2)
            }
        
        # LLM 프롬프트 생성 (JSON 형식 응답 요청)
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 채널 전략 전문가야. 각 채널별 당해 당월 매출 베스트 아이템 3개를 전년대비 주요변화로 분석해줘.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)
- 분석 가능한 채널 수: {len(valid_channels)}개
- 분석 채널 목록: {', '.join(valid_channels)}
- 분석 아이템 수: {unique_items}개

**채널별 데이터 요약**
{json_dumps_safe(channel_comparison, ensure_ascii=False, indent=2)}

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 각 채널별 당해 당월 매출 베스트 아이템 3개를 전년대비 주요변화로 분석해줘.

**중요**: 위 "채널별 데이터 요약"에 있는 채널만 분석하면 됩니다. 데이터가 없는 채널은 분석하지 마세요.

<데이터 샘플>
{json_dumps_safe(records[:200], ensure_ascii=False, indent=2)}

<요구사항>
{get_common_json_requirement()}

각 채널별로 하나의 섹션을 만들어야 합니다. 채널 목록: {', '.join(valid_channels)}

{{
  "title": "채널별 매출 top3 분석 (당해 전년 주요변화)",
  "sections": [
    {{
      "div": "{{채널명}}",
      "sub_title": "{{채널명}} 전년대비 주요 변화",
      "ai_text": "각 {{채널명}} 당해 당월 매출 베스트 아이템 3개를 한 줄씩 전년대비 주요변화로 분석해줘. 채널별 데이터 요약의 current_top3와 current_total, previous_total을 참고하여 구체적인 변화율과 원인을 분석해줘. (예: • PET: 당해 신규 모노그램 티셔츠 제품 +156.3% 폭증\\n • 다운: 클래식 모노그램 다운점퍼 폭발적 반응 +145.2%\\n • 후드 : 모노그램 후드 제품 폭발적 성장 +120.1% 등)"
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 당해 채널별 TOP 3 매출 아이템과 그중 어떤 제품이 판매율이 좋았는지
- 전년대비 주요 변화 분석
- 단기 전략 방향과 중장기 전략 방향을 구체적으로 시사

{get_common_prompt_footer()}
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱
        analysis_data = parse_llm_json_response(analysis_response, "채널별 매출 분석 (12개월 추이)")
        
        # JSON 데이터 생성
        json_data = {
            'country': 'CN',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '리테일',
            'sub_key': '채널별TOP3분석',
            'analysis_data': analysis_data,
            'summary': {
                'total_sales': round(total_sales / 1000000, 2),
                'unique_channels': unique_channels,
                'unique_items': unique_items,
                'unique_months': unique_months,
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'channel_summary': channel_summary,
            'raw_data': {
                'sample_records': [
                    {
                        'YYYYMM': r.get('YYYYMM', ''),
                        'MGMT_CHNL_NM': r.get('MGMT_CHNL_NM', ''),
                        'ITEM_NM': r.get('ITEM_NM', ''),
                        'SALE_AMT': float(r.get('SALE_AMT', 0) or 0)
                    }
                    for r in records[:50]
                ],
                'total_records_count': len(records)
            },
            'trend_data': {
                'trend_months': sorted(list(set(r.get('YYYYMM', '') for r in records if r.get('YYYYMM')))),
                'monthly_totals': monthly_totals_list,
                'monthly_details': [
                    {
                        'yyyymm': r.get('YYYYMM', ''),
                        'chnl_nm': r.get('MGMT_CHNL_NM', ''),
                        'item_nm': r.get('ITEM_NM', ''),
                        'sale_amt': round(float(r.get('SALE_AMT', 0) or 0) / 1000000, 2)
                    }
                    for r in records
                ]
            }
        }
        
        # ============================================================
        # 두 번째 분석: 브랜드별 채널 매출 종합분석 (OVERALL)
        # ============================================================
        print(f"\n{'='*60}")
        print(f"채널별 매출 종합분석 시작 (OVERALL): {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
        print(f"{'='*60}")
        
        # 데이터 요약 (두 번째 분석용)
        current_data = [r for r in records if r.get('YYYYMM') == yyyymm]
        previous_data = [r for r in records if r.get('YYYYMM') == yyyymm_py]
        
        total_sales_cy = sum(float(r.get('SALE_AMT', 0) or 0) for r in current_data)
        total_sales_py = sum(float(r.get('SALE_AMT', 0) or 0) for r in previous_data)
        
        print(f"전년 매출액: {total_sales_py:,.0f}원 ({total_sales_py/1000000:.2f}백만원)")
        print(f"당해 매출액: {total_sales_cy:,.0f}원 ({total_sales_cy/1000000:.2f}백만원)")
        
        # 채널별 요약 데이터 생성 (당해/전년 비교)
        channel_summary_overall = {}
        for record in records:
            chnl_nm = record.get('MGMT_CHNL_NM', '기타') or '기타'
            month = record.get('YYYYMM', '')
            sale_amt = float(record.get('SALE_AMT', 0) or 0)
            
            if chnl_nm not in channel_summary_overall:
                channel_summary_overall[chnl_nm] = {
                    'current_sales': 0,
                    'previous_sales': 0,
                    'all_items': []
                }
            
            if month == yyyymm:
                channel_summary_overall[chnl_nm]['current_sales'] += sale_amt
            elif month == yyyymm_py:
                channel_summary_overall[chnl_nm]['previous_sales'] += sale_amt
        
        # 채널별 상위 아이템 추출 (당해 기준)
        item_sales_by_channel_overall = {}
        for record in current_data:
            chnl_nm = record.get('MGMT_CHNL_NM', '기타') or '기타'
            item_nm = record.get('ITEM_NM', '기타') or '기타'
            sale_amt = float(record.get('SALE_AMT', 0) or 0)
            
            key = f"{chnl_nm}|{item_nm}"
            if key not in item_sales_by_channel_overall:
                item_sales_by_channel_overall[key] = {
                    'chnl_nm': chnl_nm,
                    'item_nm': item_nm,
                    'total_sales': 0
                }
            item_sales_by_channel_overall[key]['total_sales'] += sale_amt
        
        # 채널별로 전체 아이템 추출 (top3 제한 없음)
        for chnl_nm in channel_summary_overall.keys():
            channel_items = [
                item for key, item in item_sales_by_channel_overall.items()
                if item['chnl_nm'] == chnl_nm
            ]
            channel_items.sort(key=lambda x: x['total_sales'], reverse=True)
            # 모든 아이템 포함 (제한 없음)
            channel_summary_overall[chnl_nm]['all_items'] = [
                {
                    'item_nm': item['item_nm'],
                    'total_sales': round(item['total_sales'] / 1000000, 2)
                }
                for item in channel_items
            ]
            channel_summary_overall[chnl_nm]['current_sales'] = round(
                channel_summary_overall[chnl_nm]['current_sales'] / 1000000, 2
            )
            channel_summary_overall[chnl_nm]['previous_sales'] = round(
                channel_summary_overall[chnl_nm]['previous_sales'] / 1000000, 2
            )
            if channel_summary_overall[chnl_nm]['previous_sales'] > 0:
                channel_summary_overall[chnl_nm]['change_pct'] = round(
                    ((channel_summary_overall[chnl_nm]['current_sales'] - channel_summary_overall[chnl_nm]['previous_sales']) / channel_summary_overall[chnl_nm]['previous_sales'] * 100), 1
                )
            else:
                channel_summary_overall[chnl_nm]['change_pct'] = 0
        
        # 채널별로 당해/전년 데이터 존재 여부 확인
        channel_data_check_overall = {}
        for record in records:
            chnl_nm = record.get('MGMT_CHNL_NM', '기타') or '기타'
            month = record.get('YYYYMM', '')
            
            if chnl_nm not in channel_data_check_overall:
                channel_data_check_overall[chnl_nm] = {
                    'has_current': False,
                    'has_previous': False
                }
            
            if month == yyyymm:
                channel_data_check_overall[chnl_nm]['has_current'] = True
            elif month == yyyymm_py:
                channel_data_check_overall[chnl_nm]['has_previous'] = True
        
        # 당해/전년 데이터가 모두 있는 채널만 필터링
        valid_channels_overall = [
            channel for channel, check in channel_data_check_overall.items()
            if check['has_current'] and check['has_previous']
        ]
        
        # LLM 프롬프트 생성 (종합분석용)
        prompt_overall = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 채널 전략 전문가야. 브랜드 전체 채널을 종합적으로 분석하여 최고 성과 채널, 개선 필요 채널, 핵심 제안을 도출해줘.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 전년 매출액: {total_sales_py:,.0f}원 ({total_sales_py/1000000:.2f}백만원)
- 당해 매출액: {total_sales_cy:,.0f}원 ({total_sales_cy/1000000:.2f}백만원)
- 전년대비 변화: {round(((total_sales_cy - total_sales_py) / total_sales_py * 100) if total_sales_py != 0 else 0, 1)}%
- 분석 채널 수: {len(valid_channels_overall)}개
- 분석 채널 목록: {', '.join(valid_channels_overall)}
- 분석 아이템 수: {unique_items}개

**채널별 전체 데이터**
{json_dumps_safe({k: v for k, v in channel_summary_overall.items() if k in valid_channels_overall}, ensure_ascii=False, indent=2)}

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 모든 채널을 종합적으로 분석하여:
1. 최고 성과 채널: 매출 규모, 성장률, 전년대비 개선도 등을 종합하여 최고 성과를 보인 채널들을 식별
2. 개선 필요 채널: 매출 하락, 성장 둔화, 전년대비 악화 등이 있는 채널들을 식별하고 개선 방향 제시
3. 핵심 제안: 브랜드 전체 채널 포트폴리오 관점에서 즉시 실행 가능한 전략적 제안

<데이터 샘플>
{json_dumps_safe(records[:200], ensure_ascii=False, indent=2)}

<요구사항>
{get_common_json_requirement()}

{{
  "title": "브랜드별 채널 매출 종합분석",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "최고 성과 채널",
      "ai_text": "최고 성과를 보인 채널들을 종합 분석 (최대 2줄)"
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "개선 필요 채널",
      "ai_text": "개선이 필요한 채널들을 종합 분석 (최대 2줄)"
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "핵심 제안",
      "ai_text": "브랜드 전체 채널 전략에 대한 핵심 제안 (최대 2줄)"
    }}
  ]
}}

<작성 가이드라인>
- 각 섹션의 ai_text는 최대 2줄을 넘지 않도록 간결하게 작성
{get_common_prompt_guidelines()}
- 모든 채널의 데이터를 종합적으로 분석 (특정 채널만이 아닌 전체 관점)
- 채널별 top3가 아니라 전체 채널을 종합적으로 분석
- 구체적인 채널명과 수치를 포함하여 실용적인 내용으로 작성

{get_common_prompt_footer()}
"""
        
        # LLM 호출 (종합분석용)
        analysis_response_overall = call_llm(prompt_overall, max_tokens=4000)
        
        # JSON 파싱
        analysis_data_overall = parse_llm_json_response(analysis_response_overall, "브랜드별 채널 매출 종합분석")
        # 기본 구조 보완
        if len(analysis_data_overall.get('sections', [])) < 3:
            analysis_data_overall['sections'].extend([
                {"div": "종합분석-2", "sub_title": "개선 필요 채널", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "핵심 제안", "ai_text": ""}
            ])
        
        # ============================================================
        # 채널별 섹션과 종합분석을 하나로 통합
        # ============================================================
        
        # 종합분석 섹션을 채널별 섹션 뒤에 추가
        # 종합분석의 div를 "종합분석-1", "종합분석-2" 형태로 변경
        overall_sections = []
        for idx, section in enumerate(analysis_data_overall.get('sections', []), 1):
            overall_sections.append({
                'div': f'종합분석-{idx}',
                'sub_title': section.get('sub_title', ''),
                'ai_text': section.get('ai_text', '')
            })
        
        # 채널별 섹션 + 종합분석 섹션 통합
        combined_sections = analysis_data.get('sections', []) + overall_sections
        analysis_data_combined = {
            'title': analysis_data.get('title', '채널별 매출 top3 분석 (당해 전년 주요변화)'),
            'sections': combined_sections
        }
        
        # 통합된 JSON 데이터 생성
        json_data_combined = {
            'country': 'CN',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '리테일',
            'sub_key': '채널별매출분석',
            'analysis_data': analysis_data_combined,
            'summary': {
                'total_sales': round(total_sales / 1000000, 2),
                'total_sales_cy': round(total_sales_cy / 1000000, 2),
                'total_sales_py': round(total_sales_py / 1000000, 2),
                'change_pct': round(((total_sales_cy - total_sales_py) / total_sales_py * 100) if total_sales_py != 0 else 0, 1),
                'unique_channels': unique_channels,
                'unique_items': unique_items,
                'unique_months': unique_months,
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'channel_summary': channel_summary,
            'channel_summary_overall': channel_summary_overall,
            'raw_data': {
                'sample_records': [
                    {
                        'YYYYMM': r.get('YYYYMM', ''),
                        'MGMT_CHNL_NM': r.get('MGMT_CHNL_NM', ''),
                        'ITEM_NM': r.get('ITEM_NM', ''),
                        'SALE_AMT': float(r.get('SALE_AMT', 0) or 0)
                    }
                    for r in records[:50]
                ],
                'total_records_count': len(records)
            },
            'trend_data': {
                'trend_months': sorted(list(set(r.get('YYYYMM', '') for r in records if r.get('YYYYMM')))),
                'monthly_totals': monthly_totals_list,
                'monthly_details': [
                    {
                        'yyyymm': r.get('YYYYMM', ''),
                        'chnl_nm': r.get('MGMT_CHNL_NM', ''),
                        'item_nm': r.get('ITEM_NM', ''),
                        'sale_amt': round(float(r.get('SALE_AMT', 0) or 0) / 1000000, 2)
                    }
                    for r in records
                ]
            }
        }
        
        # 파일 저장 (통합된 결과)
        yyyymm_short = yyyymm[2:]  # 202510 -> 2510
        filename = f"CN_{yyyymm_short}_{brd_cd}_리테일매출_채널별매출분석"
        save_json(json_data_combined, filename)
        
        # Markdown도 저장 (통합된 sections를 조합)
        markdown_content = f"# {analysis_data_combined.get('title', '채널별 매출 분석')}\n\n"
        for section in analysis_data_combined.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 채널별 TOP3 분석 및 종합분석 완료!\n")
        return json_data_combined
        
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
        
        # 데이터 요약 및 가공
        total_sales = sum(float(r.get('SALE_AMT', 0) or 0) for r in records)
        
        # 당해/전년 데이터 분리
        current_data = [r for r in records if r.get('YYYYMM') == yyyymm]
        previous_data = [r for r in records if r.get('YYYYMM') == yyyymm_py]
        
        total_sales_cy = sum(float(r.get('SALE_AMT', 0) or 0) for r in current_data)
        total_sales_py = sum(float(r.get('SALE_AMT', 0) or 0) for r in previous_data)
        
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000:.0f}k)")
        print(f"당해 매출: {total_sales_cy:,.0f}원 ({total_sales_cy/1000:.0f}k)")
        print(f"전년 매출: {total_sales_py:,.0f}원 ({total_sales_py/1000:.0f}k)")
        
        # 카테고리별 데이터 집계 (LARGE_CLASS_NM 기준: ACC, 의류)
        category_data = {}
        for r in records:
            large_class = r.get('LARGE_CLASS_NM', '기타')
            yyyymm_val = r.get('YYYYMM', '')
            item_nm = r.get('ITEM_NM', '기타')
            prdt_cd = r.get('PRDT_CD', '')
            prdt_nm = r.get('PRDT_NM', '')
            sale_amt = float(r.get('SALE_AMT', 0) or 0)
            
            if large_class not in category_data:
                category_data[large_class] = {
                    'current': {'total': 0, 'items': {}},
                    'previous': {'total': 0, 'items': {}}
                }
            
            if yyyymm_val == yyyymm:
                category_data[large_class]['current']['total'] += sale_amt
                item_key = f"{item_nm}|{prdt_cd}"
                if item_key not in category_data[large_class]['current']['items']:
                    category_data[large_class]['current']['items'][item_key] = {
                        'item_nm': item_nm,
                        'prdt_cd': prdt_cd,
                        'prdt_nm': prdt_nm,
                        'sale_amt': 0
                    }
                category_data[large_class]['current']['items'][item_key]['sale_amt'] += sale_amt
            elif yyyymm_val == yyyymm_py:
                category_data[large_class]['previous']['total'] += sale_amt
                item_key = f"{item_nm}|{prdt_cd}"
                if item_key not in category_data[large_class]['previous']['items']:
                    category_data[large_class]['previous']['items'][item_key] = {
                        'item_nm': item_nm,
                        'prdt_cd': prdt_cd,
                        'prdt_nm': prdt_nm,
                        'sale_amt': 0
                    }
                category_data[large_class]['previous']['items'][item_key]['sale_amt'] += sale_amt
        
        # 카테고리별 강세/약세 아이템 분석
        category_analysis = {}
        for large_class, data in category_data.items():
            current_items = data['current']['items']
            previous_items = data['previous']['items']
            
            # 강세 아이템 (당해에만 있거나 전년 대비 증가)
            strong_items = []
            weak_items = []
            
            for item_key, item_data in current_items.items():
                current_amt = item_data['sale_amt']
                previous_amt = previous_items.get(item_key, {}).get('sale_amt', 0)
                
                if previous_amt == 0:
                    # 신규 아이템
                    strong_items.append({
                        'item_nm': item_data['item_nm'],
                        'prdt_nm': item_data['prdt_nm'],
                        'current_sale': round(current_amt / 1000, 0),
                        'previous_sale': 0,
                        'change_pct': 999.9,
                        'type': '신규'
                    })
                else:
                    change_pct = ((current_amt - previous_amt) / previous_amt * 100) if previous_amt > 0 else 0
                    item_info = {
                        'item_nm': item_data['item_nm'],
                        'prdt_nm': item_data['prdt_nm'],
                        'current_sale': round(current_amt / 1000, 0),
                        'previous_sale': round(previous_amt / 1000, 0),
                        'change_pct': round(change_pct, 1),
                        'type': '기존'
                    }
                    
                    if change_pct > 0:
                        strong_items.append(item_info)
                    elif change_pct < -20:  # 20% 이상 감소
                        weak_items.append(item_info)
            
            # 전년에만 있던 아이템 (단종/판매 중단)
            for item_key, item_data in previous_items.items():
                if item_key not in current_items:
                    weak_items.append({
                        'item_nm': item_data['item_nm'],
                        'prdt_nm': item_data['prdt_nm'],
                        'current_sale': 0,
                        'previous_sale': round(item_data['sale_amt'] / 1000, 0),
                        'change_pct': -100.0,
                        'type': '단종'
                    })
            
            # 정렬
            strong_items.sort(key=lambda x: x['current_sale'], reverse=True)
            weak_items.sort(key=lambda x: abs(x['change_pct']), reverse=True)
            
            category_analysis[large_class] = {
                'current_total': round(data['current']['total'] / 1000, 0),
                'previous_total': round(data['previous']['total'] / 1000, 0),
                'change_pct': round(((data['current']['total'] - data['previous']['total']) / data['previous']['total'] * 100) if data['previous']['total'] > 0 else 0, 1),
                'strong_items': strong_items[:10],  # 상위 10개
                'weak_items': weak_items[:10]  # 상위 10개
            }
        
        # AI 분석 요청
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 상품 기획 전문가야. 출고카테고리별 매출분석을 수행해줘.

**분석 기간**: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월 ({yyyymm_py} VS {yyyymm})

**전체 요약** (모든 금액은 k 단위):
- 당해 총 매출: {round(total_sales_cy / 1000, 0):,.0f}k
- 전년 총 매출: {round(total_sales_py / 1000, 0):,.0f}k
- 변화율: {round(((total_sales_cy - total_sales_py) / total_sales_py * 100) if total_sales_py > 0 else 0, 1):.1f}%

**카테고리별 분석 데이터** (모든 금액은 k 단위):
{json_dumps_safe(category_analysis, ensure_ascii=False, indent=2)}

<분석 목표>
1. ACC/의류 각 카테고리별로 강세 아이템과 약세 아이템을 분석
2. 전체(ACC/의류) 관점에서 당해/전년 어떠한 변화가 있는지 분석
3. 리스크 요소를 파악하고 종합 인사이트 도출

<요구사항>
{get_common_json_requirement()}

{{
  "title": "카테고리별 수익성 분석 (당해 전년 주요변화)",
  "sections": [
    {{
      "div": "ACC",
      "sub_title": "ACC 전년대비 주요 변화",
      "ai_text": "ACC 카테고리의 매출 성장/감소, 수익성, TOP3 제품 성과, 전략적 시사점, 단기/중장기 전략 방향을 분석한 내용. 강세 아이템과 약세 아이템을 구체적으로 언급하고, 숫자는 k 단위로 표시."
    }},
    {{
      "div": "의류",
      "sub_title": "의류 전년대비 주요 변화",
      "ai_text": "의류 카테고리의 매출 성장/감소, 수익성, TOP3 제품 성과, 전략적 시사점, 단기/중장기 전략 방향을 분석한 내용. 강세 아이템과 약세 아이템을 구체적으로 언급하고, 숫자는 k 단위로 표시."
    }},
    {{
      "div": "종합분석-1",
      "sub_title": "카테고리별 수익성 종합 평가",
      "ai_text": "전체(ACC/의류) 관점에서 당해/전년 변화를 종합적으로 평가한 내용. 전체 매출 구조, 카테고리별 기여도, 수익성 구조 등을 분석."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "성장 카테고리 및 기회",
      "ai_text": "성장하는 카테고리와 향후 기회를 분석한 내용. 강세 아이템들이 속한 카테고리와 성장 동력을 분석."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "주의 필요 카테고리",
      "ai_text": "주의가 필요한 카테고리와 약세 아이템들이 속한 카테고리를 분석한 내용. 리스크 요소와 개선 방향을 제시."
    }},
    {{
      "div": "종합분석-4",
      "sub_title": "이상징후 및 리스크 감지",
      "ai_text": "이상징후와 리스크 요소를 감지하고 분석한 내용. 데이터 이상, 매출 구조의 문제점, 잠재적 리스크 등을 분석."
    }},
    {{
      "div": "종합분석-5",
      "sub_title": "카테고리별 전략 최적화 방안",
      "ai_text": "카테고리별 전략 최적화 방안을 제시한 내용. 즉시 실행 방안과 중장기 전략 방향을 구체적으로 제시."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 강세 아이템과 약세 아이템을 구체적으로 언급
- 전체 관점에서의 변화와 리스크를 명확히 분석

{get_common_prompt_footer()}
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
            'key': '출고매출',
            'sub_key': '카테고리별매출분석',
            'analysis_data': analysis_data,
            'summary': {
                'total_sales_cy': round(total_sales_cy / 1000, 0),
                'total_sales_py': round(total_sales_py / 1000, 0),
                'change_pct': round(((total_sales_cy - total_sales_py) / total_sales_py * 100) if total_sales_py > 0 else 0, 1),
                'total_records': len(records),
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'category_analysis': category_analysis,
            'raw_data': {
                'sample_records': [dict(r) for r in records[:50]],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]  # 202510 -> 2510
        filename = f"CN_{yyyymm_short}_{brd_cd}_출고매출_카테고리별매출분석"
        save_json(json_data, filename)
        
        # Markdown도 저장
        markdown_content = f"# {analysis_data.get('title', '출고매출 카테고리별 매출분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 출고매출 카테고리별 매출분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_agent_store_sales(yyyymm, brd_cd):
    """오프라인 대리상 점당매출 종합분석"""
    print(f"\n{'='*60}")
    print(f"오프라인 대리상 점당매출 종합분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (당해 1월~지정한 연월, 전년 1월~전년 동일 월)
        analysis_year = int(yyyymm[:4])
        analysis_month = int(yyyymm[4:6])
        yyyymm_start = f"{analysis_year}01"  # 당해 1월
        yyyymm_end = yyyymm  # 함수 파라미터로 지정한 연월
        
        previous_year = analysis_year - 1
        yyyymm_py_start = f"{previous_year}01"  # 전년 1월
        yyyymm_py_end = f"{previous_year:04d}{analysis_month:02d}"  # 전년 동일 월
        
        print(f"분석 기간: {analysis_year}년 1월 ~ {analysis_year}년 {analysis_month}월 (당해) vs {previous_year}년 1월 ~ {previous_year}년 {analysis_month}월 (전년)")
        
        # SQL 쿼리 실행
        sql = f"""
WITH
    -- PARAM : 기간설정
    PARAM AS ( SELECT 'CY' AS DIV, '{yyyymm_start}' AS STD_START_YYYYMM, '{yyyymm_end}' AS STD_END_YYYYMM -- START, END 기준년월 지정 필요
               UNION ALL
               SELECT 'PY' AS DIV, '{yyyymm_py_start}' AS STD_START_YYYYMM, '{yyyymm_py_end}' AS STD_END_YYYYMM -- 전년 START, END 기준년월 지정 필요
    )
    -- SHOP : BOS 매핑용 매장
    -- SAP 매장코드가 기준인 SAP_FNF.MST_SHOP에는 ERP 기준인 SHOP_CD 중복이 있을 수 있어 1건만 처리하는 로직 추가
  , SHOP AS ( SELECT *
              FROM SAP_FNF.MST_SHOP
              QUALIFY
                  ROW_NUMBER() OVER ( PARTITION BY BRD_CD, CNTRY_CD, SHOP_CD, AGNT_CD, MAP_SHOP_AGNT_CD ORDER BY SAP_SHOP_CD ) =
                  1 )
    -- CY_FR_OFF : 대리상 OFF (당해)
  , CY_FR_OFF AS ( SELECT A.YYMM          AS YYYYMM
                        , A.BRD_CD        AS BRD_CD
                        , A.SHOP_ID as shop_cd
                        , C.SHOP_NM_EN as shop_en_nm
                        , SUM(A.SALE_AMT) AS SALE_AMT
                   FROM CHN.DM_SH_S_M A
                       JOIN PARAM
                               ON PARAM.DIV = 'CY' AND A.YYMM BETWEEN PARAM.STD_START_YYYYMM AND PARAM.STD_END_YYYYMM
                       JOIN CHN.DW_SHOP_WH_DETAIL B
                               ON A.SHOP_ID = B.OA_MAP_SHOP_ID AND B.FR_OR_CLS = 'FR' -- 대리상
                       JOIN CHN.MST_SHOP_ALL C
                               ON B.SHOP_ID = C.SHOP_ID
                   WHERE 1 = 1
                     AND B.BRD_CD = '{brd_cd}'             -- 브랜드필터링 필요
                     AND C.ANLYS_ONOFF_CLS_CD = '1' -- OFFLINE
                     AND B.ANLYS_SHOP_TYPE_NM IN ( 'FP', 'FO' )
                   GROUP BY A.YYMM
                          , A.BRD_CD
                          , A.SHOP_ID
                          , C.SHOP_NM_EN )
    -- PY_FR_OFF : 대리상 OFF (전년)
  , PY_FR_OFF AS ( SELECT A.YYMM                                                                      AS YYYYMM
                        , TO_VARCHAR(ADD_MONTHS(TO_DATE(A.YYMM || '01', 'YYYYMMDD'), 12), 'YYYYMM') AS NEXT_1Y_YYYYMM
                        , A.BRD_CD                                                                    AS BRD_CD
                        , A.SHOP_ID as shop_cd
                        , C.SHOP_NM_EN as shop_en_nm
                        , SUM(A.SALE_AMT)                                                             AS SALE_AMT
                   FROM CHN.DM_SH_S_M A
                       JOIN PARAM
                               ON PARAM.DIV = 'PY' AND A.YYMM BETWEEN PARAM.STD_START_YYYYMM AND PARAM.STD_END_YYYYMM
                       JOIN CHN.DW_SHOP_WH_DETAIL B
                               ON A.SHOP_ID = B.OA_MAP_SHOP_ID AND B.FR_OR_CLS = 'FR' -- 대리상
                       JOIN CHN.MST_SHOP_ALL C
                               ON B.SHOP_ID = C.SHOP_ID
                   WHERE 1 = 1
                     AND B.BRD_CD = '{brd_cd}'             -- 브랜드필터링 필요
                     AND C.ANLYS_ONOFF_CLS_CD = '1' -- OFFLINE
                     AND B.ANLYS_SHOP_TYPE_NM IN ( 'FP', 'FO' )
                   GROUP BY A.YYMM
                          , A.BRD_CD
                          , A.SHOP_ID
                          , C.SHOP_NM_EN )
SELECT C.YYYYMM
     , C.BRD_CD
     , C.SHOP_CD
     , C.SHOP_EN_NM
     , coalesce(sum(C.SALE_AMT),0) AS CY_SALE_AMT -- 당해 매출액
     , coalesce(sum(P.SALE_AMT),0) AS PY_SALE_AMT -- 전년 매출액
     , case when CY_SALE_AMT <> 0 and PY_SALE_AMT =0 then '신규점'
            when CY_SALE_AMT <> 0 and PY_SALE_AMT <>0 then '기존점'
            else '미지정' end as div  -- 혹시몰라서..
FROM CY_FR_OFF C
    LEFT JOIN PY_FR_OFF P
            ON C.YYYYMM = P.NEXT_1Y_YYYYMM AND C.BRD_CD = P.BRD_CD AND C.SHOP_CD = P.SHOP_CD
GROUP BY C.YYYYMM
       , C.BRD_CD
       , C.SHOP_CD
       , C.SHOP_EN_NM
having CY_SALE_AMT <> 0
ORDER BY C.YYYYMM DESC
        """
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 가공: 대리상별 집계 (월별 합계)
        agent_data = {}  # shop_cd -> {shop_en_nm, months: {yyyymm: {cy, py}}, total_cy, total_py}
        
        for r in records:
            shop_cd = r.get('SHOP_CD', '')
            shop_en_nm = r.get('SHOP_EN_NM', '')
            yyyymm_val = r.get('YYYYMM', '')
            cy_sale_amt = float(r.get('CY_SALE_AMT', 0) or 0)
            py_sale_amt = float(r.get('PY_SALE_AMT', 0) or 0)
            
            if shop_cd not in agent_data:
                agent_data[shop_cd] = {
                    'shop_en_nm': shop_en_nm,
                    'months': {},
                    'total_cy': 0,
                    'total_py': 0
                }
            
            if yyyymm_val not in agent_data[shop_cd]['months']:
                agent_data[shop_cd]['months'][yyyymm_val] = {'cy': 0, 'py': 0}
            
            agent_data[shop_cd]['months'][yyyymm_val]['cy'] += cy_sale_amt
            agent_data[shop_cd]['months'][yyyymm_val]['py'] += py_sale_amt
            agent_data[shop_cd]['total_cy'] += cy_sale_amt
            agent_data[shop_cd]['total_py'] += py_sale_amt
        
        # 대리상별 데이터 정리 (k 단위)
        agent_summary = []
        for shop_cd, data in agent_data.items():
            months_k = {}
            for yyyymm, amounts in sorted(data['months'].items()):
                months_k[yyyymm] = {
                    'cy': round(amounts['cy'] / 1000, 0),
                    'py': round(amounts['py'] / 1000, 0),
                    'change_pct': round(((amounts['cy'] - amounts['py']) / amounts['py'] * 100) if amounts['py'] != 0 else 0, 1)
                }
            
            agent_summary.append({
                'shop_cd': shop_cd,
                'shop_en_nm': data['shop_en_nm'],
                'total_cy': round(data['total_cy'] / 1000, 0),
                'total_py': round(data['total_py'] / 1000, 0),
                'total_change_pct': round(((data['total_cy'] - data['total_py']) / data['total_py'] * 100) if data['total_py'] != 0 else 0, 1),
                'months': months_k
            })
        
        # 총 매출 계산
        total_cy = sum(float(r.get('CY_SALE_AMT', 0) or 0) for r in records)
        total_py = sum(float(r.get('PY_SALE_AMT', 0) or 0) for r in records)
        
        # 대리상별 정렬 (당해 총 매출 기준)
        agent_summary_sorted = sorted(agent_summary, key=lambda x: x['total_cy'], reverse=True)
        
        print(f"총 매출액 (당해): {total_cy:,.0f}원 ({total_cy/1000:.0f}k)")
        print(f"총 매출액 (전년): {total_py:,.0f}원 ({total_py/1000:.0f}k)")
        print(f"대리상 수: {len(agent_summary_sorted)}개")
        
        # LLM 분석 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 오프라인 대리상 점당매출 분석 전문가야. 월별 대리상별 매출 추세 분석을 수행해줘.

**분석 기간**: {analysis_year}년 1월 ~ {analysis_year}년 11월 (당해) vs {previous_year}년 1월 ~ {previous_year}년 11월 (전년)

**총 매출 요약** (모든 금액은 k 단위):
- 당해 총 매출: {round(total_cy / 1000, 0):,}k
- 전년 총 매출: {round(total_py / 1000, 0):,}k
- 전년 대비 변화: {round(((total_cy - total_py) / total_py * 100) if total_py != 0 else 0, 1):.1f}%

**대리상별 매출 데이터** (모든 금액은 k 단위, 당해 총 매출 기준 내림차순):
{json_dumps_safe(agent_summary_sorted[:30], ensure_ascii=False, indent=2)}

<요구사항>
{get_common_json_requirement()}

{{
  "title": "오프라인 대리상 점당매출 종합분석",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "우수 대리상",
      "ai_text": "당월 비교와 전년 비교를 통해 우수한 성과를 보이는 대리상을 분석한 내용. 구체적인 대리상명(shop_en_nm), 매출액, 전년 대비 변화율을 제시하고, 우수한 요인을 분석해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "수익성 개선 필요",
      "ai_text": "당월 비교와 전년 비교를 통해 수익성 개선이 필요한 대리상을 분석한 내용. 구체적인 대리상명(shop_en_nm), 매출액, 전년 대비 변화율을 제시하고, 수익성 개선이 필요한 원인을 분석해줘."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "인사이트",
      "ai_text": "우수 대리상과 수익성 개선 필요 대리상 분석을 종합하여 핵심 인사이트를 제시한 내용. 대리상별 성과 차이의 원인, 개선 방안, 전략적 시사점을 구체적으로 제시해줘."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 우수 대리상: 당해 총 매출이 높고 전년 대비 성장률이 우수한 대리상 분석
- 수익성 개선 필요: 당해 총 매출이 낮거나 전년 대비 감소한 대리상 분석
- 인사이트: 대리상별 성과 차이의 원인과 개선 방안 제시

{get_common_prompt_footer()}
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱
        analysis_data = parse_llm_json_response(analysis_response, "오프라인 대리상 점당매출 종합분석")
        # 기본 구조 보완
        if len(analysis_data.get('sections', [])) < 3:
            analysis_data['sections'].extend([
                {"div": "종합분석-2", "sub_title": "수익성 개선 필요", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "인사이트", "ai_text": ""}
            ])
        
        # JSON 데이터 구성
        json_data = {
            'country': 'CN',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm_end,  # 당해 11월
            'yyyymm_py': f"{previous_year}11",
            'key': '(대리상오프)점당매출',
            'sub_key': '(대리상오프)점당매출 AI 분석',
            'analysis_data': {
                'title': analysis_data.get('title', '오프라인 대리상 점당매출 종합분석'),
                'sections': analysis_data.get('sections', [])
            },
            'summary': {
                'total_cy': round(total_cy / 1000, 0),
                'total_py': round(total_py / 1000, 0),
                'change_pct': round(((total_cy - total_py) / total_py * 100) if total_py != 0 else 0, 1),
                'total_agents': len(agent_summary_sorted),
                'analysis_period': f"{analysis_year}년 1월 ~ {analysis_year}년 11월 vs {previous_year}년 1월 ~ {previous_year}년 11월"
            },
            'agent_summary': agent_summary_sorted[:50],  # 상위 50개 대리상
            'raw_data': {
                'sample_records': [dict(r) for r in records[:50]],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm_end[2:]  # 202511 -> 2511
        filename = f"CN_{yyyymm_short}_{brd_cd}_대리상오프_점당매출"
        save_json(json_data, filename)
        
        # Markdown도 저장
        markdown_content = f"# {json_data['analysis_data'].get('title', '오프라인 대리상 점당매출 종합분석')}\n\n"
        for section in json_data['analysis_data'].get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 오프라인 대리상 점당매출 종합분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_discount_rate(yyyymm, brd_cd):
    """할인율 종합분석 - 채널별 할인율 분석 (전년월 VS 당해월, 추세 분석)"""
    print(f"\n{'='*60}")
    print(f"할인율 종합분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        start_yyyymm = f"{previous_year}01"  # 전년도 1월 (추세 분석용)
        
        print(f"분석 기간:")
        print(f"  - 전년월 VS 당해월: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        print(f"  - 추세 분석: {previous_year}년 1월 ~ {current_year}년 {current_month}월")
        
        # SQL 쿼리 실행 (추세 분석용: 전년 1월부터 당해 월까지)
        sql = get_discount_rate_query(yyyymm, yyyymm_py, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_tag_sales = sum(float(r.get('TAG_SALE_AMT', 0) or 0) for r in records)
        total_act_sales = sum(float(r.get('ACT_SALE_AMT', 0) or 0) for r in records)
        overall_discount = round((1 - total_act_sales / total_tag_sales) * 100, 1) if total_tag_sales > 0 else 0
        
        unique_channels = len(set(r.get('CHNL_NM', '') for r in records if r.get('CHNL_NM')))
        unique_months = len(set(r.get('YYYYMM', '') for r in records if r.get('YYYYMM')))
        
        print(f"총 태그매출: {total_tag_sales:,.0f}원 ({total_tag_sales/1000:.0f}k)")
        print(f"총 실제매출: {total_act_sales:,.0f}원 ({total_act_sales/1000:.0f}k)")
        print(f"전체 할인율: {overall_discount}%")
        print(f"채널 수: {unique_channels}개")
        print(f"분석 월 수: {unique_months}개월")
        
        # 당해월/전년월 데이터 분리
        current_month_data = [r for r in records if r.get('YYYYMM') == yyyymm]
        previous_month_data = [r for r in records if r.get('YYYYMM') == yyyymm_py]
        
        # 채널별 할인율 집계 (당해월)
        channel_discount_current = {}
        for record in current_month_data:
            chnl_nm = record.get('CHNL_NM') or '기타'
            tag_sale = float(record.get('TAG_SALE_AMT', 0) or 0)
            act_sale = float(record.get('ACT_SALE_AMT', 0) or 0)
            discount_pct = float(record.get('DISCOUNT_PCT', 0) or 0)
            
            if not chnl_nm:
                continue
            
            if chnl_nm not in channel_discount_current:
                channel_discount_current[chnl_nm] = {
                    'tag_sale_amt': 0,
                    'act_sale_amt': 0,
                    'discount_pct': 0
                }
            
            channel_discount_current[chnl_nm]['tag_sale_amt'] += tag_sale
            channel_discount_current[chnl_nm]['act_sale_amt'] += act_sale
        
        # 채널별 할인율 계산 (당해월)
        for chnl_nm in channel_discount_current.keys():
            tag = channel_discount_current[chnl_nm]['tag_sale_amt']
            act = channel_discount_current[chnl_nm]['act_sale_amt']
            channel_discount_current[chnl_nm]['discount_pct'] = round((1 - act / tag) * 100, 1) if tag > 0 else 0
        
        # 채널별 할인율 집계 (전년월)
        channel_discount_previous = {}
        for record in previous_month_data:
            chnl_nm = record.get('CHNL_NM') or '기타'
            tag_sale = float(record.get('TAG_SALE_AMT', 0) or 0)
            act_sale = float(record.get('ACT_SALE_AMT', 0) or 0)
            
            if not chnl_nm:
                continue
            
            if chnl_nm not in channel_discount_previous:
                channel_discount_previous[chnl_nm] = {
                    'tag_sale_amt': 0,
                    'act_sale_amt': 0,
                    'discount_pct': 0
                }
            
            channel_discount_previous[chnl_nm]['tag_sale_amt'] += tag_sale
            channel_discount_previous[chnl_nm]['act_sale_amt'] += act_sale
        
        # 채널별 할인율 계산 (전년월)
        for chnl_nm in channel_discount_previous.keys():
            tag = channel_discount_previous[chnl_nm]['tag_sale_amt']
            act = channel_discount_previous[chnl_nm]['act_sale_amt']
            channel_discount_previous[chnl_nm]['discount_pct'] = round((1 - act / tag) * 100, 1) if tag > 0 else 0
        
        # 채널별 월별 할인율 추세 데이터 생성
        channel_trend_data = {}
        for record in records:
            chnl_nm = record.get('CHNL_NM') or '기타'
            yyyymm_val = record.get('YYYYMM') or ''
            discount_pct = float(record.get('DISCOUNT_PCT', 0) or 0)
            
            if not chnl_nm or not yyyymm_val:
                continue
            
            if chnl_nm not in channel_trend_data:
                channel_trend_data[chnl_nm] = {}
            
            channel_trend_data[chnl_nm][yyyymm_val] = discount_pct
        
        # 채널별 요약 데이터 생성 (당해월/전년월 비교)
        channel_summary = {}
        all_channels = set(channel_discount_current.keys()) | set(channel_discount_previous.keys())
        
        for chnl_nm in all_channels:
            current_discount = channel_discount_current.get(chnl_nm, {}).get('discount_pct', 0)
            previous_discount = channel_discount_previous.get(chnl_nm, {}).get('discount_pct', 0)
            change_pct = current_discount - previous_discount
            
            # 월별 추세 데이터
            trend_months = sorted(channel_trend_data.get(chnl_nm, {}).keys())
            trend_values = [channel_trend_data[chnl_nm].get(m, 0) for m in trend_months]
            
            channel_summary[chnl_nm] = {
                'current_discount': current_discount,
                'previous_discount': previous_discount,
                'change_pct': round(change_pct, 1),
                'trend_months': trend_months,
                'trend_values': trend_values,
                'current_tag_sale': round(channel_discount_current.get(chnl_nm, {}).get('tag_sale_amt', 0) / 1000, 0),
                'current_act_sale': round(channel_discount_current.get(chnl_nm, {}).get('act_sale_amt', 0) / 1000, 0),
                'previous_tag_sale': round(channel_discount_previous.get(chnl_nm, {}).get('tag_sale_amt', 0) / 1000, 0),
                'previous_act_sale': round(channel_discount_previous.get(chnl_nm, {}).get('act_sale_amt', 0) / 1000, 0)
            }
        
        # 당해/전년 데이터가 모두 있는 채널만 필터링
        valid_channels = [
            chnl for chnl, data in channel_summary.items()
            if data['current_discount'] > 0 and data['previous_discount'] > 0
        ]
        
        # 전체 할인율 계산 (당해월/전년월)
        total_tag_current = sum(channel_discount_current.get(chnl, {}).get('tag_sale_amt', 0) for chnl in valid_channels)
        total_act_current = sum(channel_discount_current.get(chnl, {}).get('act_sale_amt', 0) for chnl in valid_channels)
        total_discount_current = round((1 - total_act_current / total_tag_current) * 100, 1) if total_tag_current > 0 else 0
        
        total_tag_previous = sum(channel_discount_previous.get(chnl, {}).get('tag_sale_amt', 0) for chnl in valid_channels)
        total_act_previous = sum(channel_discount_previous.get(chnl, {}).get('act_sale_amt', 0) for chnl in valid_channels)
        total_discount_previous = round((1 - total_act_previous / total_tag_previous) * 100, 1) if total_tag_previous > 0 else 0
        total_change_pct = total_discount_current - total_discount_previous
        
        # AI 분석 요청
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 할인율 전략 전문가야. 채널별 할인율 종합분석을 수행해줘.

**분석 기간**
- 전년월 VS 당해월: {previous_year}년 {current_month}월 ({yyyymm_py}) VS {current_year}년 {current_month}월 ({yyyymm})
- 추세 분석: {previous_year}년 1월 ~ {current_year}년 {current_month}월 ({start_yyyymm} ~ {yyyymm})

**전체 요약**
- 전체 할인율 (당해월): {total_discount_current}%
- 전체 할인율 (전년월): {total_discount_previous}%
- 전년대비 변화: {round(total_change_pct, 1)}%p
- 분석 채널 수: {len(valid_channels)}개
- 분석 채널 목록: {', '.join(valid_channels)}

**채널별 할인율 데이터** (당해월 VS 전년월 비교)
{json_dumps_safe({k: v for k, v in channel_summary.items() if k in valid_channels}, ensure_ascii=False, indent=2)}

**채널별 월별 할인율 추세 데이터** (전년 1월부터 당해 월까지)
{json_dumps_safe({k: {'months': v['trend_months'], 'values': v['trend_values']} for k, v in channel_summary.items() if k in valid_channels}, ensure_ascii=False, indent=2)}

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 채널별 할인율을 종합적으로 분석하여:
1. 할인율 전략이 우수한 채널: 할인율이 낮고 전년대비 개선되거나 안정적인 채널들을 식별
2. 주의 필요 채널: 할인율이 높거나 전년대비 악화된 채널들을 식별하고 개선 방향 제시
3. AI 권장사항: 채널별 할인율 전략에 대한 구체적인 권장사항과 액션플랜

<요구사항>
{get_common_json_requirement()}

{{
  "title": "채널별 할인율 종합분석",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "할인율 전략이 우수한 채널",
      "ai_text": "할인율이 낮고 전년대비 개선되거나 안정적인 채널들을 분석. 구체적인 채널명과 할인율 수치, 전년대비 변화율을 포함하여 분석. (최대 3줄)"
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "주의 필요 채널",
      "ai_text": "할인율이 높거나 전년대비 악화된 채널들을 분석. 구체적인 채널명과 할인율 수치, 전년대비 변화율, 문제점을 포함하여 분석. (최대 3줄)"
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "AI 권장사항",
      "ai_text": "채널별 할인율 전략에 대한 구체적인 권장사항과 액션플랜. 우수 채널의 성공 요인, 주의 채널의 개선 방안, 전체적인 할인율 전략 방향을 제시. (최대 4줄)"
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 할인율은 % 단위로 표시하고, 변화율은 %p(퍼센트포인트)로 표시
- 채널별 할인율 수치와 전년대비 변화율을 구체적으로 언급
- 추세 데이터를 활용하여 월별 할인율 변화 패턴도 분석

{get_common_prompt_footer()}
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱
        analysis_data = parse_llm_json_response(analysis_response, "채널별 할인율 종합분석")
        # 기본 구조 보완
        if len(analysis_data.get('sections', [])) < 3:
            analysis_data['sections'].extend([
                {"div": "종합분석-2", "sub_title": "주의 필요 채널", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "AI 권장사항", "ai_text": ""}
            ])
        
        # JSON 데이터 생성
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
                'total_discount_current': total_discount_current,
                'total_discount_previous': total_discount_previous,
                'total_change_pct': round(total_change_pct, 1),
                'unique_channels': unique_channels,
                'unique_months': unique_months,
                'analysis_period_month': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월",
                'analysis_period_trend': f"{previous_year}년 1월 ~ {current_year}년 {current_month}월"
            },
            'channel_summary': channel_summary,
            'trend_data': {
                'trend_months': sorted(list(set(r.get('YYYYMM', '') for r in records if r.get('YYYYMM')))),
                'monthly_totals': [],
                'channel_trends': channel_trend_data
            },
            'raw_data': {
                'sample_records': [dict(r) for r in records[:50]],
                'total_records_count': len(records)
            }
        }
        
        # 월별 전체 할인율 계산 (추세 분석용)
        monthly_totals_dict = {}
        for record in records:
            yyyymm_val = record.get('YYYYMM') or ''
            tag_sale = float(record.get('TAG_SALE_AMT', 0) or 0)
            act_sale = float(record.get('ACT_SALE_AMT', 0) or 0)
            
            if yyyymm_val:
                if yyyymm_val not in monthly_totals_dict:
                    monthly_totals_dict[yyyymm_val] = {'tag': 0, 'act': 0}
                monthly_totals_dict[yyyymm_val]['tag'] += tag_sale
                monthly_totals_dict[yyyymm_val]['act'] += act_sale
        
        for yyyymm_val in sorted(json_data['trend_data']['trend_months']):
            tag = monthly_totals_dict.get(yyyymm_val, {}).get('tag', 0)
            act = monthly_totals_dict.get(yyyymm_val, {}).get('act', 0)
            discount = round((1 - act / tag) * 100, 1) if tag > 0 else 0
            
            json_data['trend_data']['monthly_totals'].append({
                'yyyymm': yyyymm_val,
                'tag_sale_amt': round(tag / 1000, 0),
                'act_sale_amt': round(act / 1000, 0),
                'discount_pct': discount
            })
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]  # 202511 -> 2511
        filename = f"CN_{yyyymm_short}_{brd_cd}_할인율_종합분석"
        save_json(json_data, filename)
        
        # Markdown도 저장
        markdown_content = f"# {analysis_data.get('title', '채널별 할인율 종합분석')}\n\n"
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
        
        # SQL 쿼리 실행 (브랜드별)
        sql = get_operating_expense_query(yyyymm, yyyymm_py, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        # 법인 전체 데이터 조회 (모든 브랜드 합계)
        sql_all_brands = get_operating_expense_all_brands_query(yyyymm, yyyymm_py)
        df_all_brands = run_query(sql_all_brands, engine)
        records_all_brands = df_all_brands.to_dicts()
        
        if not records:
            print("브랜드별 데이터가 없습니다.")
            return None
        
        if not records_all_brands:
            print("법인 전체 데이터가 없습니다.")
            records_all_brands = []
        
        # 데이터 집계: 당해/전년 동월, 누적 YTD, 1년 추세로 구분
        # 1. 당해당월 데이터
        current_month_data = [r for r in records if r.get('PST_YYYYMM') == yyyymm]
        # 2. 전년 동월 데이터
        previous_month_data = [r for r in records if r.get('PST_YYYYMM') == yyyymm_py]
        # 3. 당해 YTD 누적 (당해 1월 ~ 당해당월) - 예: 2501~2511
        current_year_start = f"{current_year}01"
        current_ytd_data = [r for r in records if current_year_start <= r.get('PST_YYYYMM', '') <= yyyymm]
        # 4. 전년 YTD 누적 (전년도 1월 ~ 전년 동월) - 예: 2401~2411
        previous_year_start = f"{previous_year}01"
        previous_ytd_data = [r for r in records if previous_year_start <= r.get('PST_YYYYMM', '') <= yyyymm_py]
        # 5. 1년 추세 (전년도 1월 ~ 당해당월, 월별) - 예: 2401~2511
        trend_start_yyyymm = f"{previous_year}01"
        trend_data_by_month = {}
        for r in records:
            month = r.get('PST_YYYYMM', '')
            if trend_start_yyyymm <= month <= yyyymm:
                if month not in trend_data_by_month:
                    trend_data_by_month[month] = []
                trend_data_by_month[month].append(r)
        
        # 법인 전체 데이터 집계 (모든 브랜드 합계)
        # 1. 법인 전체 당해당월 데이터
        all_brands_current_month_data = [r for r in records_all_brands if r.get('PST_YYYYMM') == yyyymm] if records_all_brands else []
        # 2. 법인 전체 전년 동월 데이터
        all_brands_previous_month_data = [r for r in records_all_brands if r.get('PST_YYYYMM') == yyyymm_py] if records_all_brands else []
        # 3. 법인 전체 당해 YTD 누적
        all_brands_current_ytd_data = [r for r in records_all_brands if current_year_start <= r.get('PST_YYYYMM', '') <= yyyymm] if records_all_brands else []
        # 4. 법인 전체 전년 YTD 누적
        all_brands_previous_ytd_data = [r for r in records_all_brands if previous_year_start <= r.get('PST_YYYYMM', '') <= yyyymm_py] if records_all_brands else []
        # 5. 법인 전체 1년 추세
        all_brands_trend_data_by_month = {}
        if records_all_brands:
            for r in records_all_brands:
                month = r.get('PST_YYYYMM', '')
                if trend_start_yyyymm <= month <= yyyymm:
                    if month not in all_brands_trend_data_by_month:
                        all_brands_trend_data_by_month[month] = []
                    all_brands_trend_data_by_month[month].append(r)
        
        # 영업비 계정별 집계 함수
        def aggregate_expenses(data_list):
            """영업비 계정별 집계"""
            result = {
                'ad_cst_oprt': 0,  # 광고비
                'slry_csy_oprt': 0,  # 인건비
                'emp_bnft_cst_oprt': 0,  # 복리후생비
                'pmt_cms_oprt': 0,  # 지급수수료
                'shop_rnt_oprt': 0,  # 임차료
                'evnt_cst_oprt': 0,  # 수주회
                'tax_cst_oprt': 0,  # 세금과공과
                'deprc_cst_oprt': 0,  # 감가상각비
                'etc_cst_oprt': 0,  # 기타
                'sale_amt': 0,  # 매출액
                'sale_amt_vat': 0  # 매출액(VAT 제외)
            }
            for r in data_list:
                result['ad_cst_oprt'] += float(r.get('AD_CST_OPRT', 0) or 0)
                result['slry_csy_oprt'] += float(r.get('SLRY_CSY_OPRT', 0) or 0)
                result['emp_bnft_cst_oprt'] += float(r.get('EMP_BNFT_CST_OPRT', 0) or 0)
                result['pmt_cms_oprt'] += float(r.get('PMT_CMS_OPRT', 0) or 0)
                result['shop_rnt_oprt'] += float(r.get('SHOP_RNT_OPRT', 0) or 0)
                result['evnt_cst_oprt'] += float(r.get('EVNT_CST_OPRT', 0) or 0)
                result['tax_cst_oprt'] += float(r.get('TAX_CST_OPRT', 0) or 0)
                result['deprc_cst_oprt'] += float(r.get('DEPRC_CST_OPRT', 0) or 0)
                result['etc_cst_oprt'] += float(r.get('ETC_CST_OPRT', 0) or 0)
                result['sale_amt'] += float(r.get('SALE_AMT', 0) or 0)
                result['sale_amt_vat'] += float(r.get('SALE_AMT_VAT', 0) or 0)
            return result
        
        # 각 구간별 집계 (브랜드별)
        current_month_summary = aggregate_expenses(current_month_data)
        previous_month_summary = aggregate_expenses(previous_month_data)
        current_ytd_summary = aggregate_expenses(current_ytd_data)
        previous_ytd_summary = aggregate_expenses(previous_ytd_data)
        
        # 1년 추세 월별 집계 (브랜드별)
        trend_by_month = {}
        for month, month_data in sorted(trend_data_by_month.items()):
            trend_by_month[month] = aggregate_expenses(month_data)
        
        # 법인 전체 각 구간별 집계
        all_brands_current_month_summary = aggregate_expenses(all_brands_current_month_data) if records_all_brands else {}
        all_brands_previous_month_summary = aggregate_expenses(all_brands_previous_month_data) if records_all_brands else {}
        all_brands_current_ytd_summary = aggregate_expenses(all_brands_current_ytd_data) if records_all_brands else {}
        all_brands_previous_ytd_summary = aggregate_expenses(all_brands_previous_ytd_data) if records_all_brands else {}
        
        # 법인 전체 1년 추세 월별 집계
        all_brands_trend_by_month = {}
        if records_all_brands:
            for month, month_data in sorted(all_brands_trend_data_by_month.items()):
                all_brands_trend_by_month[month] = aggregate_expenses(month_data)
        
        # k 단위로 변환
        def convert_to_k(data_dict):
            """모든 금액을 k 단위로 변환"""
            result = {}
            for key, value in data_dict.items():
                result[key] = round(value / 1000, 0) if isinstance(value, (int, float)) else value
            return result
        
        current_month_k = convert_to_k(current_month_summary)
        previous_month_k = convert_to_k(previous_month_summary)
        current_ytd_k = convert_to_k(current_ytd_summary)
        previous_ytd_k = convert_to_k(previous_ytd_summary)
        trend_by_month_k = {month: convert_to_k(data) for month, data in trend_by_month.items()}
        
        # 법인 전체 k 단위로 변환
        all_brands_current_month_k = convert_to_k(all_brands_current_month_summary) if records_all_brands else {}
        all_brands_previous_month_k = convert_to_k(all_brands_previous_month_summary) if records_all_brands else {}
        all_brands_current_ytd_k = convert_to_k(all_brands_current_ytd_summary) if records_all_brands else {}
        all_brands_previous_ytd_k = convert_to_k(all_brands_previous_ytd_summary) if records_all_brands else {}
        all_brands_trend_by_month_k = {month: convert_to_k(data) for month, data in all_brands_trend_by_month.items()} if records_all_brands else {}
        
        # 법인 전체 대비 브랜드 비중 계산 함수
        def calculate_ratio(brand_amount, all_brands_amount):
            """법인 전체 대비 브랜드 비중 계산 (%)"""
            if all_brands_amount and all_brands_amount > 0:
                return round((brand_amount / all_brands_amount) * 100, 1)
            return 0.0
        
        # 법인 전체 대비 비중 계산 (당해당월)
        brand_vs_all_current_month = {}
        if records_all_brands:
            total_all_current_month = sum([
                all_brands_current_month_summary.get('ad_cst_oprt', 0),
                all_brands_current_month_summary.get('slry_csy_oprt', 0),
                all_brands_current_month_summary.get('emp_bnft_cst_oprt', 0),
                all_brands_current_month_summary.get('pmt_cms_oprt', 0),
                all_brands_current_month_summary.get('shop_rnt_oprt', 0),
                all_brands_current_month_summary.get('evnt_cst_oprt', 0),
                all_brands_current_month_summary.get('tax_cst_oprt', 0),
                all_brands_current_month_summary.get('deprc_cst_oprt', 0),
                all_brands_current_month_summary.get('etc_cst_oprt', 0)
            ])
            total_brand_current_month = sum([
                current_month_summary.get('ad_cst_oprt', 0),
                current_month_summary.get('slry_csy_oprt', 0),
                current_month_summary.get('emp_bnft_cst_oprt', 0),
                current_month_summary.get('pmt_cms_oprt', 0),
                current_month_summary.get('shop_rnt_oprt', 0),
                current_month_summary.get('evnt_cst_oprt', 0),
                current_month_summary.get('tax_cst_oprt', 0),
                current_month_summary.get('deprc_cst_oprt', 0),
                current_month_summary.get('etc_cst_oprt', 0)
            ])
            brand_vs_all_current_month = {
                'brand_total': round(total_brand_current_month / 1000, 0),
                'all_brands_total': round(total_all_current_month / 1000, 0),
                'ratio': calculate_ratio(total_brand_current_month, total_all_current_month),
                'by_account': {
                    'ad_cst_oprt': {
                        'brand': current_month_k.get('ad_cst_oprt', 0),
                        'all_brands': all_brands_current_month_k.get('ad_cst_oprt', 0),
                        'ratio': calculate_ratio(current_month_summary.get('ad_cst_oprt', 0), all_brands_current_month_summary.get('ad_cst_oprt', 0))
                    },
                    'slry_csy_oprt': {
                        'brand': current_month_k.get('slry_csy_oprt', 0),
                        'all_brands': all_brands_current_month_k.get('slry_csy_oprt', 0),
                        'ratio': calculate_ratio(current_month_summary.get('slry_csy_oprt', 0), all_brands_current_month_summary.get('slry_csy_oprt', 0))
                    },
                    'emp_bnft_cst_oprt': {
                        'brand': current_month_k.get('emp_bnft_cst_oprt', 0),
                        'all_brands': all_brands_current_month_k.get('emp_bnft_cst_oprt', 0),
                        'ratio': calculate_ratio(current_month_summary.get('emp_bnft_cst_oprt', 0), all_brands_current_month_summary.get('emp_bnft_cst_oprt', 0))
                    },
                    'pmt_cms_oprt': {
                        'brand': current_month_k.get('pmt_cms_oprt', 0),
                        'all_brands': all_brands_current_month_k.get('pmt_cms_oprt', 0),
                        'ratio': calculate_ratio(current_month_summary.get('pmt_cms_oprt', 0), all_brands_current_month_summary.get('pmt_cms_oprt', 0))
                    },
                    'shop_rnt_oprt': {
                        'brand': current_month_k.get('shop_rnt_oprt', 0),
                        'all_brands': all_brands_current_month_k.get('shop_rnt_oprt', 0),
                        'ratio': calculate_ratio(current_month_summary.get('shop_rnt_oprt', 0), all_brands_current_month_summary.get('shop_rnt_oprt', 0))
                    },
                    'evnt_cst_oprt': {
                        'brand': current_month_k.get('evnt_cst_oprt', 0),
                        'all_brands': all_brands_current_month_k.get('evnt_cst_oprt', 0),
                        'ratio': calculate_ratio(current_month_summary.get('evnt_cst_oprt', 0), all_brands_current_month_summary.get('evnt_cst_oprt', 0))
                    },
                    'tax_cst_oprt': {
                        'brand': current_month_k.get('tax_cst_oprt', 0),
                        'all_brands': all_brands_current_month_k.get('tax_cst_oprt', 0),
                        'ratio': calculate_ratio(current_month_summary.get('tax_cst_oprt', 0), all_brands_current_month_summary.get('tax_cst_oprt', 0))
                    },
                    'deprc_cst_oprt': {
                        'brand': current_month_k.get('deprc_cst_oprt', 0),
                        'all_brands': all_brands_current_month_k.get('deprc_cst_oprt', 0),
                        'ratio': calculate_ratio(current_month_summary.get('deprc_cst_oprt', 0), all_brands_current_month_summary.get('deprc_cst_oprt', 0))
                    },
                    'etc_cst_oprt': {
                        'brand': current_month_k.get('etc_cst_oprt', 0),
                        'all_brands': all_brands_current_month_k.get('etc_cst_oprt', 0),
                        'ratio': calculate_ratio(current_month_summary.get('etc_cst_oprt', 0), all_brands_current_month_summary.get('etc_cst_oprt', 0))
                    }
                }
            }
        
        # 법인 전체 대비 비중 계산 (당해 YTD)
        brand_vs_all_current_ytd = {}
        if records_all_brands:
            total_all_current_ytd = sum([
                all_brands_current_ytd_summary.get('ad_cst_oprt', 0),
                all_brands_current_ytd_summary.get('slry_csy_oprt', 0),
                all_brands_current_ytd_summary.get('emp_bnft_cst_oprt', 0),
                all_brands_current_ytd_summary.get('pmt_cms_oprt', 0),
                all_brands_current_ytd_summary.get('shop_rnt_oprt', 0),
                all_brands_current_ytd_summary.get('evnt_cst_oprt', 0),
                all_brands_current_ytd_summary.get('tax_cst_oprt', 0),
                all_brands_current_ytd_summary.get('deprc_cst_oprt', 0),
                all_brands_current_ytd_summary.get('etc_cst_oprt', 0)
            ])
            total_brand_current_ytd = sum([
                current_ytd_summary.get('ad_cst_oprt', 0),
                current_ytd_summary.get('slry_csy_oprt', 0),
                current_ytd_summary.get('emp_bnft_cst_oprt', 0),
                current_ytd_summary.get('pmt_cms_oprt', 0),
                current_ytd_summary.get('shop_rnt_oprt', 0),
                current_ytd_summary.get('evnt_cst_oprt', 0),
                current_ytd_summary.get('tax_cst_oprt', 0),
                current_ytd_summary.get('deprc_cst_oprt', 0),
                current_ytd_summary.get('etc_cst_oprt', 0)
            ])
            brand_vs_all_current_ytd = {
                'brand_total': round(total_brand_current_ytd / 1000, 0),
                'all_brands_total': round(total_all_current_ytd / 1000, 0),
                'ratio': calculate_ratio(total_brand_current_ytd, total_all_current_ytd)
            }
        
        # 총 영업비 계산
        total_expense_current_month = sum([
            current_month_summary['ad_cst_oprt'],
            current_month_summary['slry_csy_oprt'],
            current_month_summary['emp_bnft_cst_oprt'],
            current_month_summary['pmt_cms_oprt'],
            current_month_summary['shop_rnt_oprt'],
            current_month_summary['evnt_cst_oprt'],
            current_month_summary['tax_cst_oprt'],
            current_month_summary['deprc_cst_oprt'],
            current_month_summary['etc_cst_oprt']
        ])
        
        print(f"당해당월({yyyymm}) 영업비: {total_expense_current_month:,.0f}원 ({total_expense_current_month/1000:.0f}k)")
        
        # AI 분석 요청
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 재무 분석 전문가야. 영업비 종합분석을 수행해줘.

**중요: 아래 데이터는 4가지 분석 유형으로 명확히 구분되어 있습니다. 각 섹션에서 어떤 비교인지 반드시 명시해야 합니다.**

**분석 기간**
- 당해당월: {current_year}년 {current_month}월 ({yyyymm})
- 전년 동월: {previous_year}년 {current_month}월 ({yyyymm_py})
- 당해 누적: {current_year}년 1월 ~ {current_year}년 {current_month}월 ({current_year}01~{yyyymm})
- 전년 누적: {previous_year}년 1월 ~ {previous_year}년 {current_month}월 ({previous_year}01~{yyyymm_py})
- 추세 분석: {previous_year}년 1월 ~ {current_year}년 {current_month}월 ({previous_year}01~{yyyymm})

**1. 전년/당해 동월 비교** ({yyyymm_py} VS {yyyymm})
전년 동월({yyyymm_py}) 영업비 계정별 (모든 금액은 k 단위):
{json_dumps_safe(previous_month_k, ensure_ascii=False, indent=2)}

당해당월({yyyymm}) 영업비 계정별 (모든 금액은 k 단위):
{json_dumps_safe(current_month_k, ensure_ascii=False, indent=2)}

**2. 누적 YTD 비교** (전년 누적 VS 당해 누적)
전년 누적 ({previous_year}년 1월 ~ {previous_year}년 {current_month}월, {previous_year}01~{yyyymm_py}) 영업비 계정별 (모든 금액은 k 단위):
{json_dumps_safe(previous_ytd_k, ensure_ascii=False, indent=2)}

당해 누적 ({current_year}년 1월 ~ {current_year}년 {current_month}월, {current_year}01~{yyyymm}) 영업비 계정별 (모든 금액은 k 단위):
{json_dumps_safe(current_ytd_k, ensure_ascii=False, indent=2)}

**3. 1년 추세 분석** ({previous_year}년 1월 ~ {current_year}년 {current_month}월, {previous_year}01~{yyyymm}, 월별)
월별 영업비 계정별 추이 (모든 금액은 k 단위):
{json_dumps_safe(trend_by_month_k, ensure_ascii=False, indent=2)}

**4. 법인 전체 대비 브랜드 비중 분석** ({BRAND_CODE_MAP.get(brd_cd, brd_cd)} vs 법인 전체)
법인 전체: MLB + MLB KIDS + DISCOVERY + DUVETICA + SERGIO TACCHINI + SUPRA

당해당월({yyyymm}) 법인 전체 대비 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 비중 (모든 금액은 k 단위):
{json_dumps_safe(brand_vs_all_current_month, ensure_ascii=False, indent=2)}

당해 누적({current_year}01~{yyyymm}) 법인 전체 대비 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 비중 (모든 금액은 k 단위):
{json_dumps_safe(brand_vs_all_current_ytd, ensure_ascii=False, indent=2)}

<요구사항>
{get_common_json_requirement()}

{{
  "title": "영업비 종합분석",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "전년/당해 동월 비교 ({yyyymm_py} VS {yyyymm})",
      "ai_text": "전년 동월({yyyymm_py})과 당해당월({yyyymm})을 비교한 분석 내용. 각 영업비 계정별 변화율과 원인을 명확히 분석해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "누적 YTD 비교 (전년 누적 VS 당해 누적)",
      "ai_text": "전년 누적({previous_year}년 1월 ~ {previous_year}년 {current_month}월, {previous_year}01~{yyyymm_py})와 당해 누적({current_year}년 1월 ~ {current_year}년 {current_month}월, {current_year}01~{yyyymm})를 비교한 분석 내용. 누적 관점에서의 변화를 분석해줘."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "1년 추세 분석 ({previous_year}년 1월 ~ {current_year}년 {current_month}월, {previous_year}01~{yyyymm})",
      "ai_text": "{previous_year}년 1월부터 {current_year}년 {current_month}월까지({previous_year}01~{yyyymm})의 월별 추이를 분석하여 계절성, 트렌드, 특이사항을 파악한 내용."
    }},
    {{
      "div": "종합분석-4",
      "sub_title": "법인 전체 대비 브랜드 비중 분석 ({BRAND_CODE_MAP.get(brd_cd, brd_cd)} vs 법인 전체)",
      "ai_text": "{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 영업비가 법인 전체(MLB + MLB KIDS + DISCOVERY + DUVETICA + SERGIO TACCHINI + SUPRA) 대비 차지하는 비중을 분석한 내용. 각 영업비 계정별로 법인 전체 대비 비중과 브랜드의 위치를 명확히 분석해줘."
    }},
    {{
      "div": "종합분석-5",
      "sub_title": "전략적 시사점",
      "ai_text": "위 4가지 분석을 종합하여 단기 및 중장기 전략 방향을 제시한 내용."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- **중요: 각 섹션에서 어떤 비교인지 반드시 명시해야 함**
  - "전년/당해 동월 비교" 섹션: "{yyyymm_py} VS {yyyymm}" 비교임을 명시 (전년 동월 → 당해 동월)
  - "누적 YTD 비교" 섹션: "전년 누적({previous_year}01~{yyyymm_py}) VS 당해 누적({current_year}01~{yyyymm})" 비교임을 명시
  - "1년 추세 분석" 섹션: "{previous_year}년 1월 ~ {current_year}년 {current_month}월({previous_year}01~{yyyymm})" 기간의 월별 추이임을 명시
  - "법인 전체 대비 브랜드 비중 분석" 섹션: "{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 영업비가 법인 전체(MLB + MLB KIDS + DISCOVERY + DUVETICA + SERGIO TACCHINI + SUPRA) 대비 차지하는 비중" 분석임을 명시
- 영업비 계정별(광고비, 인건비, 복리후생비, 지급수수료, 임차료, 수주회, 세금과공과, 감가상각비, 기타) 분석
- 각 비교에서 변화율(%)을 계산하여 제시
- 단기 전략 방향과 중장기 전략 방향을 구체적으로 시사

{get_common_prompt_footer()}
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱
        analysis_data = parse_llm_json_response(analysis_response, "영업비 종합분석")
        
        # JSON 데이터 구성
        total_expense_current_month_k = round(total_expense_current_month / 1000, 0)
        total_expense_previous_month = sum([
            previous_month_summary['ad_cst_oprt'],
            previous_month_summary['slry_csy_oprt'],
            previous_month_summary['emp_bnft_cst_oprt'],
            previous_month_summary['pmt_cms_oprt'],
            previous_month_summary['shop_rnt_oprt'],
            previous_month_summary['evnt_cst_oprt'],
            previous_month_summary['tax_cst_oprt'],
            previous_month_summary['deprc_cst_oprt'],
            previous_month_summary['etc_cst_oprt']
        ])
        total_expense_previous_month_k = round(total_expense_previous_month / 1000, 0)
        
        total_expense_current_ytd = sum([
            current_ytd_summary['ad_cst_oprt'],
            current_ytd_summary['slry_csy_oprt'],
            current_ytd_summary['emp_bnft_cst_oprt'],
            current_ytd_summary['pmt_cms_oprt'],
            current_ytd_summary['shop_rnt_oprt'],
            current_ytd_summary['evnt_cst_oprt'],
            current_ytd_summary['tax_cst_oprt'],
            current_ytd_summary['deprc_cst_oprt'],
            current_ytd_summary['etc_cst_oprt']
        ])
        total_expense_current_ytd_k = round(total_expense_current_ytd / 1000, 0)
        
        total_expense_previous_ytd = sum([
            previous_ytd_summary['ad_cst_oprt'],
            previous_ytd_summary['slry_csy_oprt'],
            previous_ytd_summary['emp_bnft_cst_oprt'],
            previous_ytd_summary['pmt_cms_oprt'],
            previous_ytd_summary['shop_rnt_oprt'],
            previous_ytd_summary['evnt_cst_oprt'],
            previous_ytd_summary['tax_cst_oprt'],
            previous_ytd_summary['deprc_cst_oprt'],
            previous_ytd_summary['etc_cst_oprt']
        ])
        total_expense_previous_ytd_k = round(total_expense_previous_ytd / 1000, 0)
        
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
                'current_month_expense': total_expense_current_month_k,
                'previous_month_expense': total_expense_previous_month_k,
                'current_ytd_expense': total_expense_current_ytd_k,
                'previous_ytd_expense': total_expense_previous_ytd_k,
                'total_records': len(records),
                'analysis_period_month': f"{previous_year}년 {current_month}월 VS {current_year}년 {current_month}월",
                'analysis_period_ytd': f"{previous_year}년 1월~{previous_year}년 {current_month}월 VS {current_year}년 1월~{current_year}년 {current_month}월",
                'trend_period': f"{previous_year}년 1월 ~ {current_year}년 {current_month}월 ({previous_year}01~{yyyymm})"
            },
            'month_comparison': {
                'previous_month': previous_month_k,
                'current_month': current_month_k
            },
            'ytd_comparison': {
                'previous_ytd': previous_ytd_k,
                'current_ytd': current_ytd_k
            },
            'trend_by_month': trend_by_month_k,
            'brand_vs_all_brands': {
                'current_month': brand_vs_all_current_month if records_all_brands else {},
                'current_ytd': brand_vs_all_current_ytd if records_all_brands else {}
            },
            'all_brands_summary': {
                'current_month': all_brands_current_month_k if records_all_brands else {},
                'previous_month': all_brands_previous_month_k if records_all_brands else {},
                'current_ytd': all_brands_current_ytd_k if records_all_brands else {},
                'previous_ytd': all_brands_previous_ytd_k if records_all_brands else {}
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

def analyze_monthly_channel_sales_trend(yyyymm, brd_cd):
    """월별 채널별 매출 추세 분석"""
    print(f"\n{'='*60}")
    print(f"월별 채널별 매출 추세 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (최근 12개월)
        # 함수 파라미터 yyyymm을 기준으로 최근 12개월 계산
        analysis_year = int(yyyymm[:4])
        analysis_month = int(yyyymm[4:6])
        
        # 최근 12개월 계산 (yyyymm 포함하여 12개월 전까지)
        # 예: 202601 -> 202502부터 202601까지
        if analysis_month == 12:
            start_year = analysis_year - 1
            start_month = 1
        else:
            start_year = analysis_year - 1
            start_month = analysis_month + 1
        
        yyyymm_start = f"{start_year:04d}{start_month:02d}"
        yyyymm_end = yyyymm  # 함수 파라미터로 지정한 연월
        
        previous_year = analysis_year - 1
        yyyymm_py = f"{previous_year:04d}{analysis_month:02d}"
        
        print(f"분석 기간: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 (최근 12개월)")
        
        # SQL 쿼리 실행
        sql = f"""
--V2-레첼
WITH
-- SHOP : BOS 매핑용 매장
-- SAP 매장코드가 기준인 SAP_FNF.MST_SHOP에는 ERP 기준인 SHOP_CD 중복이 있을 수 있어 1건만 처리하는 로직 추가
SHOP AS (SELECT *
         FROM SAP_FNF.MST_SHOP
         QUALIFY
             ROW_NUMBER() OVER ( PARTITION BY BRD_CD, CNTRY_CD, SHOP_CD, AGNT_CD, MAP_SHOP_AGNT_CD ORDER BY SAP_SHOP_CD ) =
             1)
-- 최종조회쿼리
SELECT A.YYMM          AS YYYYMM
     , A.BRD_CD        AS BRD_CD
     , C.MGMT_CHNL_CD  as CHNL_CD
     , C.MGMT_CHNL_NM  AS CHNL_NM
     , SUM(A.SALE_AMT) AS SALE_AMT
FROM CHN.DM_SH_S_M A
         LEFT JOIN SAP_FNF.MST_PRDT B
                   ON A.PRDT_CD = B.PRDT_CD
         LEFT JOIN SHOP C
                   ON A.MAP_SHOP_AGNT_CD = C.MAP_SHOP_AGNT_CD
WHERE A.YYMM BETWEEN '{yyyymm_start}' AND '{yyyymm_end}'
  AND A.BRD_CD = '{brd_cd}'
GROUP BY A.YYMM
       , A.BRD_CD
       , c.MGMT_CHNL_CD
       , c.MGMT_CHNL_NM
ORDER BY A.YYMM DESC, CHNL_CD, SALE_AMT DESC
        """
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('SALE_AMT', 0) or 0) for r in records)
        unique_channels = len(set(r.get('CHNL_NM', '') for r in records if r.get('CHNL_NM')))
        unique_months = len(set(r.get('YYYYMM', '') for r in records if r.get('YYYYMM')))
        
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000:.0f}k)")
        print(f"채널 수: {unique_channels}개")
        print(f"분석 월 수: {unique_months}개월")
        
        # 데이터 가공: 월별/채널별 집계
        monthly_data = {}
        channel_data = {}
        
        for r in records:
            yyyymm_val = r.get('YYYYMM', '')
            chnl_nm = r.get('CHNL_NM', '기타')
            chnl_cd = r.get('CHNL_CD', '')
            sale_amt = float(r.get('SALE_AMT', 0) or 0)
            
            # 월별 데이터 집계
            if yyyymm_val not in monthly_data:
                monthly_data[yyyymm_val] = {
                    'total': 0,
                    'channels': {}
                }
            monthly_data[yyyymm_val]['total'] += sale_amt
            
            if chnl_nm not in monthly_data[yyyymm_val]['channels']:
                monthly_data[yyyymm_val]['channels'][chnl_nm] = 0
            monthly_data[yyyymm_val]['channels'][chnl_nm] += sale_amt
            
            # 채널별 데이터 집계
            if chnl_nm not in channel_data:
                channel_data[chnl_nm] = {
                    'chnl_cd': chnl_cd,
                    'total': 0,
                    'months': {}
                }
            channel_data[chnl_nm]['total'] += sale_amt
            
            if yyyymm_val not in channel_data[chnl_nm]['months']:
                channel_data[chnl_nm]['months'][yyyymm_val] = 0
            channel_data[chnl_nm]['months'][yyyymm_val] += sale_amt
        
        # 월별 총 매출 (k 단위)
        monthly_totals_k = {k: round(v['total'] / 1000, 0) for k, v in sorted(monthly_data.items())}
        
        # 채널별 총 매출 및 월별 추이 (k 단위)
        channel_summary = {}
        for chnl_nm, data in channel_data.items():
            channel_summary[chnl_nm] = {
                'chnl_cd': data['chnl_cd'],
                'total': round(data['total'] / 1000, 0),
                'months': {k: round(v / 1000, 0) for k, v in sorted(data['months'].items())}
            }
        
        # 채널별 정렬 (총 매출 기준 내림차순)
        channel_summary_sorted = dict(sorted(channel_summary.items(), key=lambda x: x[1]['total'], reverse=True))
        
        # LLM 분석 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 채널 전략 전문가야. 월별 채널별 매출 추세 분석을 수행해줘.

**분석 기간**: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 ({yyyymm_start}~{yyyymm_end}) (최근 12개월)

**월별 총 매출 추이** (모든 금액은 k 단위):
{json_dumps_safe(monthly_totals_k, ensure_ascii=False, indent=2)}

**채널별 매출 데이터** (모든 금액은 k 단위):
{json_dumps_safe(channel_summary_sorted, ensure_ascii=False, indent=2)}

<분석 목표>
1. 월별 주요인사이트: 각 월별 매출 변화와 주요 특징을 분석
2. 채널 트렌드: 채널별 성장/감소 추세와 채널 간 비교 분석
3. 전략 포인트: 데이터를 바탕으로 한 구체적인 전략 제안

<요구사항>
{get_common_json_requirement()}

{{
  "title": "월별 채널별 매출 추세 분석",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "월별 주요 인사이트",
      "ai_text": "각 월별 매출 변화와 주요 특징을 분석한 내용. 월별 총 매출 추이를 바탕으로 성장/감소 패턴, 계절성, 특이사항 등을 분석해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "채널 트렌드",
      "ai_text": "채널별 성장/감소 추세와 채널 간 비교 분석 내용. 각 채널의 월별 추이를 분석하고, 강세 채널과 약세 채널을 구체적으로 언급해줘."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "전략 포인트",
      "ai_text": "데이터를 바탕으로 한 구체적인 전략 제안 내용. 채널별 최적화 방안, 마케팅 전략, 리스크 관리 등 실행 가능한 전략을 제시해줘."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 월별 주요인사이트: 각 월의 특징과 변화 원인을 분석
- 채널 트렌드: 채널별 성장률, 비중 변화, 채널 간 비교를 분석
- 전략 포인트: 실행 가능한 구체적인 전략 제안

{get_common_prompt_footer()}
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱
        analysis_data = parse_llm_json_response(analysis_response, "월별 채널별 매출 추세 분석")
        # 기본 구조 보완
        if len(analysis_data.get('sections', [])) < 3:
            analysis_data['sections'].extend([
                {"div": "종합분석-2", "sub_title": "채널 트렌드", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "전략 포인트", "ai_text": ""}
            ])
        
        # JSON 데이터 구성
        json_data = {
            'country': 'CN',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm_end,  # 당해 당월 (현재 날짜 기준)
            'yyyymm_py': yyyymm_py,
            'key': '월별채널별매출추세',
            'analysis_data': analysis_data,
            'summary': {
                'total_sales': round(total_sales / 1000, 0),
                'unique_channels': unique_channels,
                'unique_months': unique_months,
                'analysis_period': f"{yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 (최근 12개월)"
            },
            'monthly_totals': monthly_totals_k,
            'channel_summary': channel_summary_sorted,
            'raw_data': {
                'sample_records': [dict(r) for r in records[:50]],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]  # 202510 -> 2510
        filename = f"CN_{yyyymm_short}_{brd_cd}_월별채널별매출추세분석"
        save_json(json_data, filename)
        
        # Markdown도 저장
        markdown_content = f"# {json_data['analysis_data'].get('title', '월별 채널별 매출 추세 분석')}\n\n"
        for section in json_data['analysis_data'].get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 월별 채널별 매출 추세 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_monthly_item_sales_trend(yyyymm, brd_cd):
    """월별 아이템별 매출 추세 분석"""
    print(f"\n{'='*60}")
    print(f"월별 아이템별 매출 추세 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (최근 12개월)
        # 함수 파라미터 yyyymm을 기준으로 최근 12개월 계산
        analysis_year = int(yyyymm[:4])
        analysis_month = int(yyyymm[4:6])
        
        # 최근 12개월 계산 (yyyymm 포함하여 12개월 전까지)
        # 예: 202601 -> 202502부터 202601까지
        if analysis_month == 12:
            start_year = analysis_year - 1
            start_month = 1
        else:
            start_year = analysis_year - 1
            start_month = analysis_month + 1
        
        yyyymm_start = f"{start_year:04d}{start_month:02d}"
        yyyymm_end = yyyymm  # 함수 파라미터로 지정한 연월
        
        previous_year = analysis_year - 1
        yyyymm_py = f"{previous_year:04d}{analysis_month:02d}"
        
        # 프롬프트에서 사용할 변수 정의
        current_year = analysis_year
        current_month = analysis_month
        
        print(f"분석 기간: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 (최근 12개월)")
        
        # SQL 쿼리 실행
        sql = f"""
WITH
    -- PARAM :
    PARAM AS ( SELECT 'CY' AS DIV, '{yyyymm_start}' AS STD_START_YYYYMM, '{yyyymm_end}' AS STD_END_YYYYMM -- start, end 기준년월 지정 필요
               -- UNION ALL
               -- SELECT 'PY' AS DIV, '202401' AS STD_START_YYYYMM, '202411' AS STD_END_YYYYMM
               )
    -- CY_ITEM : 딩헤 아이템 구분 기준
  , CY_ITEM AS ( SELECT A.PRDT_CD
                      , A.SESN
                      , A.PRDT_HRRC1_NM
                      , A.PRDT_HRRC2_NM
                      , A.PRDT_HRRC3_NM
                      , CASE
    --------------------------------------------------
    -- ACC 분류
    --------------------------------------------------
    -- 주의사항 : PRDT_HRRC2_NM => 첫번째 문자만 대문자고 나머지는 소문자 ..
        WHEN A.PRDT_HRRC1_NM = 'ACC' AND UPPER(A.PRDT_HRRC2_NM) = 'HEADWEAR'
            THEN '모자'
        WHEN A.PRDT_HRRC1_NM = 'ACC' AND UPPER(A.PRDT_HRRC2_NM) = 'SHOES'
            THEN '신발'
        WHEN A.PRDT_HRRC1_NM = 'ACC' AND UPPER(A.PRDT_HRRC2_NM) = 'BAG'
            THEN '가방'
        WHEN A.PRDT_HRRC1_NM = 'ACC' AND UPPER(A.PRDT_HRRC2_NM) = 'ACC_ETC'
            THEN '기타'
        --------------------------------------------------
        -- 의류 분류
        --------------------------------------------------
        -- 당시즌 (SN 통합)
        WHEN A.PRDT_HRRC1_NM = '의류' AND PARAM.STD_END_YYYYMM BETWEEN B.START_YYYYMM AND B.END_YYYYMM
            THEN REPLACE(A.SESN, 'N', 'S') || ' ' || A.PRDT_HRRC1_NM
        --------------------------------------------------
        -- 전시즌 (조회기준 월이 9~2일때만 존재)
        WHEN A.PRDT_HRRC1_NM = '의류' AND RIGHT(PARAM.STD_END_YYYYMM, 2)::INT IN ( 9, 10, 11, 12, 1, 2 ) AND
             TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -6),
                     'YYYYMM') BETWEEN B.START_YYYYMM AND B.END_YYYYMM
            THEN A.SESN || ' ' || A.PRDT_HRRC1_NM
        --------------------------------------------------
        -- 차기시즌
        WHEN A.PRDT_HRRC1_NM = '의류' AND B.START_YYYYMM > PARAM.STD_END_YYYYMM
            THEN '차기시즌 ' || A.PRDT_HRRC1_NM
        --------------------------------------------------
        -- 전년 SF 시즌
        --------------------------------------------------
        -- 조회기준 월이 3~8월일떄
        WHEN A.PRDT_HRRC1_NM = '의류' AND RIGHT(PARAM.STD_END_YYYYMM, 2)::INT IN ( 3, 4, 5, 6, 7, 8 ) AND
             (TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -6),
                      'YYYYMM') BETWEEN B.START_YYYYMM AND B.END_YYYYMM OR
              TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -12),
                      'YYYYMM') BETWEEN B.START_YYYYMM AND B.END_YYYYMM)
            THEN LEFT(A.SESN, 2) || 'SF ' || A.PRDT_HRRC1_NM
        --------------------------------------------------
        -- 조회기준 월이 9~2월일떄
        WHEN A.PRDT_HRRC1_NM = '의류' AND RIGHT(PARAM.STD_END_YYYYMM, 2)::INT IN ( 9, 10, 11, 12, 1, 2 ) AND
             (TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -12),
                      'YYYYMM') BETWEEN B.START_YYYYMM AND B.END_YYYYMM OR
              TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -18),
                      'YYYYMM') BETWEEN B.START_YYYYMM AND B.END_YYYYMM)
            THEN LEFT(A.SESN, 2) || 'SF ' || A.PRDT_HRRC1_NM
        --------------------------------------------------
        -- 과시즌
        --------------------------------------------------
        -- 조회기준 월이 3~8월일떄
        WHEN A.PRDT_HRRC1_NM = '의류' AND RIGHT(PARAM.STD_END_YYYYMM, 2)::INT IN ( 3, 4, 5, 6, 7, 8 ) AND
             B.END_YYYYMM < TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -12), 'YYYYMM')
            THEN '과시즌 ' || A.PRDT_HRRC1_NM
        --------------------------------------------------
        -- 조회기준 월이 9~2월일떄
        WHEN A.PRDT_HRRC1_NM = '의류' AND RIGHT(PARAM.STD_END_YYYYMM, 2)::INT IN ( 9, 10, 11, 12, 1, 2 ) AND
             B.END_YYYYMM < TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -18), 'YYYYMM')
            THEN '과시즌 ' || A.PRDT_HRRC1_NM
        ELSE '미지정'
                        END AS ITEM_STD
                 FROM SAP_FNF.MST_PRDT A
                     LEFT JOIN COMM.MST_SESN B
                             ON A.SESN = B.SESN
                     JOIN PARAM
                             ON PARAM.DIV = 'CY'
                 WHERE 1 = 1
                   AND A.SESN <> 'X' -- 저장품 제외
                 )
-- 최종조회쿼리
SELECT A.YYMM AS YYYYMM, A.BRD_CD AS BRD_CD, NVL(B.ITEM_STD, 'TBA') AS ITEM_STD, SUM(A.SALE_AMT) AS SALE_AMT
FROM CHN.DM_SH_S_M A
    join param
        on PAram.div = 'CY'
        and a.YYMM between param.STD_START_YYYYMM and param.STD_END_YYYYMM
    LEFT JOIN CY_ITEM B
            ON A.PRDT_CD = B.PRDT_CD
WHERE A.BRD_CD = '{brd_cd}' -- 브랜드조건 필터링 필요
GROUP BY A.YYMM
       , A.BRD_CD
       , B.ITEM_STD
having sum(a.sale_amt)<> 0
order by a.yymm
        """
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('SALE_AMT', 0) or 0) for r in records)
        unique_months = len(set(r.get('YYYYMM', '') for r in records if r.get('YYYYMM')))
        unique_items = len(set(r.get('ITEM_STD', '') for r in records if r.get('ITEM_STD')))
        
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000:.0f}k)")
        print(f"분석 월 수: {unique_months}개월")
        print(f"아이템 구분 수: {unique_items}개")
        
        # 데이터 가공: 시즌별/카테고리별로 분류
        item_data = {}
        for r in records:
            item_std = r.get('ITEM_STD', '미지정')
            yyyymm = r.get('YYYYMM', '')
            sale_amt = float(r.get('SALE_AMT', 0) or 0)
            
            if item_std not in item_data:
                item_data[item_std] = {
                    'total_sales': 0,
                    'months': {}
                }
            
            item_data[item_std]['total_sales'] += sale_amt
            if yyyymm not in item_data[item_std]['months']:
                item_data[item_std]['months'][yyyymm] = 0
            item_data[item_std]['months'][yyyymm] += sale_amt
        
        # 시즌별 아이템 분류 (의류)
        season_items = []
        # 카테고리별 아이템 분류 (ACC)
        category_items = []
        
        for item_std, data in item_data.items():
            if '의류' in item_std:
                # 시즌별 의류 분류
                season_items.append({
                    'name': item_std,
                    'total_sales': round(data['total_sales'] / 1000, 0),  # k 단위
                    'months': {k: round(v / 1000, 0) for k, v in sorted(data['months'].items())}  # k 단위
                })
            elif item_std in ['모자', '신발', '가방', '기타']:
                # 카테고리별 ACC 분류
                category_items.append({
                    'name': item_std,
                    'total_sales': round(data['total_sales'] / 1000, 0),  # k 단위
                    'months': {k: round(v / 1000, 0) for k, v in sorted(data['months'].items())}  # k 단위
                })
        
        # 시즌별/카테고리별 정렬 (매출액 기준 내림차순)
        season_items.sort(key=lambda x: x['total_sales'], reverse=True)
        category_items.sort(key=lambda x: x['total_sales'], reverse=True)
        
        # 월별 총 매출 계산
        monthly_totals = {}
        for r in records:
            yyyymm = r.get('YYYYMM', '')
            sale_amt = float(r.get('SALE_AMT', 0) or 0)
            if yyyymm not in monthly_totals:
                monthly_totals[yyyymm] = 0
            monthly_totals[yyyymm] += sale_amt
        
        monthly_totals_k = {k: round(v / 1000, 0) for k, v in sorted(monthly_totals.items())}
        
        # LLM 분석 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 상품 기획 전문가야. 월별 아이템별 매출 추세 분석을 수행해줘.

**분석 기간**: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 ({yyyymm_start}~{yyyymm_end}) (최근 12개월)

**월별 총 매출 추이** (모든 금액은 k 단위):
{json_dumps_safe(monthly_totals_k, ensure_ascii=False, indent=2)}

**시즌별 의류 매출** (모든 금액은 k 단위):
{json_dumps_safe(season_items, ensure_ascii=False, indent=2)}

**카테고리별 ACC 매출** (모든 금액은 k 단위):
{json_dumps_safe(category_items, ensure_ascii=False, indent=2)}

<요구사항>
{get_common_json_requirement()}

{{
  "title": "월별 아이템별 매출 추세 분석",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "시즌 트렌드",
      "ai_text": "시즌별 의류(당시즌, 전시즌, 차기시즌, 과시즌 등)의 월별 매출 추이를 분석한 내용. 각 시즌별 특징과 변화 추이를 명확히 분석해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "카테고리",
      "ai_text": "카테고리별 ACC(모자, 신발, 가방, 기타)의 월별 매출 추이를 분석한 내용. 각 카테고리별 특징과 변화 추이를 명확히 분석해줘."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "핵심 액션",
      "ai_text": "위 분석을 종합하여 핵심 액션 인사이트를 제시해줘."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 시즌별 의류의 월별 추이와 특징을 분석
- 카테고리별 ACC의 월별 추이와 특징을 분석
- 상품 기획 및 재고 관리 관점에서의 액션 아이템 제시

{get_common_prompt_footer()}
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱
        analysis_data = parse_llm_json_response(analysis_response, "월별 아이템별 매출 추세 분석")
        # 기본 구조 보완
        if len(analysis_data.get('sections', [])) < 3:
            analysis_data['sections'].extend([
                {"div": "종합분석-2", "sub_title": "카테고리", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "핵심 액션", "ai_text": ""}
            ])
        
        # JSON 데이터 구성
        json_data = {
            'country': 'CN',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm_end,  # 당해 당월 (현재 날짜 기준)
            'yyyymm_py': yyyymm_py,
            'key': '월별아이템별매출추세',
            'analysis_data': {
                'title': analysis_data.get('title', '아이템별 매출 종합분석 (당해 1월~현재월)'),
                'sections': analysis_data.get('sections', [])
            },
            'summary': {
                'total_sales': round(total_sales / 1000, 0),
                'unique_months': unique_months,
                'unique_items': unique_items,
                'analysis_period': f"{yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 (최근 12개월)"
            },
            'monthly_totals': monthly_totals_k,
            'season_items': season_items,
            'category_items': category_items,
            'raw_data': {
                'sample_records': [dict(r) for r in records[:50]],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]  # 202510 -> 2510
        filename = f"CN_{yyyymm_short}_{brd_cd}_월별아이템별매출추세"
        save_json(json_data, filename)
        
        # Markdown도 저장
        markdown_content = f"# {json_data['analysis_data'].get('title', '월별 아이템별 매출 추세 분석')}\n\n"
        for section in json_data['analysis_data'].get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 월별 아이템별 매출 추세 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_monthly_item_stock_trend(yyyymm, brd_cd):
    """월별 아이템별 재고 추세 분석"""
    print(f"\n{'='*60}")
    print(f"월별 아이템별 재고 추세 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (최근 12개월)
        # 함수 파라미터 yyyymm을 기준으로 최근 12개월 계산
        analysis_year = int(yyyymm[:4])
        analysis_month = int(yyyymm[4:6])
        
        # 최근 12개월 계산 (yyyymm 포함하여 12개월 전까지)
        # 예: 202601 -> 202502부터 202601까지
        if analysis_month == 12:
            start_year = analysis_year - 1
            start_month = 1
        else:
            start_year = analysis_year - 1
            start_month = analysis_month + 1
        
        yyyymm_start = f"{start_year:04d}{start_month:02d}"
        yyyymm_end = yyyymm  # 함수 파라미터로 지정한 연월
        
        previous_year = analysis_year - 1
        yyyymm_py = f"{previous_year:04d}{analysis_month:02d}"
        
        # 프롬프트에서 사용할 변수 정의
        current_year = analysis_year
        current_month = analysis_month
        
        print(f"분석 기간: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 (최근 12개월)")
        
        # SQL 쿼리 실행
        sql = f"""
WITH
    -- PARAM :
    PARAM AS ( SELECT 'CY' AS DIV, '{yyyymm_start}' AS STD_START_YYYYMM, '{yyyymm_end}' AS STD_END_YYYYMM -- start, end 기준년월 지정 필요
               -- UNION ALL
               -- SELECT 'PY' AS DIV, '202401' AS STD_START_YYYYMM, '202411' AS STD_END_YYYYMM
               )
    -- CY_ITEM : 당해 아이템 구분 기준
  , CY_ITEM AS ( SELECT A.PRDT_CD
                      , A.SESN
                      , A.PRDT_HRRC1_NM
                      , A.PRDT_HRRC2_NM
                      , A.PRDT_HRRC3_NM
                      , CASE
    --------------------------------------------------
    -- ACC 분류
    --------------------------------------------------
    -- 주의사항 : PRDT_HRRC2_NM => 첫번째 문자만 대문자고 나머지는 소문자 ..
        WHEN A.PRDT_HRRC1_NM = 'ACC' AND UPPER(A.PRDT_HRRC2_NM) = 'HEADWEAR'
            THEN '모자'
        WHEN A.PRDT_HRRC1_NM = 'ACC' AND UPPER(A.PRDT_HRRC2_NM) = 'SHOES'
            THEN '신발'
        WHEN A.PRDT_HRRC1_NM = 'ACC' AND UPPER(A.PRDT_HRRC2_NM) = 'BAG'
            THEN '가방'
        WHEN A.PRDT_HRRC1_NM = 'ACC' AND UPPER(A.PRDT_HRRC2_NM) = 'ACC_ETC'
            THEN '기타'
        --------------------------------------------------
        -- 의류 분류
        --------------------------------------------------
        -- 당시즌 (SN 통합)
        WHEN A.PRDT_HRRC1_NM = '의류' AND PARAM.STD_END_YYYYMM BETWEEN B.START_YYYYMM AND B.END_YYYYMM
            THEN REPLACE(A.SESN, 'N', 'S') || ' ' || A.PRDT_HRRC1_NM
        --------------------------------------------------
        -- 전시즌 (조회기준 월이 9~2일때만 존재)
        WHEN A.PRDT_HRRC1_NM = '의류' AND RIGHT(PARAM.STD_END_YYYYMM, 2)::INT IN ( 9, 10, 11, 12, 1, 2 ) AND
             TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -6),
                     'YYYYMM') BETWEEN B.START_YYYYMM AND B.END_YYYYMM
            THEN A.SESN || ' ' || A.PRDT_HRRC1_NM
        --------------------------------------------------
        -- 차기시즌
        WHEN A.PRDT_HRRC1_NM = '의류' AND B.START_YYYYMM > PARAM.STD_END_YYYYMM
            THEN '차기시즌 ' || A.PRDT_HRRC1_NM
        --------------------------------------------------
        -- 전년 SF 시즌
        --------------------------------------------------
        -- 조회기준 월이 3~8월일떄
        WHEN A.PRDT_HRRC1_NM = '의류' AND RIGHT(PARAM.STD_END_YYYYMM, 2)::INT IN ( 3, 4, 5, 6, 7, 8 ) AND
             (TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -6),
                      'YYYYMM') BETWEEN B.START_YYYYMM AND B.END_YYYYMM OR
              TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -12),
                      'YYYYMM') BETWEEN B.START_YYYYMM AND B.END_YYYYMM)
            THEN LEFT(A.SESN, 2) || 'SF ' || A.PRDT_HRRC1_NM
        --------------------------------------------------
        -- 조회기준 월이 9~2월일떄
        WHEN A.PRDT_HRRC1_NM = '의류' AND RIGHT(PARAM.STD_END_YYYYMM, 2)::INT IN ( 9, 10, 11, 12, 1, 2 ) AND
             (TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -12),
                      'YYYYMM') BETWEEN B.START_YYYYMM AND B.END_YYYYMM OR
              TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -18),
                      'YYYYMM') BETWEEN B.START_YYYYMM AND B.END_YYYYMM)
            THEN LEFT(A.SESN, 2) || 'SF ' || A.PRDT_HRRC1_NM
        --------------------------------------------------
        -- 과시즌
        --------------------------------------------------
        -- 조회기준 월이 3~8월일떄
        WHEN A.PRDT_HRRC1_NM = '의류' AND RIGHT(PARAM.STD_END_YYYYMM, 2)::INT IN ( 3, 4, 5, 6, 7, 8 ) AND
             B.END_YYYYMM < TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -12), 'YYYYMM')
            THEN '과시즌 ' || A.PRDT_HRRC1_NM
        --------------------------------------------------
        -- 조회기준 월이 9~2월일떄
        WHEN A.PRDT_HRRC1_NM = '의류' AND RIGHT(PARAM.STD_END_YYYYMM, 2)::INT IN ( 9, 10, 11, 12, 1, 2 ) AND
             B.END_YYYYMM < TO_CHAR(ADD_MONTHS(TO_DATE(PARAM.STD_END_YYYYMM, 'YYYYMM'), -18), 'YYYYMM')
            THEN '과시즌 ' || A.PRDT_HRRC1_NM
        ELSE '미지정'
                        END AS ITEM_STD
                 FROM SAP_FNF.MST_PRDT A
                     LEFT JOIN COMM.MST_SESN B
                             ON A.SESN = B.SESN
                     JOIN PARAM
                             ON PARAM.DIV = 'CY'
                 WHERE 1 = 1
                   AND A.SESN <> 'X' -- 저장품 제외
                 )
-- STOCK : 재고
    -- OR => SAP / FR => BOS
  , STOCK AS (
        SELECT YYYYMM, BRD_CD, ITEM_STD, SUM(STOCK_TAG_AMT_EXPECTED) AS STOCK_TAG_AMT_EXPECTED
            FROM ( SELECT A.YYMM                      AS YYYYMM
                        , A.BRD_CD                    AS BRD_CD
                        , D.ITEM_STD                  AS ITEM_STD
                        , SUM(STOCK_TAG_AMT_EXPECTED) AS STOCK_TAG_AMT_EXPECTED
                   FROM CHN.DW_STOCK_M A
                       JOIN CHN.DW_SHOP_WH_DETAIL B
                               ON A.SHOP_ID = B.OA_MAP_SHOP_ID AND B.FR_OR_CLS = 'FR' -- 대리상만
                       JOIN CY_ITEM D
                               ON A.PRDT_CD = D.PRDT_CD
                       JOIN PARAM P
                               ON P.DIV = 'CY' AND A.YYMM BETWEEN P.STD_START_YYYYMM AND P.STD_END_YYYYMM
                   WHERE 1 = 1
                     AND A.BRD_CD = '{brd_cd}' -- 브랜드필터링 필요
                   GROUP BY A.YYMM
                          , A.BRD_CD
                          , D.ITEM_STD
                   UNION ALL
                   SELECT A.YYYYMM               AS YYYYMM
                        , A.BRD_CD               AS BRD_CD
                        , ITEM_STD               AS ITEM_STD
                        , SUM(END_STOCK_TAG_AMT) AS STOCK_TAG_AMT_EXPECTED
                   FROM SAP_FNF.DW_CN_IVTR_PRDT_M A
                       JOIN CY_ITEM D
                               ON A.PRDT_CD = D.PRDT_CD
                       JOIN PARAM P
                               ON P.DIV = 'CY' AND A.YYYYMM BETWEEN P.STD_START_YYYYMM AND P.STD_END_YYYYMM
                   WHERE 1 = 1
                     AND A.BRD_CD = '{brd_cd}' -- 브랜드필터링 필요
                   GROUP BY A.YYYYMM
                          , A.BRD_CD
                          , ITEM_STD )
            GROUP BY YYYYMM, BRD_CD, ITEM_STD
               )
SELECT *
FROM STOCK
order by yyyymm
        """
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_stock = sum(float(r.get('STOCK_TAG_AMT_EXPECTED', 0) or 0) for r in records)
        unique_months = len(set(r.get('YYYYMM', '') for r in records if r.get('YYYYMM')))
        unique_items = len(set(r.get('ITEM_STD', '') for r in records if r.get('ITEM_STD')))
        
        print(f"총 재고액: {total_stock:,.0f}원 ({total_stock/1000:.0f}k)")
        print(f"분석 월 수: {unique_months}개월")
        print(f"아이템 구분 수: {unique_items}개")
        
        # 데이터 가공: 아이템별/월별 재고 집계
        item_stock_data = {}
        monthly_totals = {}
        
        for r in records:
            item_std = r.get('ITEM_STD', '미지정')
            yyyymm = r.get('YYYYMM', '')
            stock_amt = float(r.get('STOCK_TAG_AMT_EXPECTED', 0) or 0)
            
            # 아이템별 재고 집계
            if item_std not in item_stock_data:
                item_stock_data[item_std] = {
                    'total_stock': 0,
                    'months': {}
                }
            item_stock_data[item_std]['total_stock'] += stock_amt
            
            if yyyymm not in item_stock_data[item_std]['months']:
                item_stock_data[item_std]['months'][yyyymm] = 0
            item_stock_data[item_std]['months'][yyyymm] += stock_amt
            
            # 월별 총 재고 집계
            if yyyymm not in monthly_totals:
                monthly_totals[yyyymm] = 0
            monthly_totals[yyyymm] += stock_amt
        
        # k 단위로 변환
        monthly_totals_k = {k: round(v / 1000, 0) for k, v in sorted(monthly_totals.items())}
        
        # 아이템별 재고 데이터 (k 단위)
        item_stock_k = {}
        for item_std, data in item_stock_data.items():
            item_stock_k[item_std] = {
                'total_stock': round(data['total_stock'] / 1000, 0),
                'months': {k: round(v / 1000, 0) for k, v in sorted(data['months'].items())}
            }
        
        # 재고 증가/감소 추세 분석
        stock_trends = {}
        for item_std, data in item_stock_data.items():
            months_sorted = sorted(data['months'].items())
            if len(months_sorted) >= 2:
                first_month_stock = months_sorted[0][1]
                last_month_stock = months_sorted[-1][1]
                change_pct = ((last_month_stock - first_month_stock) / first_month_stock * 100) if first_month_stock > 0 else 0
                
                # 최대/최소 재고
                max_stock = max(v for k, v in data['months'].items())
                min_stock = min(v for k, v in data['months'].items())
                max_month = max(data['months'].items(), key=lambda x: x[1])[0]
                min_month = min(data['months'].items(), key=lambda x: x[1])[0]
                
                stock_trends[item_std] = {
                    'change_pct': round(change_pct, 1),
                    'first_month': months_sorted[0][0],
                    'last_month': months_sorted[-1][0],
                    'first_stock': round(first_month_stock / 1000, 0),
                    'last_stock': round(last_month_stock / 1000, 0),
                    'max_stock': round(max_stock / 1000, 0),
                    'min_stock': round(min_stock / 1000, 0),
                    'max_month': max_month,
                    'min_month': min_month
                }
        
        # LLM 분석 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 재고 관리 전문가야. 월별 아이템별 재고 추세 분석을 수행해줘.

**분석 기간**: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 ({yyyymm_start}~{yyyymm_end}) (최근 12개월)

**월별 총 재고 추이** (모든 금액은 k 단위):
{json_dumps_safe(monthly_totals_k, ensure_ascii=False, indent=2)}

**아이템별 재고 데이터** (모든 금액은 k 단위):
{json_dumps_safe(item_stock_k, ensure_ascii=False, indent=2)}

**아이템별 재고 추세 분석**:
{json_dumps_safe(stock_trends, ensure_ascii=False, indent=2)}

<분석 목표>
1. 조기경보: 재고 증가 추세가 우려되는 아이템, 재고 과다 아이템, 재고 회전율 저하 아이템 등을 분석
2. 긍정신호: 재고 최적화가 잘 되고 있는 아이템, 재고 회전율이 좋은 아이템, 재고 감소 추세가 긍정적인 아이템 등을 분석
3. 핵심액션: 재고 관리 개선을 위한 구체적인 실행 방안 제시

<요구사항>
{get_common_json_requirement()}

{{
  "title": "월별 아이템별 재고 추세 분석",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "조기경보",
      "ai_text": "재고 증가 추세가 우려되는 아이템, 재고 과다 아이템, 재고 회전율 저하 아이템 등을 분석한 내용. 구체적인 아이템명과 수치를 제시하고, 위험 수준을 명확히 분석해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "긍정신호",
      "ai_text": "재고 최적화가 잘 되고 있는 아이템, 재고 회전율이 좋은 아이템, 재고 감소 추세가 긍정적인 아이템 등을 분석한 내용. 구체적인 아이템명과 수치를 제시하고, 긍정적인 요인을 분석해줘."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "핵심액션",
      "ai_text": "재고 관리 개선을 위한 구체적인 실행 방안을 제시한 내용. 조기경보와 긍정신호 분석을 바탕으로 실행 가능한 액션 아이템을 구체적으로 제시해줘."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 조기경보: 위험 수준이 높은 아이템을 우선순위로 분석
- 긍정신호: 잘 관리되고 있는 아이템의 성공 요인 분석
- 핵심액션: 실행 가능한 구체적인 액션 아이템 제시

{get_common_prompt_footer()}
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱
        analysis_data = parse_llm_json_response(analysis_response, "월별 아이템별 재고 추세 분석")
        # 기본 구조 보완
        if len(analysis_data.get('sections', [])) < 3:
            analysis_data['sections'].extend([
                {"div": "종합분석-2", "sub_title": "긍정신호", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "핵심액션", "ai_text": ""}
            ])
        
        # JSON 데이터 구성
        json_data = {
            'country': 'CN',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm_end,  # 당해 당월 (현재 날짜 기준)
            'yyyymm_py': yyyymm_py,
            'key': '월별아이템별재고추세',
            'analysis_data': analysis_data,
            'summary': {
                'total_stock': round(total_stock / 1000, 0),
                'unique_months': unique_months,
                'unique_items': unique_items,
                'analysis_period': f"{yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 (최근 12개월)"
            },
            'monthly_totals': monthly_totals_k,
            'item_stock_data': item_stock_k,
            'stock_trends': stock_trends,
            'raw_data': {
                'sample_records': [dict(r) for r in records[:50]],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]  # 202510 -> 2510
        filename = f"CN_{yyyymm_short}_{brd_cd}_월별아이템별재고추세"
        save_json(json_data, filename)
        
        # Markdown도 저장
        markdown_content = f"# {json_data['analysis_data'].get('title', '월별 아이템별 재고 추세 분석')}\n\n"
        for section in json_data['analysis_data'].get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 월별 아이템별 재고 추세 분석 완료!\n")
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
    yyyymm_list = generate_yyyymm_list('202602')
    
    # 방법 2: 여러 달 분석 (2024년 1월 ~ 2025년 10월)
    # yyyymm_list = generate_yyyymm_list('202501', '202510')
    
    # 방법 3: 직접 리스트 지정
    # yyyymm_list = ['202509', '202510', '202511']
    
    if len(yyyymm_list) == 1:
        print(f"분석할 기간: {len(yyyymm_list)}개월 ({yyyymm_list[0]})")
    else:
        print(f"분석할 기간: {len(yyyymm_list)}개월 ({yyyymm_list[0]} ~ {yyyymm_list[-1]})")
    
    # 브랜드 선택 (원하는 브랜드만 주석 해제)
    brands_to_analyze = [
        'M',   # MLB
        'I',   # MLB KIDS
        'X',   # DISCOVERY
        'V',   # DUVETICA
        'W',   # SUPRA
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
                analyze_retail_channel_top3_sales(yyyymm, brd_cd)  # 리테일매출 채널별 TOP3 분석 (완료)
                analyze_outbound_category_sales(yyyymm, brd_cd)  # 출고매출 카테고리별 분석 (완료)
                analyze_agent_store_sales(yyyymm, brd_cd)  # 대리상 점당매출 종합분석
                analyze_discount_rate(yyyymm, brd_cd)  # 할인율 종합분석 (완료)
                analyze_operating_expense(yyyymm, brd_cd)  # 영업비 종합분석 (완료)
                analyze_monthly_channel_sales_trend(yyyymm, brd_cd)  # 월별 채널별 매출 추세 분석 (완료)
                analyze_monthly_item_sales_trend(yyyymm, brd_cd)  # 월별 아이템별 매출 추세 분석
                analyze_monthly_item_stock_trend(yyyymm, brd_cd)  # 월별 아이템별 재고 추세 분석 (완료)
                pass  # 주석 처리된 함수가 없을 경우를 위한 pass
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


