"""
대만(TW)용 분석 도구 - hkmc_analysis와 동일 로직, TWD→HKD 환율 적용
- SQL 쿼리 실행 (CNTRY_CD='TW', comm.hst_exchn_rate로 HKD 환산)
- LLM 호출 (Claude)
- Markdown/JSON 파일 저장 (tw_output)
"""

import os
import json
import polars as pl
import anthropic
from datetime import datetime, timedelta
from decimal import Decimal
from calendar import monthrange
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
    'X': 'DISCOVERY'
}

OUTPUT_JSON_PATH = './tw_output/json'
OUTPUT_MD_PATH = './tw_output/md'

# 출력 폴더 생성
os.makedirs(OUTPUT_JSON_PATH, exist_ok=True)
os.makedirs(OUTPUT_MD_PATH, exist_ok=True)

# ============================================================================
# DB 연결
# ============================================================================
def get_db_engine():
    """Snowflake DB 연결 엔진 생성"""
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    authenticator = os.getenv('SNOWFLAKE_AUTHENTICATOR')
    database = os.getenv('SNOWFLAKE_DATABASE')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    
    if not all([account, user, database, warehouse, authenticator]):
        raise ValueError("Snowflake 환경 변수가 설정되지 않았습니다. .env 파일을 확인하세요.")
    
    return create_engine(
        URL(
            account=account,
            user=user,
            authenticator=authenticator,
            database=database,
            warehouse=warehouse,
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
- 모든 금액은 K(천) 단위로 표시 (원본 데이터를 1,000으로 나누어 표기)
- 단위는 K, 3자리마다 쉼표 표기
- ⚠️ **중요: 천 단위 표시 시 반드시 정수로 표기하고 소수점을 사용하지 말 것**
  - 올바른 예: 1,234 K, 588 K, 1,378 K
  - 잘못된 예: 1,234.56 K, 588.67 K, 1,378.0 K (절대 사용 금지)
  - 소수점이 있는 경우 반올림하여 정수로 표기 (예: 588.67 → 589 K, 1,378.0 → 1,378 K)
- 비중(%)은 소수점 첫째자리까지 표현
- 매출액은 act_sale_amt 컬럼을 사용하며, 대만은 VAT+/VAT- 개념이 있으므로 매출을 말할 때 반드시 (V+) 여부를 붙여야 함. 예: "매출액(V+)", "실판가(V+)", "총 매출(V+)" 등으로 표기.

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
# 공통 프롬프트 템플릿
# ============================================================================
def get_common_prompt_guidelines():
    """공통 프롬프트 가이드라인 텍스트 반환 (대만 전용)"""
    return """- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 천 단위(K)로 표시하고 절대 변형하지 말 것
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시 (예: "첫 번째 줄\\n두 번째 줄")
- ai_text 내에서 여러 문단이나 항목을 나눌 때는 \\n\\n을 사용
- 불릿 포인트나 리스트 항목 사이에는 \\n을 사용
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)
- 대만은 VAT+/VAT- 개념이 있으므로 매출을 말할 때 반드시 (V+) 여부를 붙여야 함. 예: "매출액(V+)", "실판가(V+)", "총 매출(V+)" 등으로 표기."""

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

def extract_key_from_filename(filename):
    """
    파일명에서 KEY와 sub_key를 추출 (브랜드 코드 제외)
    
    파일명 형식: HKMC_{yyyymm_short}_{brd_cd}_{분석타입}_{세부분석}
    예시: HKMC_2509_M_실판매출_채널별매출분석
    
    Returns:
        tuple: (key, sub_key, country)
        - key: 분석타입만 (예: 실판매출)
        - sub_key: 세부분석만 (예: 채널별매출분석)
        - country: HKMC
    """
    # HKMC_2509_M_실판매출_채널별매출분석 형식
    parts = filename.split('_')
    if len(parts) < 4:
        return None, None, 'TW'
    
    # TW 제거하고 나머지 부분 사용 (파일명 형식: TW_2509_M_실판매출_채널별매출분석)
    if parts[0] == 'TW':
        parts = parts[1:]  # ['2509', 'M', '실판매출', '채널별매출분석']
    
    if len(parts) < 3:
        return None, None, 'TW'
    
    # yyyymm_short, brd_cd, 나머지
    analysis_parts = parts[2:]  # ['실판매출', '채널별매출분석']
    
    if len(analysis_parts) == 0:
        return None, None, 'TW'
    
    # KEY: 첫번째 분석타입만 (브랜드 코드 제외)
    key = analysis_parts[0]  # '실판매출'
    
    # sub_key: 두번째부터 끝까지 (브랜드 코드 제외)
    if len(analysis_parts) > 1:
        sub_key = '_'.join(analysis_parts[1:])  # '채널별매출분석'
    else:
        sub_key = None
    
    return key, sub_key, 'TW'

def save_json(data, filename):
    """
    JSON 파일 저장 - 필드 순서: country, brand_cd, brand_name, yyyymm, yyyymm_py, key, sub_key, analysis_data, ...
    
    ⚠️ 중요: 모든 JSON 출력은 다음 구조를 반드시 포함해야 함:
    {
      "country": "TW",
      "brand_cd": "M",
      "brand_name": "MLB",
      "yyyymm": "202512",
      "yyyymm_py": "202412",
      "key": "실판매출",
      "sub_key": "채널별매출분석",
      "analysis_data": { ... },
      ...
    }
    """
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
        
        data = new_data
    
    file_path = os.path.join(OUTPUT_JSON_PATH, f"{filename}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, cls=DecimalEncoder)
    print(f"[OK] JSON 저장: {file_path}")
    return file_path

# ============================================================================
# 분석 함수들
# ============================================================================

def get_channel_sales_query(yyyymm, brd_cd):
    """
    채널별 매출 분석 쿼리 (대만 TW) - TWD→HKD 환율 적용
    DM_HMD_IVTR_SHOP_PRDT_M + MST_HMD_SHOP + comm.hst_exchn_rate
    """
    brd_filter = f"(a.BRD_CD = '{brd_cd}' OR ('{brd_cd}' = 'M' AND a.BRD_CD = 'I'))"
    return f"""
select YYYYMM,
       a.CNTRY_CD,
       DECODE(a.BRD_CD, 'I', 'M', a.BRD_CD) AS BRD_CD,
       mgmt_chnl_nm,
       a.LOCAL_SHOP_CD,
       B.SHOP_NM,
       'HKD' AS CURRENCY,
       SUM(SALE_QTY) AS SALE_QTY,
       SUM(TAG_SALE_AMT * x.exchn_rate) AS TAG_SALE_AMT,
       SUM(ACT_SALE_AMT * x.exchn_rate) AS ACT_SALE_AMT
from SAP_FNF.DM_HMD_IVTR_SHOP_PRDT_M a
join SAP_FNF.MST_HMD_SHOP B on A.LOCAL_SHOP_CD = B.LOCAL_SHOP_CD AND A.BRD_CD = B.BRD_CD AND B.TYPE_NM = 'SHOP'
join comm.hst_exchn_rate x on a.yyyymm between x.efct_start_yyyymm and x.efct_end_yyyymm and x.target_crncy = 'HKD' and x.source_crncy = a.currency
where 1=1
  AND YYYYMM = '{yyyymm}'
  AND a.CNTRY_CD in ('TW')
  AND MGMT_CHNL_NM <> '미지정'
  AND {brd_filter}
GROUP BY YYYYMM, a.CNTRY_CD, DECODE(a.BRD_CD, 'I', 'M', a.BRD_CD), mgmt_chnl_nm, a.LOCAL_SHOP_CD, B.SHOP_NM
ORDER BY YYYYMM, CNTRY_CD, BRD_CD, MGMT_CHNL_NM
"""

# ---------------------------------------------------------------------------
# 매장별 직접이익 정의 (직접이익 구할 때 무조건 아래 식 사용)
# 직접이익 = 실판가(V-) - 매출원가 - 직접비
#   - 실판가(V-): SAP_FNF.DM_HMD_IVTR_SHOP_PRDT_M.VAT_EXC_ACT_SALE_AMT (get_store_sales_query)
#   - 매출원가: SAP_FNF.DM_HMD_CST_SHOP_M, CST_TYPE = 'COGS' (get_store_cogs_query)
#   - 직접비:   SAP_FNF.DM_HMD_CST_SHOP_M, CST_TYPE = 'DCST' (get_store_direct_cost_query)
# ---------------------------------------------------------------------------
def get_store_cogs_query(yyyymm, brd_cd):
    """매장별 매출원가 쿼리 (대만 TW, TWD→HKD 환율 적용)"""
    if brd_cd == 'M':
        brd_filter = "A.BRD_CD in ('M', 'I')"
    else:
        brd_filter = f"A.BRD_CD = '{brd_cd}'"
    return f"""
SELECT a.LOCAL_SHOP_CD,
       a.CTGR1,
       a.CTGR2,
       a.CTGR3,
       SUM(a.AMT * x.exchn_rate) AS AMT
FROM sap_fnf.DM_HMD_CST_SHOP_M A
LEFT JOIN SAP_FNF.MST_HMD_SHOP B ON A.LOCAL_SHOP_CD = B.LOCAL_SHOP_CD AND A.BRD_CD = B.BRD_CD AND B.TYPE_NM IN ('SHOP')
JOIN comm.hst_exchn_rate x ON a.yyyymm BETWEEN x.efct_start_yyyymm AND x.efct_end_yyyymm AND x.target_crncy = 'HKD' AND x.source_crncy = a.currency
WHERE a.YYYYMM = '{yyyymm}'
  AND a.CNTRY_CD in ('TW')
  AND a.CST_TYPE = 'COGS'
  AND a.CTGR1 = '매출원가'
  AND {brd_filter}
GROUP BY a.LOCAL_SHOP_CD, a.CTGR1, a.CTGR2, a.CTGR3
ORDER BY a.LOCAL_SHOP_CD, a.CTGR1, a.CTGR2, a.CTGR3
"""

def get_store_direct_cost_query(yyyymm, brd_cd):
    """매장별 직접비용 쿼리 (대만 TW, TWD→HKD 환율 적용)"""
    if brd_cd == 'M':
        brd_filter = "A.BRD_CD in ('M', 'I')"
    else:
        brd_filter = f"A.BRD_CD = '{brd_cd}'"
    return f"""
SELECT a.LOCAL_SHOP_CD,
       a.CTGR1,
       a.CTGR2,
       a.CTGR3,
       SUM(a.AMT * x.exchn_rate) AS AMT
FROM sap_fnf.DM_HMD_CST_SHOP_M A
LEFT JOIN SAP_FNF.MST_HMD_SHOP B ON A.LOCAL_SHOP_CD = B.LOCAL_SHOP_CD AND A.BRD_CD = B.BRD_CD AND B.TYPE_NM IN ('SHOP')
JOIN comm.hst_exchn_rate x ON a.yyyymm BETWEEN x.efct_start_yyyymm AND x.efct_end_yyyymm AND x.target_crncy = 'HKD' AND x.source_crncy = a.currency
WHERE a.YYYYMM = '{yyyymm}'
  AND a.CNTRY_CD in ('TW')
  AND a.CST_TYPE = 'DCST'
  AND a.CTGR1 IN ('급여', '임차료', '물류비', '기타직접비')
  AND {brd_filter}
GROUP BY a.LOCAL_SHOP_CD, a.CTGR1, a.CTGR2, a.CTGR3
ORDER BY a.LOCAL_SHOP_CD, a.CTGR1, a.CTGR2, a.CTGR3
"""

def get_store_profit_query(yyyymm, brd_cd):
    """매장별 직접이익 분석 쿼리 (대만 TW, TWD→HKD 환율 적용)
    
    Returns:
        SQL query that includes:
        - shop_cd, shop_nm, mgmt_chnl_nm
        - area_sqm_cy, area_sqm_py
        - vat_exc_sale_amt_cy, vat_exc_sale_amt_py (실판가 V-)
        - dcst_cy, dcst_py (직접비)
        - cogs_cy, cogs_py (원가)
        - dcst_cogs_cy, dcst_cogs_py (직접비+원가)
        - dprft, dprft_py (직접이익)
        - yoy (year-over-year percentage)
        - profit_ratio (direct profit rate)
        - shop_div (흑자/성장, 흑자/역성장, 적자/성장, 적자/역성장)
        - shop_area_div (아울렛, 대형정상, 중소형정상)
    """
    # Calculate previous year month
    current_year = int(yyyymm[:4])
    current_month = int(yyyymm[4:6])
    previous_year = current_year - 1
    yyyymm_py = f"{previous_year:04d}{current_month:02d}"
    
    # Brand filter for main query
    if brd_cd == 'M':
        brd_filter_main = "('M', 'I')"
    else:
        brd_filter_main = f"('{brd_cd}')"
    
    return f"""
with param as (
    select 'CY' as div, '{yyyymm}' as std_end_yyyymm
    union all
    select 'PY' as div, '{yyyymm_py}' as std_end_yyyymm
),
exchn_rate as (
    select source_crncy, exchn_rate
    from comm.hst_exchn_rate a
    join param p on p.std_end_yyyymm between a.efct_start_yyyymm and a.efct_end_yyyymm
        and p.div = 'CY'
    where 1=1
        and target_crncy = 'HKD'
    union all
    select 'HKD', 1
),
area_sqm as (
    select p.div,
           a.brd_cd,
           a.local_shop_cd,
           sum(area_sqm)::float area_sqm
    from sap_fnf.prep_hmd_shop_hist_m a
    join param p on a.yyyymm = p.std_end_yyyymm
    group by 1, 2, 3
),
raw as (
    select div,
           CNTRY_CD,
           BRD_CD,
           MGMT_CHNL_NM,
           local_shop_cd,
           shop_nm,
           sum(vat_exc_sale_amt) as vat_exc_sale_amt,
           sum(dcst) as dcst,
           sum(cogs) as cogs
    from (
        select p.div,
               b.cntry_cd,
               a.brd_cd,
               b.mgmt_chnl_nm,
               a.local_shop_cd,
               replace(b.local_shop_nm, a.local_shop_cd || ' ', '') as shop_nm,
               sum(vat_exc_act_sale_amt * x.exchn_rate) vat_exc_sale_amt,
               0 dcst,
               0 cogs
        from sap_fnf.dm_hmd_ivtr_shop_prdt_m a
        left join sap_fnf.mst_hmd_shop b on a.local_shop_cd = b.local_shop_cd
            and a.brd_cd = b.brd_cd
            and b.type_nm = 'SHOP'
        join param p on a.yyyymm = p.std_end_yyyymm
        join exchn_rate x on a.currency = x.source_crncy
        group by p.div, b.cntry_cd, a.brd_cd, b.mgmt_chnl_nm, a.local_shop_cd, b.local_shop_nm
        union all
        select p.div,
               b.cntry_cd,
               a.brd_cd,
               b.mgmt_chnl_nm,
               a.local_shop_cd,
               replace(b.local_shop_nm, a.local_shop_cd || ' ', '') as shop_nm,
               0 vat_exc_sale_amt,
               sum(case when cst_type = 'DCST' then amt * x.exchn_rate else 0 end) dcst,
               sum(case when cst_type = 'COGS' then amt * x.exchn_rate else 0 end) cogs
        from sap_fnf.dm_hmd_cst_shop_m a
        left join sap_fnf.mst_hmd_shop b on a.local_shop_cd = b.local_shop_cd
            and a.brd_cd = b.brd_cd
            and b.type_nm = 'SHOP'
        join param p on a.yyyymm = p.std_end_yyyymm
        join exchn_rate x on a.currency = x.source_crncy
        group by p.div, b.cntry_cd, a.brd_cd, b.mgmt_chnl_nm, a.local_shop_cd, b.local_shop_nm
    )
    group by div, CNTRY_CD, BRD_CD, MGMT_CHNL_NM, local_shop_cd, shop_nm
),
main as (
    select a.cntry_cd,
           a.local_shop_cd shop_cd,
           a.shop_nm,
           a.mgmt_chnl_nm,
           sum(case when a.div = 'CY' then b.area_sqm else 0 end) area_sqm_cy,
           sum(case when a.div = 'PY' then b.area_sqm else 0 end) area_sqm_py,
           sum(case when a.div = 'CY' then a.vat_exc_sale_amt else 0 end) vat_exc_sale_amt_cy,
           sum(case when a.div = 'PY' then a.vat_exc_sale_amt else 0 end) vat_exc_sale_amt_py,
           sum(case when a.div = 'CY' then a.dcst else 0 end) as dcst_cy,
           sum(case when a.div = 'PY' then a.dcst else 0 end) as dcst_py,
           sum(case when a.div = 'CY' then a.cogs else 0 end) as cogs_cy,
           sum(case when a.div = 'PY' then a.cogs else 0 end) as cogs_py
    from raw a
    left join area_sqm b on a.div = b.div
        and a.local_shop_cd = b.local_shop_cd
        and a.brd_cd = b.brd_cd
    where 1=1
        and cntry_cd in ('TW')
        and a.brd_cd in {brd_filter_main}
        and mgmt_chnl_nm <> '온라인'
    group by a.cntry_cd, a.local_shop_cd, a.mgmt_chnl_nm, a.shop_nm
)
select cntry_cd,
       shop_cd,
       shop_nm,
       area_sqm_cy,
       area_sqm_py,
       mgmt_chnl_nm,
       vat_exc_sale_amt_cy,
       vat_exc_sale_amt_py,
       dcst_cy,
       dcst_py,
       cogs_cy,
       cogs_py,
       (dcst_cy + cogs_cy) as dcst_cogs_cy,
       (dcst_py + cogs_py) as dcst_cogs_py,
       (vat_exc_sale_amt_cy - dcst_cogs_cy) as dprft,
       (vat_exc_sale_amt_py - dcst_cogs_py) as dprft_py,
       case when dprft_py = 0 then 0 else round(dprft / dprft_py * 100) end yoy,
       case when vat_exc_sale_amt_cy = 0 then 0 
            else round((vat_exc_sale_amt_cy - dcst_cogs_cy) / vat_exc_sale_amt_cy * 100, 1) end as profit_ratio,
       row_number() over(order by dprft desc) as rank,
       case when dprft > 0 and yoy > 100 then '흑자/성장'
            when dprft > 0 and yoy <= 100 then '흑자/역성장'
            when dprft <= 0 and yoy > 100 then '적자/성장'
            when dprft <= 0 and yoy <= 100 then '적자/역성장'
       end as shop_div,
       case when mgmt_chnl_nm = '아울렛' then '아울렛'
            when area_sqm_cy >= 40 then '대형정상'
            when area_sqm_cy < 40 then '중소형정상'
       end as shop_area_div
from main
order by rank
"""

def analyze_channel_sales(yyyymm, brd_cd):
    """채널별 매출 분석 - 최고성과채널, 개선필요채널, 핵심 제안 도출"""
    print(f"\n{'='*60}")
    print(f"채널별 매출 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # SQL 쿼리 실행 (당해·전년 각각 1회)
        sql_cy = get_channel_sales_query(yyyymm, brd_cd)
        sql_py = get_channel_sales_query(yyyymm_py, brd_cd)
        df_cy = run_query(sql_cy, engine)
        df_py = run_query(sql_py, engine)
        records_cy = df_cy.to_dicts()
        records_py = df_py.to_dicts()
        
        def aggregate_by_channel(records):
            """채널별 대만(TW) 집계 (MGMT_CHNL_NM 기준)"""
            by_chnl = {}
            for r in records:
                chnl_nm = r.get('MGMT_CHNL_NM', '기타')
                amt = float(r.get('ACT_SALE_AMT', 0) or 0)
                if chnl_nm not in by_chnl:
                    by_chnl[chnl_nm] = {'tw_sale_amt': 0, 'total_sale_amt': 0}
                by_chnl[chnl_nm]['total_sale_amt'] += amt
                by_chnl[chnl_nm]['tw_sale_amt'] += amt
            return by_chnl
        
        cy_data = aggregate_by_channel(records_cy)
        py_data = aggregate_by_channel(records_py)
        
        if not cy_data and not py_data:
            print("데이터가 없습니다.")
            return None
        
        # 채널별 요약 데이터 생성 (YOY 계산 포함)
        channel_summary = []
        all_channels = set(list(cy_data.keys()) + list(py_data.keys()))
        
        for chnl_nm in all_channels:
            cy = cy_data.get(chnl_nm, {'tw_sale_amt': 0, 'total_sale_amt': 0})
            py = py_data.get(chnl_nm, {'tw_sale_amt': 0, 'total_sale_amt': 0})
            total_yoy = round((cy['total_sale_amt'] / py['total_sale_amt'] * 100) if py['total_sale_amt'] != 0 else 0, 1)
            channel_summary.append({
                'chnl_nm': chnl_nm,
                'tw_sale_amt_cy_k': round(cy['tw_sale_amt'] / 1000, 0),
                'total_sale_amt_cy_k': round(cy['total_sale_amt'] / 1000, 0),
                'tw_sale_amt_py_k': round(py['tw_sale_amt'] / 1000, 0),
                'total_sale_amt_py_k': round(py['total_sale_amt'] / 1000, 0),
                'total_yoy': total_yoy
            })
        
        # 데이터 요약
        total_sales_cy = sum(cy['total_sale_amt'] for cy in cy_data.values())
        unique_channels = len(all_channels)
        
        print(f"총 매출액(V+) (당해): {total_sales_cy:,.0f} HKD ({total_sales_cy/1000:.0f} K)")
        print(f"채널 수: {unique_channels}개")
        
        # 채널별 정렬 (총 매출 기준 내림차순)
        channel_summary_sorted = sorted(channel_summary, key=lambda x: x['total_sale_amt_cy_k'], reverse=True)
        
        # LLM 분석 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 대만 채널 전략 전문가야. 채널별 매출 분석을 수행해줘.

**분석 기간**: {current_year}년 {current_month}월 (당해) vs {previous_year}년 {current_month}월 (전년)

**채널별 매출 데이터** (모든 금액은 K 단위, 천 단위):
{json_dumps_safe(channel_summary_sorted, ensure_ascii=False, indent=2)}

<분석 목표>
1. 최고성과채널: 매출(V+) 규모와 성장률을 종합적으로 고려하여 최고 성과를 보이는 채널 분석
2. 개선필요채널: 매출(V+) 규모가 작거나 전년 대비 감소한 채널 분석 및 문제점 도출
3. 핵심 제안: 채널별 최적화 방안과 실행 가능한 구체적인 액션플랜 제시

<요구사항>
{get_common_json_requirement()}

{{
  "title": "채널별 매출 분석",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "최고성과채널",
      "ai_text": "매출(V+) 규모와 성장률을 종합적으로 고려하여 최고 성과를 보이는 채널을 분석한 내용. 구체적인 수치와 함께 성공 요인을 분석해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "개선필요채널",
      "ai_text": "매출(V+) 규모가 작거나 전년 대비 감소한 채널을 분석하고 문제점을 도출한 내용. 구체적인 수치와 함께 개선이 필요한 이유를 분석해줘."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "핵심 제안",
      "ai_text": "채널별 최적화 방안과 실행 가능한 구체적인 액션플랜을 제시한 내용. 최고성과채널의 성공 요인을 다른 채널에 적용할 수 있는 방안과 개선필요채널의 구체적인 개선 전략을 제시해줘."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 최고성과채널: 매출(V+) 규모, 성장률을 종합적으로 분석
- 개선필요채널: 매출(V+) 감소 원인, 채널별 특성, 시장 환경 등을 분석
- 핵심 제안: 실행 가능한 구체적인 액션플랜과 전략 제시

{get_common_prompt_footer()}
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱
        analysis_data = parse_llm_json_response(analysis_response, "채널별 매출 분석")
        # 기본 구조 보완
        if len(analysis_data.get('sections', [])) < 3:
            analysis_data['sections'].extend([
                {"div": "종합분석-2", "sub_title": "개선필요채널", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "핵심 제안", "ai_text": ""}
            ])
        
        # JSON 데이터 구성
        # ⚠️ 중요: JSON 구조는 항상 다음 순서로 구성되어야 함
        # 1. country, 2. brand_cd, 3. brand_name, 4. yyyymm, 5. yyyymm_py, 6. key, 7. sub_key, 8. analysis_data, ...
        json_data = {
            'country': 'TW',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '실판매출',
            'sub_key': '채널별매출분석',
            'analysis_data': analysis_data,
            'summary': {
                'total_sales_cy_k': round(total_sales_cy / 1000, 0),
                'unique_channels': unique_channels,
                'analysis_period': f"{current_year}년 {current_month:02d}월 vs {previous_year}년 {current_month:02d}월"
            },
            'channel_summary': channel_summary_sorted,
            'raw_data': {
                'sample_records': [dict(r) for r in (records_cy + records_py)[:50]],
                'total_records_count': len(records_cy) + len(records_py)
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]  # 202510 -> 2510
        filename = f"TW_{yyyymm_short}_{brd_cd}_실판매출_채널별매출분석"
        save_json(json_data, filename)
        
        # Markdown도 저장
        markdown_content = f"# {json_data['analysis_data'].get('title', '채널별 매출 분석')}\n\n"
        for section in json_data['analysis_data'].get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 채널별 매출 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def get_store_sales_query(yyyymm, brd_cd):
    """매장별 매출 쿼리 (대만 TW, TWD→HKD 환율 적용)"""
    brd_filter = f"(a.BRD_CD = '{brd_cd}' OR ('{brd_cd}' = 'M' AND a.BRD_CD = 'I'))"
    return f"""
select YYYYMM,
       a.CNTRY_CD,
       DECODE(a.BRD_CD, 'I', 'M', a.BRD_CD) AS BRD_CD,
       mgmt_chnl_nm,
       a.LOCAL_SHOP_CD,
       B.SHOP_NM,
       'HKD' AS CURRENCY,
       SUM(SALE_QTY) AS SALE_QTY,
       SUM(TAG_SALE_AMT * x.exchn_rate) AS TAG_SALE_AMT,
       SUM(VAT_EXC_ACT_SALE_AMT * x.exchn_rate) AS VAT_EXC_ACT_SALE_AMT,
       SUM(ACT_SALE_AMT * x.exchn_rate) AS ACT_SALE_AMT
from SAP_FNF.DM_HMD_IVTR_SHOP_PRDT_M a
join SAP_FNF.MST_HMD_SHOP B on A.LOCAL_SHOP_CD = B.LOCAL_SHOP_CD AND A.BRD_CD = B.BRD_CD AND B.TYPE_NM = 'SHOP'
join comm.hst_exchn_rate x on a.yyyymm between x.efct_start_yyyymm and x.efct_end_yyyymm and x.target_crncy = 'HKD' and x.source_crncy = a.currency
where 1=1
  AND YYYYMM = '{yyyymm}'
  AND a.CNTRY_CD in ('TW')
  AND MGMT_CHNL_NM <> '미지정'
  AND {brd_filter}
GROUP BY YYYYMM, a.CNTRY_CD, DECODE(a.BRD_CD, 'I', 'M', a.BRD_CD), mgmt_chnl_nm, a.LOCAL_SHOP_CD, B.SHOP_NM
ORDER BY YYYYMM, CNTRY_CD, BRD_CD, MGMT_CHNL_NM
"""

def get_store_cost_query(yyyymm, brd_cd):
    """대만 영업비 쿼리 (TWD→HKD 환율 적용, IDCST)"""
    if brd_cd == 'M':
        brd_filter = "A.BRD_CD in ('M', 'I')"
    else:
        brd_filter = f"A.BRD_CD = '{brd_cd}'"
    return f"""
select YYYYMM,
       a.CNTRY_CD,
       DECODE(a.BRD_CD, 'I', 'M', a.BRD_CD) AS BRD_CD,
       mgmt_chnl_nm,
       a.LOCAL_SHOP_CD,
       'HKD' AS CURRENCY,
       CST_TYPE,
       CTGR1,
       CTGR2,
       CTGR3,
       SUM(a.AMT * x.exchn_rate) AS AMT
from sap_fnf.DM_HMD_CST_SHOP_M A
left join SAP_FNF.MST_HMD_SHOP B on A.LOCAL_SHOP_CD = B.LOCAL_SHOP_CD AND A.BRD_CD = B.BRD_CD AND B.TYPE_NM IN ('SHOP', 'OFFICE')
join comm.hst_exchn_rate x on a.yyyymm between x.efct_start_yyyymm and x.efct_end_yyyymm and x.target_crncy = 'HKD' and x.source_crncy = a.currency
where 1=1
  AND YYYYMM = '{yyyymm}'
  AND a.CNTRY_CD in ('TW')
  AND CST_TYPE = 'IDCST'
  AND {brd_filter}
GROUP BY YYYYMM, A.CNTRY_CD, DECODE(A.BRD_CD, 'I', 'M', A.BRD_CD), mgmt_chnl_nm, a.LOCAL_SHOP_CD, CTGR1, CTGR2, CTGR3, CST_TYPE
ORDER BY CNTRY_CD, BRD_CD, CTGR1, CTGR2, CTGR3
"""

def analyze_store_profit(yyyymm, brd_cd):
    """영업이익_매장별직접이익 분석"""
    print(f"\n{'='*60}")
    print(f"매장별 직접이익 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # 새로운 통합 쿼리 사용
        profit_sql = get_store_profit_query(yyyymm, brd_cd)
        print(f"[DEBUG] 실행할 쿼리 (일부):\n{profit_sql[:500]}...")
        profit_df = run_query(profit_sql, engine)
        print(f"[DEBUG] 쿼리 실행 완료. DataFrame shape: {profit_df.shape if hasattr(profit_df, 'shape') else 'N/A'}")
        print(f"[DEBUG] DataFrame 컬럼명: {profit_df.columns if hasattr(profit_df, 'columns') else 'N/A'}")
        profit_records = profit_df.to_dicts()
        
        if not profit_records:
            print("데이터가 없습니다.")
            return None
        
        # 디버깅: 첫 번째 레코드의 키 확인
        if profit_records:
            print(f"[DEBUG] 쿼리 결과 레코드 수: {len(profit_records)}개")
            print(f"[DEBUG] 첫 번째 레코드의 키: {list(profit_records[0].keys())}")
            sample_record = profit_records[0]
            print(f"[DEBUG] 샘플 데이터 (첫 번째 레코드):")
            for key, value in sample_record.items():
                print(f"  {key}: {value} (type: {type(value).__name__})")
        
        # 쿼리 결과를 store_profit_data로 변환
        # 데이터베이스에서 반환되는 컬럼명은 대문자일 수 있으므로 대소문자 모두 확인하는 헬퍼 함수
        def get_value(record, key, default=0):
            # 소문자, 대문자, 원본 모두 시도
            if key.lower() in record:
                value = record[key.lower()]
            elif key.upper() in record:
                value = record[key.upper()]
            elif key in record:
                value = record[key]
            else:
                value = None
            
            if value is None or value == '':
                return float(default)
            try:
                return float(value)
            except (ValueError, TypeError):
                return float(default)
        
        def get_str_value(record, key, default=''):
            # 소문자, 대문자, 원본 모두 시도
            if key.lower() in record:
                return str(record[key.lower()] or default)
            elif key.upper() in record:
                return str(record[key.upper()] or default)
            elif key in record:
                return str(record[key] or default)
            else:
                return default
        
        store_profit_data = []
        for record in profit_records:
            # 쿼리에서 반환된 값들은 모두 HKD 단위 (원본 값)
            vat_exc_sale_amt_cy = get_value(record, 'vat_exc_sale_amt_cy', 0)
            vat_exc_sale_amt_py = get_value(record, 'vat_exc_sale_amt_py', 0)
            dcst_cy = get_value(record, 'dcst_cy', 0)
            dcst_py = get_value(record, 'dcst_py', 0)
            cogs_cy = get_value(record, 'cogs_cy', 0)
            cogs_py = get_value(record, 'cogs_py', 0)
            direct_profit = get_value(record, 'dprft', 0)  # 직접이익 (HKD)
            direct_profit_py = get_value(record, 'dprft_py', 0)  # 전년 직접이익 (HKD)
            profit_yoy_pct = get_value(record, 'yoy', 0)
            profit_ratio = get_value(record, 'profit_ratio', 0)
            
            # 디버깅: 첫 번째 레코드만 상세 출력
            if len(store_profit_data) == 0:
                print(f"[DEBUG] 첫 번째 매장 데이터:")
                print(f"  매장명: {get_str_value(record, 'shop_nm', 'N/A')}")
                print(f"  실판가(V-) CY: {vat_exc_sale_amt_cy:,.0f}")
                print(f"  직접비 CY: {dcst_cy:,.0f}")
                print(f"  원가 CY: {cogs_cy:,.0f}")
                print(f"  직접이익: {direct_profit:,.0f}")
            
            store_profit_data.append({
                'SHOP_CD': get_str_value(record, 'shop_cd', ''),
                'SHOP_NM': get_str_value(record, 'shop_nm', ''),
                'CNTRY_CD': get_str_value(record, 'cntry_cd', ''),
                'MGMT_CHNL_NM': get_str_value(record, 'mgmt_chnl_nm', ''),
                'AREA_SQM': get_value(record, 'area_sqm_cy', 0),
                'VAT_EXC_SALE_AMT': vat_exc_sale_amt_cy,  # 실판가(V-)
                'VAT_EXC_SALE_AMT_PY': vat_exc_sale_amt_py,
                'DCST': dcst_cy,  # 직접비
                'DCST_PY': dcst_py,
                'COGS': cogs_cy,  # 원가
                'COGS_PY': cogs_py,
                'DIRECT_PROFIT': direct_profit,
                'DIRECT_PROFIT_PY': direct_profit_py,
                'DIRECT_PROFIT_RATE': profit_ratio,
                'PROFIT_YOY_PCT': profit_yoy_pct,
                'SHOP_DIV': get_str_value(record, 'shop_div', ''),
                'SHOP_AREA_DIV': get_str_value(record, 'shop_area_div', ''),
                'RANK': int(get_value(record, 'rank', 0))
            })
        
        # TOP 10 / LOW 10 / IMPROVING / DECLINING 분류
        top_stores = sorted([s for s in store_profit_data if s['DIRECT_PROFIT'] > 0], 
                           key=lambda x: x['DIRECT_PROFIT'], reverse=True)[:10]
        low_stores = sorted([s for s in store_profit_data if s['DIRECT_PROFIT'] <= 0 or s['DIRECT_PROFIT_RATE'] < 5],
                           key=lambda x: x['DIRECT_PROFIT'])[:10]
        improving_stores = sorted([s for s in store_profit_data if s.get('PROFIT_YOY_PCT', 0) > 0],
                                 key=lambda x: x.get('PROFIT_YOY_PCT', 0), reverse=True)[:10]
        declining_stores = sorted([s for s in store_profit_data if s.get('PROFIT_YOY_PCT', 0) < 0],
                                 key=lambda x: x.get('PROFIT_YOY_PCT', 0))[:10]
        
        # K 단위로 변환 (프롬프트용) - DPRFT 사용
        top_stores_formatted = [{
            'SHOP_NM': s['SHOP_NM'],
            'CNTRY_CD': s['CNTRY_CD'],
            'DPRFT': round(s['DIRECT_PROFIT'] / 1000, 0),  # 직접이익 (K 단위)
            'DIRECT_PROFIT_RATE': s['DIRECT_PROFIT_RATE'],
            'PROFIT_YOY_PCT': s['PROFIT_YOY_PCT']
        } for s in top_stores]
        
        low_stores_formatted = [{
            'SHOP_NM': s['SHOP_NM'],
            'CNTRY_CD': s['CNTRY_CD'],
            'DPRFT': round(s['DIRECT_PROFIT'] / 1000, 0),  # 직접이익 (K 단위)
            'DIRECT_PROFIT_RATE': s['DIRECT_PROFIT_RATE'],
            'PROFIT_YOY_PCT': s['PROFIT_YOY_PCT']
        } for s in low_stores]
        
        # 전체 요약
        total_profit = sum(s['DIRECT_PROFIT'] for s in store_profit_data)
        unique_stores = len(store_profit_data)
        avg_profit_rate = round(sum(s['DIRECT_PROFIT_RATE'] for s in store_profit_data) / unique_stores, 2) if unique_stores > 0 else 0
        
        print(f"총 DPRFT: {total_profit:,.0f} HKD ({total_profit/1000:.0f} K)")
        print(f"평균 직접이익률: {avg_profit_rate}%")
        print(f"매장 수: {unique_stores}개")
        
        # LLM 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 대만 매장 수익성 전문가야. 매장별 DPRFT(직접이익) 데이터를 분석하여 DPRFT가 높은 매장의 성공 요인과 낮은 매장의 문제점을 도출하고, 구체적인 개선 방안을 제시해줘.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 총 DPRFT: {round(total_profit/1000, 0):,.0f} K
- 평균 직접이익률: {avg_profit_rate}%
- 분석 매장 수: {unique_stores}개

**DPRFT 높은 매장 TOP 10**
{json_dumps_safe(top_stores_formatted, ensure_ascii=False, indent=2)}

**DPRFT 낮은 매장**
{json_dumps_safe(low_stores_formatted, ensure_ascii=False, indent=2)}

<분석 목표>
1. DPRFT가 높은 매장의 성공 요인 분석
2. DPRFT가 낮은 매장의 문제점 및 전년 대비 변화 분석
3. 채널별 효율성 비교 및 개선 방안

<요구사항>
{get_common_json_requirement()}

{{
  "title": "AI 종합인사이트",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "DPRFT가 높은 매장의 성공 요인 분석",
      "ai_text": "DPRFT TOP 10 매장의 공통 특성, 성공 요인, 비용 구조 효율성을 종합적으로 분석해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "DPRFT가 낮은 매장의 문제점 및 전년 대비 변화 분석",
      "ai_text": "DPRFT가 마이너스이거나 매우 낮은 매장의 공통 문제점, 주요 원인, 전년 대비 악화된 매장의 악화 원인을 종합적으로 분석해줘."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "채널별 효율성 비교 및 개선 방안",
      "ai_text": "대만 지역 매장 효율성 비교, DPRFT가 낮은 매장의 구체적인 개선 방안, 우수 매장 성공 사례 적용 방안을 제시해줘."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 모든 수치는 K 단위 정수로 표기
- DPRFT(직접이익) 계산: DPRFT = 실판가(V-) - 매출원가 - 직접비
- 직접이익률은 실판가(V-) 기준으로 계산됨
- 매장별 DPRFT를 명시적으로 표시할 때는 "DPRFT" 용어를 사용해줘 (예: "Nanfang TS MALL의 DPRFT는 192 K")
- 구체적인 매장명과 수치를 포함하여 작성
- 실행 가능한 구체적인 개선 방안 제시

{get_common_prompt_footer()}
"""
        
        # LLM 호출
        analysis_response = call_llm(prompt, max_tokens=4000)
        analysis_data = parse_llm_json_response(analysis_response, "AI 종합인사이트")
        
        if len(analysis_data.get('sections', [])) < 3:
            analysis_data['sections'].extend([
                {"div": "종합분석-2", "sub_title": "직접이익이 낮은 매장의 문제점 및 전년 대비 변화 분석", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "채널별 효율성 비교 및 개선 방안", "ai_text": ""}
            ])
        
        # JSON 데이터 구성
        json_data = {
            'country': 'TW',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '영업이익',
            'sub_key': '매장별직접이익',
            'analysis_data': analysis_data,
            'summary': {
                'total_dprft_k': round(total_profit / 1000, 0),  # 총 DPRFT (K 단위)
                'avg_profit_rate': avg_profit_rate,
                'unique_stores': unique_stores,
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'store_analysis': {
                'top_stores': top_stores_formatted,  # DPRFT 필드 포함
                'low_stores': low_stores_formatted,  # DPRFT 필드 포함
                'improving_stores': [{'SHOP_NM': s['SHOP_NM'], 'PROFIT_YOY_PCT': s['PROFIT_YOY_PCT']} for s in improving_stores[:10]],
                'declining_stores': [{'SHOP_NM': s['SHOP_NM'], 'PROFIT_YOY_PCT': s['PROFIT_YOY_PCT']} for s in declining_stores[:10]]
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]
        filename = f"TW_{yyyymm_short}_{brd_cd}_영업이익_매장별직접이익"
        save_json(json_data, filename)
        
        # Markdown 저장
        markdown_content = f"# {analysis_data.get('title', 'AI 종합인사이트')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 매장별 직접이익 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_operating_expense(yyyymm, brd_cd):
    """영업비 AI 종합분석"""
    print(f"\n{'='*60}")
    print(f"영업비 AI 종합분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # 당해/전년 매장별 비용 조회
        cost_cy_sql = get_store_cost_query(yyyymm, brd_cd)
        cost_py_sql = get_store_cost_query(yyyymm_py, brd_cd)
        
        cost_cy_df = run_query(cost_cy_sql, engine)
        cost_py_df = run_query(cost_py_sql, engine)
        cost_cy_records = cost_cy_df.to_dicts()
        cost_py_records = cost_py_df.to_dicts()
        
        if not cost_cy_records:
            print("데이터가 없습니다.")
            return None
        
        # CTGR1 > CTGR2 > CTGR3 계층 구조로 집계 (당해)
        ctgr1_summary_cy = {}
        ctgr2_summary_cy = {}
        ctgr3_summary_cy = {}
        
        for record in cost_cy_records:
            ctgr1 = record.get('CTGR1', '기타')
            ctgr2 = record.get('CTGR2', '')
            ctgr3 = record.get('CTGR3', '')
            amt = float(record.get('AMT', 0) or 0)
            mgmt_chnl_nm = record.get('MGMT_CHNL_NM', '')
            
            # CTGR1별 집계
            if ctgr1 not in ctgr1_summary_cy:
                ctgr1_summary_cy[ctgr1] = {
                    'TOTAL_AMT': 0,
                    'CTGR2_LIST': {},
                    'CHNL_COUNT': set()
                }
            ctgr1_summary_cy[ctgr1]['TOTAL_AMT'] += amt
            if mgmt_chnl_nm:
                ctgr1_summary_cy[ctgr1]['CHNL_COUNT'].add(mgmt_chnl_nm)
            
            # CTGR2별 집계 (CTGR1 하위)
            ctgr1_ctgr2_key = f"{ctgr1}|{ctgr2}"
            if ctgr1_ctgr2_key not in ctgr2_summary_cy:
                ctgr2_summary_cy[ctgr1_ctgr2_key] = {
                    'CTGR1': ctgr1,
                    'CTGR2': ctgr2,
                    'TOTAL_AMT': 0,
                    'CTGR3_LIST': {}
                }
                ctgr1_summary_cy[ctgr1]['CTGR2_LIST'][ctgr2] = ctgr2_summary_cy[ctgr1_ctgr2_key]
            ctgr2_summary_cy[ctgr1_ctgr2_key]['TOTAL_AMT'] += amt
            
            # CTGR3별 집계 (CTGR2 하위)
            ctgr1_ctgr2_ctgr3_key = f"{ctgr1}|{ctgr2}|{ctgr3}"
            if ctgr1_ctgr2_ctgr3_key not in ctgr3_summary_cy:
                ctgr3_summary_cy[ctgr1_ctgr2_ctgr3_key] = {
                    'CTGR1': ctgr1,
                    'CTGR2': ctgr2,
                    'CTGR3': ctgr3,
                    'TOTAL_AMT': 0
                }
                ctgr2_summary_cy[ctgr1_ctgr2_key]['CTGR3_LIST'][ctgr3] = ctgr3_summary_cy[ctgr1_ctgr2_ctgr3_key]
            ctgr3_summary_cy[ctgr1_ctgr2_ctgr3_key]['TOTAL_AMT'] += amt
        
        # CTGR1 > CTGR2 > CTGR3 계층 구조로 집계 (전년)
        ctgr1_summary_py = {}
        ctgr2_summary_py = {}
        ctgr3_summary_py = {}
        
        for record in cost_py_records:
            ctgr1 = record.get('CTGR1', '기타')
            ctgr2 = record.get('CTGR2', '')
            ctgr3 = record.get('CTGR3', '')
            amt = float(record.get('AMT', 0) or 0)
            
            # CTGR1별 집계
            if ctgr1 not in ctgr1_summary_py:
                ctgr1_summary_py[ctgr1] = {'TOTAL_AMT': 0, 'CTGR2_LIST': {}}
            ctgr1_summary_py[ctgr1]['TOTAL_AMT'] += amt
            
            # CTGR2별 집계
            ctgr1_ctgr2_key = f"{ctgr1}|{ctgr2}"
            if ctgr1_ctgr2_key not in ctgr2_summary_py:
                ctgr2_summary_py[ctgr1_ctgr2_key] = {
                    'CTGR1': ctgr1,
                    'CTGR2': ctgr2,
                    'TOTAL_AMT': 0,
                    'CTGR3_LIST': {}
                }
                if 'CTGR2_LIST' not in ctgr1_summary_py[ctgr1]:
                    ctgr1_summary_py[ctgr1]['CTGR2_LIST'] = {}
                ctgr1_summary_py[ctgr1]['CTGR2_LIST'][ctgr2] = ctgr2_summary_py[ctgr1_ctgr2_key]
            ctgr2_summary_py[ctgr1_ctgr2_key]['TOTAL_AMT'] += amt
            
            # CTGR3별 집계
            ctgr1_ctgr2_ctgr3_key = f"{ctgr1}|{ctgr2}|{ctgr3}"
            if ctgr1_ctgr2_ctgr3_key not in ctgr3_summary_py:
                ctgr3_summary_py[ctgr1_ctgr2_ctgr3_key] = {
                    'CTGR1': ctgr1,
                    'CTGR2': ctgr2,
                    'CTGR3': ctgr3,
                    'TOTAL_AMT': 0
                }
                ctgr2_summary_py[ctgr1_ctgr2_key]['CTGR3_LIST'][ctgr3] = ctgr3_summary_py[ctgr1_ctgr2_ctgr3_key]
            ctgr3_summary_py[ctgr1_ctgr2_ctgr3_key]['TOTAL_AMT'] += amt
        
        # 계층 구조 데이터 생성 (CTGR1 > CTGR2 > CTGR3)
        def build_hierarchy_data(ctgr1_dict_cy, ctgr1_dict_py):
            hierarchy = []
            for ctgr1, cy_data in sorted(ctgr1_dict_cy.items(), key=lambda x: x[1]['TOTAL_AMT'], reverse=True):
                cy_amt = cy_data['TOTAL_AMT']
                py_amt = ctgr1_dict_py.get(ctgr1, {}).get('TOTAL_AMT', 0)
                yoy_pct = round(((cy_amt - py_amt) / py_amt * 100) if py_amt > 0 else 0, 1)
                
                ctgr2_list = []
                for ctgr2, ctgr2_data in sorted(cy_data.get('CTGR2_LIST', {}).items(), 
                                                 key=lambda x: x[1]['TOTAL_AMT'], reverse=True):
                    ctgr2_cy_amt = ctgr2_data['TOTAL_AMT']
                    ctgr2_py_data = ctgr1_dict_py.get(ctgr1, {}).get('CTGR2_LIST', {}).get(ctgr2, {})
                    ctgr2_py_amt = ctgr2_py_data.get('TOTAL_AMT', 0)
                    ctgr2_yoy_pct = round(((ctgr2_cy_amt - ctgr2_py_amt) / ctgr2_py_amt * 100) if ctgr2_py_amt > 0 else 0, 1)
                    
                    ctgr3_list = []
                    for ctgr3, ctgr3_data in sorted(ctgr2_data.get('CTGR3_LIST', {}).items(),
                                                    key=lambda x: x[1]['TOTAL_AMT'], reverse=True):
                        ctgr3_cy_amt = ctgr3_data['TOTAL_AMT']
                        ctgr3_key = f"{ctgr1}|{ctgr2}|{ctgr3}"
                        ctgr3_py_amt = ctgr3_summary_py.get(ctgr3_key, {}).get('TOTAL_AMT', 0)
                        ctgr3_yoy_pct = round(((ctgr3_cy_amt - ctgr3_py_amt) / ctgr3_py_amt * 100) if ctgr3_py_amt > 0 else 0, 1)
                        
                        ctgr3_list.append({
                            'CTGR3': ctgr3,
                            'AMT_CY_K': round(ctgr3_cy_amt / 1000, 0),
                            'AMT_PY_K': round(ctgr3_py_amt / 1000, 0),
                            'YOY_PCT': ctgr3_yoy_pct
                        })
                    
                    ctgr2_list.append({
                        'CTGR2': ctgr2,
                        'AMT_CY_K': round(ctgr2_cy_amt / 1000, 0),
                        'AMT_PY_K': round(ctgr2_py_amt / 1000, 0),
                        'YOY_PCT': ctgr2_yoy_pct,
                        'CTGR3_LIST': ctgr3_list
                    })
                
                hierarchy.append({
                    'CTGR1': ctgr1,
                    'AMT_CY_K': round(cy_amt / 1000, 0),
                    'AMT_PY_K': round(py_amt / 1000, 0),
                    'YOY_PCT': yoy_pct,
                    'CHNL_COUNT': len(cy_data.get('CHNL_COUNT', set())),
                    'CTGR2_LIST': ctgr2_list
                })
            return hierarchy
        
        hierarchy_data = build_hierarchy_data(ctgr1_summary_cy, ctgr1_summary_py)
        
        # CTGR1별 요약 데이터 (기존 호환성 유지)
        ctgr1_data = []
        for ctgr1, cy_data in ctgr1_summary_cy.items():
            cy_amt = cy_data['TOTAL_AMT']
            py_amt = ctgr1_summary_py.get(ctgr1, {}).get('TOTAL_AMT', 0)
            yoy_pct = round(((cy_amt - py_amt) / py_amt * 100) if py_amt > 0 else 0, 1)
            
            ctgr1_data.append({
                'CTGR1': ctgr1,
                'AMT_CY_K': round(cy_amt / 1000, 0),
                'AMT_PY_K': round(py_amt / 1000, 0),
                'YOY_PCT': yoy_pct,
                'CTGR2_COUNT': len(cy_data.get('CTGR2_LIST', {})),
                'CHNL_COUNT': len(cy_data.get('CHNL_COUNT', set()))
            })
        
        ctgr1_data_sorted = sorted(ctgr1_data, key=lambda x: x['AMT_CY_K'], reverse=True)
        
        # 전체 요약
        total_cost_cy = sum(cy_data['TOTAL_AMT'] for cy_data in ctgr1_summary_cy.values())
        total_cost_py = sum(py_data['TOTAL_AMT'] for py_data in ctgr1_summary_py.values())
        total_yoy_pct = round(((total_cost_cy - total_cost_py) / total_cost_py * 100) if total_cost_py > 0 else 0, 1)
        
        print(f"총 영업비 (당해): {total_cost_cy:,.0f} HKD ({total_cost_cy/1000:.0f} K)")
        print(f"총 영업비 (전년): {total_cost_py:,.0f} HKD ({total_cost_py/1000:.0f} K)")
        print(f"전년대비 변화: {total_yoy_pct:.1f}%")
        print(f"CTGR1 카테고리 수: {len(ctgr1_data)}개")
        
        # LLM 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 대만 영업비 전문가야. 영업비 데이터를 분석하여 비용 구조의 효율성과 개선 방안을 제시해줘.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 총 영업비 (당해): {round(total_cost_cy/1000, 0):,.0f} K
- 총 영업비 (전년): {round(total_cost_py/1000, 0):,.0f} K
- 전년대비 변화: {total_yoy_pct:.1f}%

**CTGR1별 영업비 분석 (요약)**
{json_dumps_safe(ctgr1_data_sorted, ensure_ascii=False, indent=2)}

**CTGR1 > CTGR2 > CTGR3 계층 구조 상세 분석**
{json_dumps_safe(hierarchy_data, ensure_ascii=False, indent=2)}

<분석 목표>
1. 영업비 구조 분석 및 주요 비용 항목 식별 (CTGR1 > CTGR2 > CTGR3 드릴다운 분석)
2. 전년 대비 변화 분석 및 효율성 평가 (각 계층별 변화율 분석)
3. 비용 최적화 방안 제시 (구체적인 CTGR1/CTGR2/CTGR3 항목별 개선 방안)

<요구사항>
{get_common_json_requirement()}

{{
  "title": "AI 종합인사이트",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "영업비 구조 분석",
      "ai_text": "CTGR1 > CTGR2 > CTGR3 계층 구조로 영업비를 드릴다운 분석하고, 각 계층별 주요 비용 항목을 식별해줘. 특히 비용이 큰 CTGR1 항목의 하위 CTGR2, CTGR3까지 상세히 분석해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "전년 대비 변화 분석",
      "ai_text": "CTGR1, CTGR2, CTGR3 각 계층별로 전년 대비 변화율을 분석하고, 증가/감소가 큰 항목들을 구체적으로 지적해줘. 각 계층별 효율성을 평가해줘."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "비용 최적화 방안",
      "ai_text": "CTGR1 > CTGR2 > CTGR3 계층 구조를 고려하여, 구체적인 비용 항목(CTGR1/CTGR2/CTGR3)별로 실행 가능한 비용 최적화 방안을 제시해줘."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 모든 수치는 K 단위 정수로 표기
- CTGR1 > CTGR2 > CTGR3 계층 구조를 활용하여 드릴다운 분석 수행
- 각 계층별로 구체적인 항목명(CTGR1/CTGR2/CTGR3)과 수치를 포함하여 작성
- 비용이 큰 항목부터 우선순위를 두고 분석
- 실행 가능한 구체적인 개선 방안을 CTGR1/CTGR2/CTGR3 항목별로 제시

{get_common_prompt_footer()}
"""
        
        # LLM 호출
        analysis_response = call_llm(prompt, max_tokens=4000)
        analysis_data = parse_llm_json_response(analysis_response, "AI 종합인사이트")
        
        if len(analysis_data.get('sections', [])) < 3:
            analysis_data['sections'].extend([
                {"div": "종합분석-2", "sub_title": "전년 대비 변화 분석", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "비용 최적화 방안", "ai_text": ""}
            ])
        
        # JSON 데이터 구성
        json_data = {
            'country': 'TW',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '영업비',
            'sub_key': 'AI종합분석',
            'analysis_data': analysis_data,
            'summary': {
                'total_cost_cy_k': round(total_cost_cy / 1000, 0),
                'total_cost_py_k': round(total_cost_py / 1000, 0),
                'total_yoy_pct': total_yoy_pct,
                'ctgr1_count': len(ctgr1_data),
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'ctgr1_summary': ctgr1_data_sorted,
            'hierarchy_data': hierarchy_data  # CTGR1 > CTGR2 > CTGR3 계층 구조 데이터
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]
        filename = f"TW_{yyyymm_short}_{brd_cd}_영업비_AI종합분석"
        save_json(json_data, filename)
        
        # Markdown 저장
        markdown_content = f"# {analysis_data.get('title', 'AI 종합인사이트')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 영업비 AI 종합분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_discount_rate_overall(yyyymm, brd_cd):
    """할인율 AI 종합분석"""
    print(f"\n{'='*60}")
    print(f"할인율 AI 종합분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # 당해/전년 매장별 매출 조회
        sales_cy_sql = get_store_sales_query(yyyymm, brd_cd)
        sales_py_sql = get_store_sales_query(yyyymm_py, brd_cd)
        
        sales_cy_df = run_query(sales_cy_sql, engine)
        sales_py_df = run_query(sales_py_sql, engine)
        sales_cy_records = sales_cy_df.to_dicts()
        sales_py_records = sales_py_df.to_dicts()
        
        if not sales_cy_records:
            print("데이터가 없습니다.")
            return None
        
        # 전체 할인율 계산 ((TAG-ACT)/TAG*100 또는 1-ACT/TAG*100)
        total_tag_cy = sum(float(r.get('TAG_SALE_AMT', 0) or 0) for r in sales_cy_records)
        total_act_cy = sum(float(r.get('ACT_SALE_AMT', 0) or 0) for r in sales_cy_records)
        discount_rate_cy = round(((total_tag_cy - total_act_cy) / total_tag_cy * 100) if total_tag_cy > 0 else 0, 1)
        
        total_tag_py = sum(float(r.get('TAG_SALE_AMT', 0) or 0) for r in sales_py_records)
        total_act_py = sum(float(r.get('ACT_SALE_AMT', 0) or 0) for r in sales_py_records)
        discount_rate_py = round(((total_tag_py - total_act_py) / total_tag_py * 100) if total_tag_py > 0 else 0, 1)
        
        # 매장별 할인율 계산 ((TAG-ACT)/TAG*100)
        store_discount_data = []
        for record in sales_cy_records:
            shop_key = record.get('LOCAL_SHOP_CD', '')
            tag_amt = float(record.get('TAG_SALE_AMT', 0) or 0)
            act_amt = float(record.get('ACT_SALE_AMT', 0) or 0)
            discount_rate = round(((tag_amt - act_amt) / tag_amt * 100) if tag_amt > 0 else 0, 1)
            
            # 전년 데이터 찾기
            py_record = next((r for r in sales_py_records if r.get('LOCAL_SHOP_CD') == shop_key), None)
            if py_record:
                py_tag = float(py_record.get('TAG_SALE_AMT', 0) or 0)
                py_act = float(py_record.get('ACT_SALE_AMT', 0) or 0)
                py_discount = round(((py_tag - py_act) / py_tag * 100) if py_tag > 0 else 0, 1)
                yoy = discount_rate - py_discount
            else:
                py_discount = 0
                yoy = 0
            
            store_discount_data.append({
                'SHOP_CD': shop_key,
                'SHOP_NM': record.get('SHOP_NM', ''),
                'CNTRY_CD': record.get('CNTRY_CD', ''),
                'DISCOUNT_RATE_CY': discount_rate,
                'DISCOUNT_RATE_PY': py_discount,
                'YOY': round(yoy, 1),
                'ACT_SALE_AMT_K': round(act_amt / 1000, 0)
            })
        
        # 할인율이 낮은 매장 (우수) - 할인율이 낮을수록 할인을 덜 했다는 의미
        excellent_stores = sorted([s for s in store_discount_data if s['DISCOUNT_RATE_CY'] < discount_rate_cy and s['YOY'] < 0],
                                 key=lambda x: (x['DISCOUNT_RATE_CY'], x['YOY']))[:10]
        
        # 할인율이 높은 매장 (주의) - 할인율이 높을수록 할인을 많이 했다는 의미
        warning_stores = sorted([s for s in store_discount_data if s['DISCOUNT_RATE_CY'] > discount_rate_cy or s['YOY'] > 0],
                               key=lambda x: (-x['DISCOUNT_RATE_CY'], -x['YOY']))[:10]
        
        print(f"전체 할인율 (당해): {discount_rate_cy:.1f}%")
        print(f"전체 할인율 (전년): {discount_rate_py:.1f}%")
        print(f"전년대비 변화: {round(discount_rate_cy - discount_rate_py, 1)}%p")
        print(f"분석 매장 수: {len(store_discount_data)}개")
        
        # LLM 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 대만 가격 전략 전문가야. 할인율 데이터를 분석하여 가격 전략의 효율성을 평가하고 최적화 방안을 제시해줘.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 전체 할인율 (당해): {discount_rate_cy:.1f}%
- 전체 할인율 (전년): {discount_rate_py:.1f}%
- 전년대비 변화: {round(discount_rate_cy - discount_rate_py, 1)}%p

**할인율이 낮은 매장 (우수)**
{json_dumps_safe(excellent_stores, ensure_ascii=False, indent=2)}

**할인율이 높은 매장 (주의)**
{json_dumps_safe(warning_stores, ensure_ascii=False, indent=2)}

<분석 목표>
1. 할인율 구조 분석 및 주요 특징 식별
2. 전년 대비 변화 분석 및 효율성 평가
3. 가격 전략 최적화 방안 제시

<요구사항>
{get_common_json_requirement()}

{{
  "title": "AI 종합인사이트",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "할인율 구조 분석",
      "ai_text": "전체 할인율 구조를 분석하고 주요 특징을 식별해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "전년 대비 변화 분석",
      "ai_text": "전년 대비 할인율 변화를 분석하고 효율성을 평가해줘."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "가격 전략 최적화 방안",
      "ai_text": "가격 전략 최적화를 위한 구체적인 방안을 제시해줘."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 할인율은 소수점 첫째자리까지 표기
- 구체적인 매장명과 수치를 포함하여 작성
- 실행 가능한 구체적인 개선 방안 제시

{get_common_prompt_footer()}
"""
        
        # LLM 호출
        analysis_response = call_llm(prompt, max_tokens=4000)
        analysis_data = parse_llm_json_response(analysis_response, "AI 종합인사이트")
        
        if len(analysis_data.get('sections', [])) < 3:
            analysis_data['sections'].extend([
                {"div": "종합분석-2", "sub_title": "전년 대비 변화 분석", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "가격 전략 최적화 방안", "ai_text": ""}
            ])
        
        # JSON 데이터 구성
        json_data = {
            'country': 'TW',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '할인율',
            'sub_key': 'AI종합분석',
            'analysis_data': analysis_data,
            'summary': {
                'discount_rate_cy': discount_rate_cy,
                'discount_rate_py': discount_rate_py,
                'yoy': round(discount_rate_cy - discount_rate_py, 1),
                'store_count': len(store_discount_data),
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'store_analysis': {
                'excellent_stores': excellent_stores,
                'warning_stores': warning_stores
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]
        filename = f"TW_{yyyymm_short}_{brd_cd}_할인율_AI종합분석"
        save_json(json_data, filename)
        
        # Markdown 저장
        markdown_content = f"# {analysis_data.get('title', 'AI 종합인사이트')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 할인율 AI 종합분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_store_efficiency_overall(yyyymm, brd_cd):
    """매장 효율성 AI 종합분석 (직접이익 기준, 평당 일평균)"""
    print(f"\n{'='*60}")
    print(f"매장 효율성 AI 종합분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # 매장별 평수 정보 (제공된 데이터)
    store_area_sqm = {
        'TAIMALL': 47,
        'LALAPORT Taichung': 40,
        'MLB忠孝旗艦店': 56,
        'LALAPORT Nangang': 47,
        'HANSHIN ARENA': 38,
        'Metrowalk': 36,
        'Nanfang TS MALL': 30,
        'Banqiao Megacity': 24,
        '新光三越南西3': 30,
        'TAIPEI 101': 19,
        '台中中友百貨': 38,
        'SKM TAINAN': 26,
        'ZhongXiao SOGO': 32,
        'Gloria Outlet': 47,
        'Mitsui Outlet Taichung': 33,
        'Mitsui Outlet Park Linkou': 32
    }
    
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # 당해/전년 매장별 매출 조회
        sales_cy_sql = get_store_sales_query(yyyymm, brd_cd)
        sales_py_sql = get_store_sales_query(yyyymm_py, brd_cd)
        
        sales_cy_df = run_query(sales_cy_sql, engine)
        sales_py_df = run_query(sales_py_sql, engine)
        sales_cy_records = sales_cy_df.to_dicts()
        sales_py_records = sales_py_df.to_dicts()
        
        # 당해/전년 매장별 매출원가 조회
        cogs_cy_sql = get_store_cogs_query(yyyymm, brd_cd)
        cogs_py_sql = get_store_cogs_query(yyyymm_py, brd_cd)
        
        cogs_cy_df = run_query(cogs_cy_sql, engine)
        cogs_py_df = run_query(cogs_py_sql, engine)
        cogs_cy_records = cogs_cy_df.to_dicts()
        cogs_py_records = cogs_py_df.to_dicts()
        
        # 당해/전년 매장별 직접비용 조회
        cost_cy_sql = get_store_direct_cost_query(yyyymm, brd_cd)
        cost_py_sql = get_store_direct_cost_query(yyyymm_py, brd_cd)
        
        cost_cy_df = run_query(cost_cy_sql, engine)
        cost_py_df = run_query(cost_py_sql, engine)
        cost_cy_records = cost_cy_df.to_dicts()
        cost_py_records = cost_py_df.to_dicts()
        
        if not sales_cy_records:
            print("데이터가 없습니다.")
            return None
        
        # 매장별 매출 집계 (당해)
        store_sales_cy = {}
        for record in sales_cy_records:
            shop_key = record.get('LOCAL_SHOP_CD', '')
            shop_nm = record.get('SHOP_NM', '')
            chnl_nm = record.get('MGMT_CHNL_NM', '')
            if shop_key not in store_sales_cy:
                store_sales_cy[shop_key] = {
                    'SHOP_NM': shop_nm,
                    'CNTRY_CD': record.get('CNTRY_CD', ''),
                    'MGMT_CHNL_NM': chnl_nm,
                    'VAT_EXC_ACT_SALE_AMT': 0,
                    'ACT_SALE_AMT': 0
                }
            store_sales_cy[shop_key]['VAT_EXC_ACT_SALE_AMT'] += float(record.get('VAT_EXC_ACT_SALE_AMT', 0) or 0)
            store_sales_cy[shop_key]['ACT_SALE_AMT'] += float(record.get('ACT_SALE_AMT', 0) or 0)
        
        # 매장별 매출 집계 (전년)
        store_sales_py = {}
        for record in sales_py_records:
            shop_key = record.get('LOCAL_SHOP_CD', '')
            if shop_key not in store_sales_py:
                store_sales_py[shop_key] = {
                    'VAT_EXC_ACT_SALE_AMT': 0,
                    'ACT_SALE_AMT': 0
                }
            store_sales_py[shop_key]['VAT_EXC_ACT_SALE_AMT'] += float(record.get('VAT_EXC_ACT_SALE_AMT', 0) or 0)
            store_sales_py[shop_key]['ACT_SALE_AMT'] += float(record.get('ACT_SALE_AMT', 0) or 0)
        
        # 매장별 매출원가 집계 (당해)
        store_cogs_cy = {}
        for record in cogs_cy_records:
            shop_key = record.get('LOCAL_SHOP_CD', '')
            amt = float(record.get('AMT', 0) or 0)
            if shop_key not in store_cogs_cy:
                store_cogs_cy[shop_key] = {'TOTAL_COGS': 0}
            store_cogs_cy[shop_key]['TOTAL_COGS'] += amt
        
        # 매장별 매출원가 집계 (전년)
        store_cogs_py = {}
        for record in cogs_py_records:
            shop_key = record.get('LOCAL_SHOP_CD', '')
            amt = float(record.get('AMT', 0) or 0)
            if shop_key not in store_cogs_py:
                store_cogs_py[shop_key] = {'TOTAL_COGS': 0}
            store_cogs_py[shop_key]['TOTAL_COGS'] += amt
        
        # 매장별 직접비용 집계 (당해)
        store_cost_cy = {}
        for record in cost_cy_records:
            shop_key = record.get('LOCAL_SHOP_CD', '')
            amt = float(record.get('AMT', 0) or 0)
            if shop_key not in store_cost_cy:
                store_cost_cy[shop_key] = {'TOTAL_COST': 0}
            store_cost_cy[shop_key]['TOTAL_COST'] += amt
        
        # 매장별 직접비용 집계 (전년)
        store_cost_py = {}
        for record in cost_py_records:
            shop_key = record.get('LOCAL_SHOP_CD', '')
            amt = float(record.get('AMT', 0) or 0)
            if shop_key not in store_cost_py:
                store_cost_py[shop_key] = {'TOTAL_COST': 0}
            store_cost_py[shop_key]['TOTAL_COST'] += amt
        
        # 매장별 직접이익 계산 및 효율성 데이터 생성
        store_efficiency_data = []
        for shop_key, sales_data in store_sales_cy.items():
            shop_nm = sales_data['SHOP_NM']
            chnl_nm = sales_data.get('MGMT_CHNL_NM', '')
            
            # 온라인 매장 제외
            if '온라인' in chnl_nm or 'online' in shop_nm.lower() or 'MOMO' in shop_nm or '91APP' in shop_nm or 'SHOPEE' in shop_nm:
                continue
            
            vat_exc_act_sale_amt = sales_data['VAT_EXC_ACT_SALE_AMT']
            act_sale_amt = sales_data['ACT_SALE_AMT']
            cogs = store_cogs_cy.get(shop_key, {}).get('TOTAL_COGS', 0)
            direct_cost = store_cost_cy.get(shop_key, {}).get('TOTAL_COST', 0)
            direct_profit = vat_exc_act_sale_amt - cogs - direct_cost
            
            # 전년 대비 비교
            prev_vat_exc_sale = store_sales_py.get(shop_key, {}).get('VAT_EXC_ACT_SALE_AMT', 0)
            prev_cogs = store_cogs_py.get(shop_key, {}).get('TOTAL_COGS', 0)
            prev_cost = store_cost_py.get(shop_key, {}).get('TOTAL_COST', 0)
            prev_profit = prev_vat_exc_sale - prev_cogs - prev_cost
            
            # 평수 정보 찾기 (매장명 매칭)
            area_sqm = 0
            for store_name, area in store_area_sqm.items():
                if store_name in shop_nm or shop_nm in store_name:
                    area_sqm = area
                    break
            
            if area_sqm == 0:
                print(f"경고: {shop_nm}의 평수 정보를 찾을 수 없습니다.")
                continue
            
            # 채널 분류 (정상/아울렛)
            if 'Outlet' in shop_nm or '아울렛' in chnl_nm or 'OUTLET' in chnl_nm:
                channel_type = '아울렛'
            else:
                channel_type = '정상'
            
            # 평당 일평균 직접이익 계산
            # 월 직접이익 / 평수 / 월 일수 = 일평균 평당 직접이익
            days_in_month = monthrange(current_year, current_month)[1]
            day_per_sqm_profit = round(direct_profit / area_sqm / days_in_month, 0) if area_sqm > 0 else 0
            
            # 전년 평당 일평균 직접이익
            prev_day_per_sqm_profit = round(prev_profit / area_sqm / days_in_month, 0) if area_sqm > 0 else 0
            
            profit_yoy_pct = round(((direct_profit - prev_profit) / prev_profit * 100) if prev_profit != 0 else 0, 1)
            
            store_efficiency_data.append({
                'SHOP_CD': shop_key,
                'SHOP_NM': shop_nm,
                'CNTRY_CD': sales_data['CNTRY_CD'],
                'CHNL_NM': channel_type,
                'AREA_SQM': area_sqm,
                'VAT_EXC_ACT_SALE_AMT_K': round(vat_exc_act_sale_amt / 1000, 0),
                'ACT_SALE_AMT_K': round(act_sale_amt / 1000, 0),
                'COGS_K': round(cogs / 1000, 0),
                'DIRECT_COST_K': round(direct_cost / 1000, 0),
                'DIRECT_PROFIT_K': round(direct_profit / 1000, 0),
                'DAY_PER_SQM_PROFIT': day_per_sqm_profit,  # 평당 일평균 직접이익
                'DAY_PER_SQM_PROFIT_PY': prev_day_per_sqm_profit,
                'PROFIT_YOY_PCT': profit_yoy_pct
            })
        
        if not store_efficiency_data:
            print("오프라인 매장 데이터가 없습니다.")
            return None
        
        # 채널별 집계 (정상/아울렛)
        channel_summary = {}
        for store in store_efficiency_data:
            chnl_nm = store['CHNL_NM']
            if chnl_nm not in channel_summary:
                channel_summary[chnl_nm] = {
                    'store_count': 0,
                    'total_area_sqm': 0,
                    'total_profit': 0,
                    'avg_day_per_sqm_profit': 0
                }
            channel_summary[chnl_nm]['store_count'] += 1
            channel_summary[chnl_nm]['total_area_sqm'] += store['AREA_SQM']
            channel_summary[chnl_nm]['total_profit'] += store['DIRECT_PROFIT_K'] * 1000
        
        for chnl_nm, data in channel_summary.items():
            if data['total_area_sqm'] > 0:
                # 평당 일평균 직접이익 = 총 직접이익 / 총 평수 / 일수
                data['avg_day_per_sqm_profit'] = round(data['total_profit'] / data['total_area_sqm'] / days_in_month, 0)
        
        channel_summary_formatted = [{
            'CHNL_NM': k,
            'STORE_COUNT': v['store_count'],
            'AVG_DAY_PER_SQM_PROFIT': v['avg_day_per_sqm_profit']
        } for k, v in sorted(channel_summary.items())]
        
        # 전체 평균 계산
        total_profit_cy = sum(s['DIRECT_PROFIT_K'] * 1000 for s in store_efficiency_data)
        total_area_cy = sum(s['AREA_SQM'] for s in store_efficiency_data)
        avg_day_per_sqm_profit_cy = round(total_profit_cy / total_area_cy / days_in_month, 0) if total_area_cy > 0 else 0
        
        # 전년 평균 (전년 데이터가 있는 매장만)
        py_stores = [s for s in store_efficiency_data if s['DAY_PER_SQM_PROFIT_PY'] != 0]
        if py_stores:
            total_profit_py = sum(s['DAY_PER_SQM_PROFIT_PY'] * s['AREA_SQM'] * days_in_month for s in py_stores)
            total_area_py = sum(s['AREA_SQM'] for s in py_stores)
            avg_day_per_sqm_profit_py = round(total_profit_py / total_area_py / days_in_month, 0) if total_area_py > 0 else 0
        else:
            avg_day_per_sqm_profit_py = 0
        
        store_count_cy = len(store_efficiency_data)
        store_count_py = len(py_stores)
        avg_yoy_pct = round(((avg_day_per_sqm_profit_cy - avg_day_per_sqm_profit_py) / avg_day_per_sqm_profit_py * 100) if avg_day_per_sqm_profit_py != 0 else 0, 1)
        
        # 고효율 매장 (평당 일평균 직접이익이 평균보다 높고 개선)
        excellent_stores = sorted([s for s in store_efficiency_data 
                                  if s['DAY_PER_SQM_PROFIT'] > avg_day_per_sqm_profit_cy and s['PROFIT_YOY_PCT'] > 0],
                                 key=lambda x: (x['DAY_PER_SQM_PROFIT'], x['PROFIT_YOY_PCT']), reverse=True)[:10]
        
        # 비효율 매장 (평당 일평균 직접이익이 평균보다 낮거나 악화)
        warning_stores = sorted([s for s in store_efficiency_data 
                                if s['DAY_PER_SQM_PROFIT'] < avg_day_per_sqm_profit_cy or s['PROFIT_YOY_PCT'] < 0],
                               key=lambda x: (x['DAY_PER_SQM_PROFIT'], x['PROFIT_YOY_PCT']))[:10]
        
        print(f"평균 평당 일평균 직접이익 (당해): {avg_day_per_sqm_profit_cy:,.0f} HKD/㎡/일 ({store_count_cy}개 매장)")
        print(f"평균 평당 일평균 직접이익 (전년): {avg_day_per_sqm_profit_py:,.0f} HKD/㎡/일 ({store_count_py}개 매장)")
        print(f"전년대비 변화: {avg_yoy_pct:.1f}%")
        print(f"채널 수: {len(channel_summary)}개 (정상/아울렛)")
        
        # LLM 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 대만 매장 운영 전문가야. 매장 효율성(평당 일평균 직접이익) 데이터를 분석하여 고효율 매장과 비효율 매장을 식별하고 최적화 방안을 제시해줘.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 평균 평당 일평균 직접이익 (당해): {avg_day_per_sqm_profit_cy:,.0f} HKD/㎡/일 ({store_count_cy}개 오프라인 매장)
- 평균 평당 일평균 직접이익 (전년): {avg_day_per_sqm_profit_py:,.0f} HKD/㎡/일 ({store_count_py}개 매장)
- 전년대비 변화: {avg_yoy_pct:.1f}%

**채널별 평균 평당 일평균 직접이익**
{json_dumps_safe(channel_summary_formatted, ensure_ascii=False, indent=2)}

**고효율 매장 (평당 일평균 직접이익이 평균보다 높고 개선)**
{json_dumps_safe(excellent_stores, ensure_ascii=False, indent=2)}

**비효율 매장 (평당 일평균 직접이익이 평균보다 낮거나 악화)**
{json_dumps_safe(warning_stores, ensure_ascii=False, indent=2)}

<분석 목표>
1. 매장 효율성 구조 분석 및 주요 특징 식별 (정상/아울렛 채널별 비교)
2. 전년 대비 변화 분석 및 효율성 평가
3. 매장 효율성 개선 방안 제시

<요구사항>
{get_common_json_requirement()}

{{
  "title": "AI 종합인사이트",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "매장 효율성 구조 분석",
      "ai_text": "평당 일평균 직접이익 구조를 분석하고 주요 특징을 식별해줘. 정상/아울렛 채널별 효율성 차이를 분석하고 고효율 매장의 공통 특징을 도출해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "전년 대비 변화 분석",
      "ai_text": "전년 대비 평당 일평균 직접이익 변화를 분석하고 효율성을 평가해줘. 개선된 매장과 악화된 매장의 원인을 분석해줘."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "매장 효율성 개선 방안",
      "ai_text": "매장 효율성 개선을 위한 구체적인 방안을 제시해줘. 고효율 매장의 성공 요인을 다른 매장에 적용할 수 있는 방안과 비효율 매장의 구체적인 개선 전략을 제시해줘."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 평당 일평균 직접이익은 HKD/㎡/일 단위로 표기
- 직접이익은 K 단위 정수로 표기
- 구체적인 매장명, 채널명(정상/아울렛), 수치를 포함하여 작성
- 실행 가능한 구체적인 개선 방안 제시

{get_common_prompt_footer()}
"""
        
        # LLM 호출
        analysis_response = call_llm(prompt, max_tokens=4000)
        analysis_data = parse_llm_json_response(analysis_response, "AI 종합인사이트")
        
        if len(analysis_data.get('sections', [])) < 3:
            analysis_data['sections'].extend([
                {"div": "종합분석-2", "sub_title": "전년 대비 변화 분석", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "매장 효율성 개선 방안", "ai_text": ""}
            ])
        
        # JSON 데이터 구성
        json_data = {
            'country': 'TW',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '매장효율성',
            'sub_key': 'AI종합분석',
            'analysis_data': analysis_data,
            'summary': {
                'avg_day_per_sqm_profit_cy': avg_day_per_sqm_profit_cy,
                'avg_day_per_sqm_profit_py': avg_day_per_sqm_profit_py,
                'avg_yoy_pct': avg_yoy_pct,
                'store_count_cy': store_count_cy,
                'store_count_py': store_count_py,
                'channel_count': len(channel_summary),
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'channel_summary': channel_summary_formatted,
            'store_analysis': {
                'excellent_stores': excellent_stores,
                'warning_stores': warning_stores
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]
        filename = f"TW_{yyyymm_short}_{brd_cd}_매장효율성_AI종합분석"
        save_json(json_data, filename)
        
        # Markdown 저장
        markdown_content = f"# {analysis_data.get('title', 'AI 종합인사이트')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 매장 효율성 AI 종합분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def get_store_sales_per_sqm_query(yyyymm, brd_cd):
    """평당매출 분석 쿼리 (당해/전년 비교)"""
    # BRD_CD: M 요청 시 I 포함 (I는 M으로 표시)
    brd_filter_mst = f"('{brd_cd}' = 'M' AND brd_cd IN ('M', 'I')) OR brd_cd = '{brd_cd}'"
    brd_filter_raw = f"('{brd_cd}' = 'M' AND a.brd_cd IN ('M', 'I')) OR a.brd_cd = '{brd_cd}'"
    
    current_year = int(yyyymm[:4])
    current_month = int(yyyymm[4:6])
    previous_year = current_year - 1
    yyyymm_py = f"{previous_year:04d}{current_month:02d}"
    
    return f"""
-- 평당매출
with param as (
    select 'CY' as div
        , '{yyyymm}' as std_end_yyyymm
    union all
    select 'PY' as div
        , '{yyyymm_py}' as std_end_yyyymm
)
-- 채널 기준
, channel_std as (
    select brd_cd
         , local_shop_cd
         , local_shop_nm
         , cntry_cd
         , decode(cntry_cd, 'HK', '홍콩', 'MO', '마카오', 'TW', '대만') as cntry_nm
         , mgmt_chnl_nm as chnl_nm
    from sap_fnf.mst_hmd_shop
    where cntry_cd in ('TW')
      and type_nm = 'SHOP'
      and ({brd_filter_mst})
    group by brd_cd, local_shop_cd, local_shop_nm, cntry_cd, mgmt_chnl_nm
)
-- 환율
, exchn_rate as (
    select p.div
         , source_crncy
         , exchn_rate
    from comm.hst_exchn_rate a
    join param p
    on p.std_end_yyyymm between a.efct_start_yyyymm and a.efct_end_yyyymm
    where 1=1
    and target_crncy = 'HKD'
    union all
    select p.div, 'HKD', 1
    from param p
)
-- 평수
, area_sqm as (
    select p.div
         , decode(p.div, 'CY', a.yyyymm, 'PY', (a.yyyymm::int + 100)::varchar) as yyyymm
         , a.brd_cd
         , b.chnl_nm
         , a.cntry_cd
         , a.local_shop_cd
         , sum(area_sqm)::float area_sqm
    from sap_fnf.prep_hmd_shop_hist_m a
    join channel_std b
      on a.brd_cd = b.brd_cd
     and a.local_shop_cd = b.local_shop_cd
    join param p
      on a.yyyymm = p.std_end_yyyymm
    group by 1,2,3,4,5,6
)
, raw as (
    select p.div
        , decode(p.div, 'CY', a.yyyymm, 'PY', (a.yyyymm::int + 100)::varchar) as yyyymm
        , a.brd_cd
        , a.cntry_cd
        , b.chnl_nm
        , a.local_shop_cd
        , b.local_shop_nm
        , max(day(last_day(to_date(p.std_end_yyyymm, 'YYYYMM')))) as day_cnt
        , sum(a.act_sale_amt * x.exchn_rate) as sale_amt
        , sum(a.tag_sale_amt * x.exchn_rate) as tag_amt
    from sap_fnf.dm_hmd_ivtr_shop_prdt_m a
    join channel_std b
      on a.brd_cd = b.brd_cd
     and a.local_shop_cd = b.local_shop_cd
     and b.chnl_nm <> '3.온라인'
     and b.local_shop_cd <> 'M03'
    join param p
      on a.yyyymm = p.std_end_yyyymm
    join exchn_rate x
      on a.currency = x.source_crncy
      and p.div = x.div
    where ({brd_filter_raw})
    group by 1,2,3,4,5,6,7
), main as (
    select a.div
        , a.yyyymm
        , a.cntry_cd
        , a.chnl_nm
        , a.local_shop_cd
        , max(a.local_shop_nm) local_shop_nm
        , max(a.day_cnt) day_cnt
        , sum(a.sale_amt) sale_amt --실판가
        , sum(a.tag_amt) tag_amt --tag가
        , sum(b.area_sqm) area_sqm --평수
    from raw a
    join area_sqm b
      on a.local_shop_cd = b.local_shop_cd
     and a.yyyymm = b.yyyymm
     and a.brd_cd = b.brd_cd
     and a.div = b.div
    where 1=1
    group by a.div
        , a.yyyymm
        , a.cntry_cd
        , a.chnl_nm
        , a.local_shop_cd
)
select *
    , sale_amt / nullif(area_sqm, 0) as mon_per_sale --월 평당매출
    , sale_amt / nullif(area_sqm, 0) / nullif(day_cnt, 0) as day_per_sale --일 평당매출
from main
order by div, cntry_cd, chnl_nm, local_shop_cd
"""

def analyze_store_sales_per_sqm(yyyymm, brd_cd):
    """평당매출 AI 종합분석"""
    print(f"\n{'='*60}")
    print(f"평당매출 AI 종합분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # 평당매출 쿼리 실행
        sql = get_store_sales_per_sqm_query(yyyymm, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 당해/전년 데이터 분리
        cy_data = {}
        py_data = {}
        
        for record in records:
            div = record.get('DIV', '')
            shop_key = record.get('LOCAL_SHOP_CD', '')
            chnl_nm = record.get('CHNL_NM', '')
            cntry_cd = record.get('CNTRY_CD', '')
            
            sale_amt = float(record.get('SALE_AMT', 0) or 0)
            area_sqm = float(record.get('AREA_SQM', 0) or 0)
            mon_per_sale = float(record.get('MON_PER_SALE', 0) or 0)
            day_per_sale = float(record.get('DAY_PER_SALE', 0) or 0)
            
            if div == 'CY':
                if shop_key not in cy_data:
                    cy_data[shop_key] = {
                        'SHOP_NM': record.get('LOCAL_SHOP_NM', ''),
                        'CNTRY_CD': cntry_cd,
                        'CHNL_NM': chnl_nm,
                        'SALE_AMT': 0,
                        'AREA_SQM': 0,
                        'MON_PER_SALE': 0,
                        'DAY_PER_SALE': 0
                    }
                cy_data[shop_key]['SALE_AMT'] += sale_amt
                cy_data[shop_key]['AREA_SQM'] = area_sqm  # 평수는 동일하므로 마지막 값 사용
                cy_data[shop_key]['MON_PER_SALE'] = mon_per_sale if area_sqm > 0 else 0
                cy_data[shop_key]['DAY_PER_SALE'] = day_per_sale if area_sqm > 0 else 0
            elif div == 'PY':
                if shop_key not in py_data:
                    py_data[shop_key] = {
                        'SALE_AMT': 0,
                        'AREA_SQM': 0,
                        'MON_PER_SALE': 0,
                        'DAY_PER_SALE': 0
                    }
                py_data[shop_key]['SALE_AMT'] += sale_amt
                py_data[shop_key]['AREA_SQM'] = area_sqm
                py_data[shop_key]['MON_PER_SALE'] = mon_per_sale if area_sqm > 0 else 0
                py_data[shop_key]['DAY_PER_SALE'] = day_per_sale if area_sqm > 0 else 0
        
        # 매장별 평당매출 데이터 생성
        store_per_sqm_data = []
        for shop_key, cy_info in cy_data.items():
            cy_mon_per_sale = cy_info['MON_PER_SALE']
            cy_day_per_sale = cy_info['DAY_PER_SALE']
            cy_sale_amt = cy_info['SALE_AMT']
            area_sqm = cy_info['AREA_SQM']
            
            py_info = py_data.get(shop_key, {})
            py_mon_per_sale = py_info.get('MON_PER_SALE', 0)
            py_day_per_sale = py_info.get('DAY_PER_SALE', 0)
            
            mon_yoy_pct = round(((cy_mon_per_sale - py_mon_per_sale) / py_mon_per_sale * 100) if py_mon_per_sale > 0 else 0, 1)
            day_yoy_pct = round(((cy_day_per_sale - py_day_per_sale) / py_day_per_sale * 100) if py_day_per_sale > 0 else 0, 1)
            
            store_per_sqm_data.append({
                'SHOP_CD': shop_key,
                'SHOP_NM': cy_info['SHOP_NM'],
                'CNTRY_CD': cy_info['CNTRY_CD'],
                'CHNL_NM': cy_info['CHNL_NM'],
                'AREA_SQM': round(area_sqm, 1),
                'SALE_AMT_K': round(cy_sale_amt / 1000, 0),
                'MON_PER_SALE': round(cy_mon_per_sale, 0),
                'DAY_PER_SALE': round(cy_day_per_sale, 0),
                'MON_PER_SALE_PY': round(py_mon_per_sale, 0),
                'DAY_PER_SALE_PY': round(py_day_per_sale, 0),
                'MON_YOY_PCT': mon_yoy_pct,
                'DAY_YOY_PCT': day_yoy_pct
            })
        
        # 전체 평균 계산
        total_sale_cy = sum(cy['SALE_AMT'] for cy in cy_data.values())
        total_area = sum(cy['AREA_SQM'] for cy in cy_data.values())
        avg_mon_per_sale_cy = round(total_sale_cy / total_area, 0) if total_area > 0 else 0
        
        total_sale_py = sum(py['SALE_AMT'] for py in py_data.values())
        total_area_py = sum(py['AREA_SQM'] for py in py_data.values())
        avg_mon_per_sale_py = round(total_sale_py / total_area_py, 0) if total_area_py > 0 else 0
        
        avg_yoy_pct = round(((avg_mon_per_sale_cy - avg_mon_per_sale_py) / avg_mon_per_sale_py * 100) if avg_mon_per_sale_py > 0 else 0, 1)
        
        # 우수 매장 (평균보다 높고 개선)
        excellent_stores = sorted([s for s in store_per_sqm_data 
                                  if s['MON_PER_SALE'] > avg_mon_per_sale_cy and s['MON_YOY_PCT'] > 0],
                                 key=lambda x: (x['MON_PER_SALE'], x['MON_YOY_PCT']), reverse=True)[:10]
        
        # 개선 필요 매장 (평균보다 낮거나 악화)
        warning_stores = sorted([s for s in store_per_sqm_data 
                                if s['MON_PER_SALE'] < avg_mon_per_sale_cy or s['MON_YOY_PCT'] < 0],
                               key=lambda x: (x['MON_PER_SALE'], x['MON_YOY_PCT']))[:10]
        
        # 채널별 집계
        channel_summary = {}
        for store in store_per_sqm_data:
            chnl_nm = store['CHNL_NM']
            if chnl_nm not in channel_summary:
                channel_summary[chnl_nm] = {
                    'store_count': 0,
                    'total_area_sqm': 0,
                    'total_sale_amt': 0,
                    'avg_mon_per_sale': 0
                }
            channel_summary[chnl_nm]['store_count'] += 1
            channel_summary[chnl_nm]['total_area_sqm'] += store['AREA_SQM']
            channel_summary[chnl_nm]['total_sale_amt'] += store['SALE_AMT_K'] * 1000
        
        for chnl_nm, data in channel_summary.items():
            if data['total_area_sqm'] > 0:
                data['avg_mon_per_sale'] = round(data['total_sale_amt'] / data['total_area_sqm'], 0)
        
        channel_summary_formatted = [{
            'CHNL_NM': k,
            'STORE_COUNT': v['store_count'],
            'AVG_MON_PER_SALE': v['avg_mon_per_sale']
        } for k, v in sorted(channel_summary.items(), key=lambda x: x[1]['avg_mon_per_sale'], reverse=True)]
        
        print(f"평균 월 평당매출 (당해): {avg_mon_per_sale_cy:,.0f} HKD/㎡ ({len(cy_data)}개 매장)")
        print(f"평균 월 평당매출 (전년): {avg_mon_per_sale_py:,.0f} HKD/㎡ ({len(py_data)}개 매장)")
        print(f"전년대비 변화: {avg_yoy_pct:.1f}%")
        print(f"채널 수: {len(channel_summary)}개")
        
        # LLM 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 대만 매장 운영 전문가야. 평당매출(평방미터당 매출) 데이터를 분석하여 우수 매장과 개선이 필요한 매장을 식별하고 최적화 방안을 제시해줘.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 평균 월 평당매출 (당해): {avg_mon_per_sale_cy:,.0f} HKD/㎡ ({len(cy_data)}개 매장)
- 평균 월 평당매출 (전년): {avg_mon_per_sale_py:,.0f} HKD/㎡ ({len(py_data)}개 매장)
- 전년대비 변화: {avg_yoy_pct:.1f}%

**채널별 평균 평당매출**
{json_dumps_safe(channel_summary_formatted, ensure_ascii=False, indent=2)}

**우수 매장 (평균보다 높고 개선)**
{json_dumps_safe(excellent_stores, ensure_ascii=False, indent=2)}

**개선 필요 매장**
{json_dumps_safe(warning_stores, ensure_ascii=False, indent=2)}

<분석 목표>
1. 평당매출 구조 분석 및 주요 특징 식별
2. 전년 대비 변화 분석 및 효율성 평가
3. 평당매출 개선 방안 제시

<요구사항>
{get_common_json_requirement()}

{{
  "title": "AI 종합인사이트",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "평당매출 구조 분석",
      "ai_text": "평당매출 구조를 분석하고 주요 특징을 식별해줘. 채널별, 지역별 평당매출 차이를 분석하고 우수 매장의 공통 특징을 도출해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "전년 대비 변화 분석",
      "ai_text": "전년 대비 평당매출 변화를 분석하고 효율성을 평가해줘. 개선된 매장과 악화된 매장의 원인을 분석해줘."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "평당매출 개선 방안",
      "ai_text": "평당매출 개선을 위한 구체적인 방안을 제시해줘. 우수 매장의 성공 요인을 다른 매장에 적용할 수 있는 방안과 개선 필요 매장의 구체적인 개선 전략을 제시해줘."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 평당매출은 HKD/㎡ 단위로 표기
- 매출액(V+)은 K 단위 정수로 표기
- 구체적인 매장명, 채널명, 수치를 포함하여 작성
- 실행 가능한 구체적인 개선 방안 제시

{get_common_prompt_footer()}
"""
        
        # LLM 호출
        analysis_response = call_llm(prompt, max_tokens=4000)
        analysis_data = parse_llm_json_response(analysis_response, "AI 종합인사이트")
        
        if len(analysis_data.get('sections', [])) < 3:
            analysis_data['sections'].extend([
                {"div": "종합분석-2", "sub_title": "전년 대비 변화 분석", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "평당매출 개선 방안", "ai_text": ""}
            ])
        
        # JSON 데이터 구성
        json_data = {
            'country': 'TW',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '평당매출',
            'sub_key': 'AI종합분석',
            'analysis_data': analysis_data,
            'summary': {
                'avg_mon_per_sale_cy': avg_mon_per_sale_cy,
                'avg_mon_per_sale_py': avg_mon_per_sale_py,
                'avg_yoy_pct': avg_yoy_pct,
                'store_count_cy': len(cy_data),
                'store_count_py': len(py_data),
                'channel_count': len(channel_summary),
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'channel_summary': channel_summary_formatted,
            'store_analysis': {
                'excellent_stores': excellent_stores,
                'warning_stores': warning_stores
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]
        filename = f"TW_{yyyymm_short}_{brd_cd}_평당매출_AI종합분석"
        save_json(json_data, filename)
        
        # Markdown 저장
        markdown_content = f"# {analysis_data.get('title', 'AI 종합인사이트')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 평당매출 AI 종합분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def get_channel_sales_query_range(yyyymm_start, yyyymm_end, brd_cd):
    """채널별 매출 쿼리 (대만 TW, 기간 범위, TWD→HKD 환율 적용)"""
    brd_filter = f"(a.BRD_CD = '{brd_cd}' OR ('{brd_cd}' = 'M' AND a.BRD_CD = 'I'))"
    return f"""
select YYYYMM,
       a.CNTRY_CD,
       DECODE(a.BRD_CD, 'I', 'M', a.BRD_CD) AS BRD_CD,
       mgmt_chnl_nm,
       a.LOCAL_SHOP_CD,
       B.SHOP_NM,
       'HKD' AS CURRENCY,
       SUM(SALE_QTY) AS SALE_QTY,
       SUM(TAG_SALE_AMT * x.exchn_rate) AS TAG_SALE_AMT,
       SUM(ACT_SALE_AMT * x.exchn_rate) AS ACT_SALE_AMT
from SAP_FNF.DM_HMD_IVTR_SHOP_PRDT_M a
join SAP_FNF.MST_HMD_SHOP B on A.LOCAL_SHOP_CD = B.LOCAL_SHOP_CD AND A.BRD_CD = B.BRD_CD AND B.TYPE_NM = 'SHOP'
join comm.hst_exchn_rate x on a.yyyymm between x.efct_start_yyyymm and x.efct_end_yyyymm and x.target_crncy = 'HKD' and x.source_crncy = a.currency
where 1=1
  AND YYYYMM BETWEEN '{yyyymm_start}' AND '{yyyymm_end}'
  AND a.CNTRY_CD in ('TW')
  AND MGMT_CHNL_NM <> '미지정'
  AND {brd_filter}
GROUP BY YYYYMM, a.CNTRY_CD, DECODE(a.BRD_CD, 'I', 'M', a.BRD_CD), mgmt_chnl_nm, a.LOCAL_SHOP_CD, B.SHOP_NM
ORDER BY YYYYMM, CNTRY_CD, BRD_CD, MGMT_CHNL_NM
"""

def analyze_monthly_channel_sales_trend(yyyymm, brd_cd):
    """월별 채널별 매출 추세 분석 (최근 12개월)"""
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
        # 예: 202601 -> 202502부터 202601까지 (2025년 2월~2026년 1월)
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
        
        # SQL 쿼리 실행 (12개월 범위)
        sql = get_channel_sales_query_range(yyyymm_start, yyyymm_end, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('ACT_SALE_AMT', 0) or 0) for r in records)
        unique_channels = len(set(r.get('MGMT_CHNL_NM', '') for r in records if r.get('MGMT_CHNL_NM')))
        unique_months = len(set(r.get('YYYYMM', '') for r in records if r.get('YYYYMM')))
        
        print(f"총 매출액(V+): {total_sales:,.0f} HKD ({total_sales/1000:.0f} K)")
        print(f"채널 수: {unique_channels}개")
        print(f"분석 월 수: {unique_months}개월")
        
        # 데이터 가공: 월별/채널별 집계
        monthly_data = {}
        channel_data = {}
        
        for r in records:
            yyyymm_val = r.get('YYYYMM', '')
            chnl_nm = r.get('MGMT_CHNL_NM', '기타')
            cntry_cd = r.get('CNTRY_CD', '')
            sale_amt = float(r.get('ACT_SALE_AMT', 0) or 0)
            
            # 월별 데이터 집계 (대만 TW 단일 지역)
            if yyyymm_val not in monthly_data:
                monthly_data[yyyymm_val] = {
                    'total': 0,
                    'tw_total': 0,
                    'channels': {}
                }
            monthly_data[yyyymm_val]['total'] += sale_amt
            monthly_data[yyyymm_val]['tw_total'] += sale_amt
            
            if chnl_nm not in monthly_data[yyyymm_val]['channels']:
                monthly_data[yyyymm_val]['channels'][chnl_nm] = {
                    'total': 0,
                    'tw': 0
                }
            monthly_data[yyyymm_val]['channels'][chnl_nm]['total'] += sale_amt
            monthly_data[yyyymm_val]['channels'][chnl_nm]['tw'] += sale_amt
            
            # 채널별 데이터 집계
            if chnl_nm not in channel_data:
                channel_data[chnl_nm] = {
                    'total': 0,
                    'tw_total': 0,
                    'months': {}
                }
            channel_data[chnl_nm]['total'] += sale_amt
            channel_data[chnl_nm]['tw_total'] += sale_amt
            
            if yyyymm_val not in channel_data[chnl_nm]['months']:
                channel_data[chnl_nm]['months'][yyyymm_val] = {
                    'total': 0,
                    'tw': 0
                }
            channel_data[chnl_nm]['months'][yyyymm_val]['total'] += sale_amt
            channel_data[chnl_nm]['months'][yyyymm_val]['tw'] += sale_amt
        
        # 월별 총 매출 (k 단위)
        monthly_totals_k = {k: round(v['total'] / 1000, 0) for k, v in sorted(monthly_data.items())}
        
        # 채널별 총 매출 및 월별 추이 (k 단위)
        channel_summary = {}
        for chnl_nm, data in channel_data.items():
            channel_summary[chnl_nm] = {
                'total': round(data['total'] / 1000, 0),
                'tw_total': round(data['tw_total'] / 1000, 0),
                'months': {k: round(v['total'] / 1000, 0) for k, v in sorted(data['months'].items())}
            }
        
        # 채널별 정렬 (총 매출 기준 내림차순)
        channel_summary_sorted = dict(sorted(channel_summary.items(), key=lambda x: x[1]['total'], reverse=True))
        
        # LLM 분석 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 대만 채널 전략 전문가야. 월별 채널별 매출 추세 분석을 수행해줘.

**분석 기간**: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 ({yyyymm_start}~{yyyymm_end}) (최근 12개월)

**월별 총 매출(V+) 추이** (모든 금액은 K 단위, 천 단위):
{json_dumps_safe(monthly_totals_k, ensure_ascii=False, indent=2)}

**채널별 매출(V+) 데이터** (모든 금액은 K 단위, 천 단위):
{json_dumps_safe(channel_summary_sorted, ensure_ascii=False, indent=2)}

<분석 목표>
1. 월별 주요인사이트: 각 월별 매출(V+) 변화와 주요 특징을 분석
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
      "ai_text": "각 월별 매출(V+) 변화와 주요 특징을 분석한 내용. 월별 총 매출(V+) 추이를 바탕으로 성장/감소 패턴, 계절성, 특이사항 등을 분석해줘."
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
        
        # JSON 데이터 구성 (cn_analysis.py 형식 참고)
        json_data = {
            'country': 'TW',
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
        filename = f"TW_{yyyymm_short}_{brd_cd}_월별채널별매출추세분석"
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

def get_monthly_channel_item_sales_query(yyyymm_start, yyyymm_end, yyyymm_start_py, yyyymm_end_py, brd_cd):
    """월별 채널별 아이템별 매출 추세 쿼리 (최근 12개월, 당해/전년 비교)"""
    # BRD_CD: M 요청 시 I 포함 (I는 M으로 표시)
    brd_filter = f"('{brd_cd}' = 'M' AND brd_cd IN ('M', 'I')) OR brd_cd = '{brd_cd}'"
    
    # main CTE에서 사용할 브랜드 필터
    if brd_cd == 'M':
        brd_filter_main = "brd_cd in ('M', 'I')"
    else:
        brd_filter_main = f"brd_cd = '{brd_cd}'"
    
    return f"""
with param as (
    select 'CY' as div
        , '{yyyymm_start}' as std_start_yyyymm
        , '{yyyymm_end}' as std_end_yyyymm
    union all
    select 'PY' as div
        , '{yyyymm_start_py}' as std_start_yyyymm
        , '{yyyymm_end_py}' as std_end_yyyymm
)
-- cy_item : 당해 아이템 구분 기준
, cy_item as (
    select a.prdt_cd
            , a.sesn
            , a.prdt_hrrc1_nm
            , a.prdt_hrrc2_nm
            , a.prdt_hrrc3_nm
            , case
                --------------------------------------------------
                -- ACC 분류
                --------------------------------------------------
                when a.prdt_hrrc1_nm='ACC' and a.prdt_hrrc2_nm='Headwear'
                    then  '모자'
                when a.prdt_hrrc1_nm='ACC' and a.prdt_hrrc2_nm='Shoes'
                    then  '신발'
                when a.prdt_hrrc1_nm='ACC' and a.prdt_hrrc2_nm='Bag'
                    then  '가방'
                when a.prdt_hrrc1_nm='ACC' and a.prdt_hrrc2_nm='Acc_etc'
                    then  '기타 ACC'
                --------------------------------------------------
                -- 의류 분류
                --------------------------------------------------
                -- 당시즌 (SN 통합)
                when a.prdt_hrrc1_nm='의류' and param.STD_END_YYYYMM between b.start_yyyymm and b.end_yyyymm
                    then replace(a.sesn, 'N', 'S') || ' ' || a.prdt_hrrc1_nm
                --------------------------------------------------
                -- 전시즌 (조회기준 월이 9~2일때만 존재)
                when a.prdt_hrrc1_nm='의류' and right(param.STD_END_YYYYMM, 2)::int in (9,10,11,12,1,2)
                        and TO_CHAR(ADD_MONTHS(TO_DATE(param.STD_END_YYYYMM, 'YYYYMM'), -6), 'YYYYMM') between b.start_yyyymm and b.end_yyyymm
                    then replace(a.sesn, 'N', 'S') || ' ' || a.prdt_hrrc1_nm
                --------------------------------------------------
                -- 차기시즌
                when a.prdt_hrrc1_nm='의류' and b.START_YYYYMM > param.STD_END_YYYYMM
                        then '차기시즌 ' || a.prdt_hrrc1_nm
                --------------------------------------------------
                --------------------------------------------------
                -- 과시즌
                    when a.prdt_hrrc1_nm='의류' and to_char(add_months(to_date(param.STD_END_YYYYMM, 'YYYYMM'), -12), 'YYYYMM') >= b.start_yyyymm
                        then '과시즌 ' || a.prdt_hrrc1_nm
                    else '미지정' end as item_std

    from sap_fnf.mst_prdt a
    left join comm.mst_sesn b
        on a.sesn = b.sesn
    join param
        on param.div = 'CY'
    where 1=1
    and a.sesn <> 'X'
)
-- py_item : 전년 아이템 구분 기준
, py_item as (
    select a.prdt_cd
            , a.sesn
            , a.prdt_hrrc1_nm
            , a.prdt_hrrc2_nm
            , a.prdt_hrrc3_nm
            , case
                --------------------------------------------------
                -- ACC 분류
                --------------------------------------------------
                when a.prdt_hrrc1_nm='ACC' and a.prdt_hrrc2_nm='Headwear'
                    then '모자'
                when a.prdt_hrrc1_nm='ACC' and a.prdt_hrrc2_nm='Shoes'
                    then '신발'
                when a.prdt_hrrc1_nm='ACC' and a.prdt_hrrc2_nm='Bag'
                    then '가방'
                when a.prdt_hrrc1_nm='ACC' and a.prdt_hrrc2_nm='Acc_etc'
                    then '기타 ACC'
                --------------------------------------------------
                -- 의류 분류
                --------------------------------------------------
                -- 당시즌
                when a.prdt_hrrc1_nm='의류' and param.STD_END_YYYYMM between b.start_yyyymm and b.end_yyyymm
                    then (left(a.sesn,2)+1)::int || decode(right(a.sesn,1), 'N', 'S', right(a.sesn,1)) || ' ' || a.prdt_hrrc1_nm
                --------------------------------------------------
                -- 전시즌 (조회기준 월이 9~2일때만 존재)
                when a.prdt_hrrc1_nm='의류' and right(param.STD_END_YYYYMM, 2)::int in (9,10,11,12,1,2)
                        and TO_CHAR(ADD_MONTHS(TO_DATE(param.STD_END_YYYYMM, 'YYYYMM'), -6), 'YYYYMM') between b.start_yyyymm and b.end_yyyymm
                    then (left(a.sesn,2)+1)::int || decode(right(a.sesn,1), 'N', 'S', right(a.sesn,1)) || ' ' || a.prdt_hrrc1_nm
                --------------------------------------------------
                -- 차기시즌
                when a.prdt_hrrc1_nm='의류' and b.START_YYYYMM > param.STD_END_YYYYMM
                        then '차기시즌 ' || a.prdt_hrrc1_nm
                --------------------------------------------------
                -- 과시즌
                    --------------------------------------------------
                    -- 조회기준 월이 3~8월일떄
                    when a.prdt_hrrc1_nm='의류' and to_char(add_months(to_date(param.STD_END_YYYYMM, 'YYYYMM'), -12), 'YYYYMM') >= b.start_yyyymm
                        then '과시즌 ' || a.prdt_hrrc1_nm
                    else '미지정' end as item_std
    from sap_fnf.mst_prdt a
    left join comm.mst_sesn b
        on a.sesn = b.sesn
    join param
        on param.div = 'PY'
    where 1=1
    and a.sesn <> 'X'
)
-- 채널 기준
, channel_std as (
    select local_shop_cd
        , cntry_cd
        , local_shop_nm
        , brd_cd
        , decode(cntry_cd, 'HK', '홍콩', 'MO', '마카오', 'TW', '대만') as cntry_nm
        , mgmt_chnl_nm as chnl_nm
    from sap_fnf.mst_hmd_shop
    where cntry_cd in ('TW')
      and type_nm = 'SHOP'
      and ({brd_filter})
    group by local_shop_cd
            , cntry_cd
            , local_shop_nm
            , mgmt_chnl_nm
            , brd_cd
)
-- 환율
, exchn_rate as (
    select source_crncy
        , exchn_rate
    from comm.hst_exchn_rate a
    join param p
    on p.std_end_yyyymm between a.efct_start_yyyymm and a.efct_end_yyyymm
    and p.div = 'CY'
    where 1=1
    and target_crncy = 'HKD'
    union all
    select 'HKD', 1
)
-- 당해 전년
, raw as (
    select p.div
        , a.yyyymm
        , a.brd_cd
        , b.chnl_nm
        , c.item_std
        , a.local_shop_cd
        , b.local_shop_nm
        , sum(act_sale_amt * x.exchn_rate) sale_amt
        , sum(tag_sale_amt * x.exchn_rate) tag_amt
    from sap_fnf.dm_hmd_ivtr_shop_prdt_m a
    join channel_std b
      on a.local_shop_cd = b.local_shop_cd
     and a.brd_cd = b.brd_cd
    join cy_item c
    on a.prdt_cd = c.prdt_cd
    join param p
    on p.div = 'CY'
    and a.yyyymm between p.std_start_yyyymm and p.std_end_yyyymm
    join exchn_rate x
      on a.currency = x.source_crncy
    group by 1,2,3,4,5,6,7
    having sum(a.act_sale_amt) <> 0
    union all
    select p.div
        , a.yyyymm
        , a.brd_cd
        , b.chnl_nm
        , c.item_std
        , a.local_shop_cd
        , b.local_shop_nm
        , sum(act_sale_amt * x.exchn_rate) sale_amt
        , sum(tag_sale_amt * x.exchn_rate) tag_amt
    from sap_fnf.dm_hmd_ivtr_shop_prdt_m a
    join channel_std b
      on a.local_shop_cd = b.local_shop_cd
     and a.brd_cd = b.brd_cd
    join py_item c
      on a.prdt_cd = c.prdt_cd
    join param p
    on p.div = 'PY'
     and a.yyyymm between p.std_start_yyyymm and p.std_end_yyyymm
    join exchn_rate x
      on a.currency = x.source_crncy
    group by 1,2,3,4,5,6,7
    having sum(a.act_sale_amt) <> 0
)
, main as (
    select div
        , yyyymm
        , chnl_nm
        , item_std
        , local_shop_cd
        , local_shop_nm
        , sum(sale_amt) sale_amt
        , sum(tag_amt) tag_amt
    from raw
    where {brd_filter_main}
    group by div
            , yyyymm
            , chnl_nm
            , item_std
            , local_shop_cd
        , local_shop_nm
)
select *
from main
order by div, chnl_nm, local_shop_cd, item_std
"""

def analyze_monthly_channel_item_sales_trend(yyyymm, brd_cd):
    """월별 채널별 아이템별 매출 추세 분석 (최근 12개월)"""
    print(f"\n{'='*60}")
    print(f"월별 채널별 아이템별 매출 추세 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (최근 12개월)
        analysis_year = int(yyyymm[:4])
        analysis_month = int(yyyymm[4:6])
        
        # 최근 12개월 계산 (yyyymm 포함하여 12개월 전까지)
        # 예: 202601 -> 202502부터 202601까지 (2025년 2월~2026년 1월)
        if analysis_month == 12:
            start_year = analysis_year - 1
            start_month = 1
        else:
            start_year = analysis_year - 1
            start_month = analysis_month + 1
        
        yyyymm_start = f"{start_year:04d}{start_month:02d}"
        yyyymm_end = yyyymm
        
        # 전년 동일 기간 (12개월 전)
        if start_month == 12:
            start_year_py = start_year - 1
            start_month_py = 1
        else:
            start_year_py = start_year - 1
            start_month_py = start_month
        
        yyyymm_start_py = f"{start_year_py:04d}{start_month_py:02d}"
        yyyymm_end_py = f"{start_year:04d}{start_month:02d}"
        
        print(f"분석 기간: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 (최근 12개월)")
        print(f"전년 동일 기간: {yyyymm_start_py[:4]}년 {yyyymm_start_py[4:6]}월 ~ {yyyymm_end_py[:4]}년 {yyyymm_end_py[4:6]}월")
        
        # SQL 쿼리 실행
        sql = get_monthly_channel_item_sales_query(yyyymm_start, yyyymm_end, yyyymm_start_py, yyyymm_end_py, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('SALE_AMT', 0) or 0) for r in records if r.get('DIV') == 'CY')
        unique_channels = len(set(r.get('CHNL_NM', '') for r in records if r.get('CHNL_NM')))
        unique_items = len(set(r.get('ITEM_STD', '') for r in records if r.get('ITEM_STD')))
        unique_months = len(set(r.get('YYYYMM', '') for r in records if r.get('DIV') == 'CY'))
        
        print(f"총 매출액 (당해): {total_sales:,.0f} HKD ({total_sales/1000:.0f} K)")
        print(f"채널 수: {unique_channels}개")
        print(f"아이템 수: {unique_items}개")
        print(f"분석 월 수: {unique_months}개월")
        
        # 데이터 가공: 월별/채널별/아이템별 집계
        monthly_data = {}
        channel_data = {}
        item_data = {}
        channel_item_data = {}
        
        for r in records:
            div = r.get('DIV', '')
            yyyymm_val = r.get('YYYYMM', '')
            chnl_nm = r.get('CHNL_NM', '기타')
            item_std = r.get('ITEM_STD', '기타')
            sale_amt = float(r.get('SALE_AMT', 0) or 0)
            
            # 당해 데이터만 집계 (CY)
            if div != 'CY':
                continue
            
            # 월별 데이터 집계
            if yyyymm_val not in monthly_data:
                monthly_data[yyyymm_val] = {'total': 0, 'channels': {}, 'items': {}}
            monthly_data[yyyymm_val]['total'] += sale_amt
            
            if chnl_nm not in monthly_data[yyyymm_val]['channels']:
                monthly_data[yyyymm_val]['channels'][chnl_nm] = 0
            monthly_data[yyyymm_val]['channels'][chnl_nm] += sale_amt
            
            if item_std not in monthly_data[yyyymm_val]['items']:
                monthly_data[yyyymm_val]['items'][item_std] = 0
            monthly_data[yyyymm_val]['items'][item_std] += sale_amt
            
            # 채널별 데이터 집계
            if chnl_nm not in channel_data:
                channel_data[chnl_nm] = {'total': 0, 'months': {}, 'items': {}}
            channel_data[chnl_nm]['total'] += sale_amt
            
            if yyyymm_val not in channel_data[chnl_nm]['months']:
                channel_data[chnl_nm]['months'][yyyymm_val] = 0
            channel_data[chnl_nm]['months'][yyyymm_val] += sale_amt
            
            if item_std not in channel_data[chnl_nm]['items']:
                channel_data[chnl_nm]['items'][item_std] = 0
            channel_data[chnl_nm]['items'][item_std] += sale_amt
            
            # 아이템별 데이터 집계
            if item_std not in item_data:
                item_data[item_std] = {'total': 0, 'months': {}, 'channels': {}}
            item_data[item_std]['total'] += sale_amt
            
            if yyyymm_val not in item_data[item_std]['months']:
                item_data[item_std]['months'][yyyymm_val] = 0
            item_data[item_std]['months'][yyyymm_val] += sale_amt
            
            if chnl_nm not in item_data[item_std]['channels']:
                item_data[item_std]['channels'][chnl_nm] = 0
            item_data[item_std]['channels'][chnl_nm] += sale_amt
            
            # 채널-아이템별 데이터 집계
            key = f"{chnl_nm}|{item_std}"
            if key not in channel_item_data:
                channel_item_data[key] = {
                    'chnl_nm': chnl_nm,
                    'item_std': item_std,
                    'total': 0,
                    'months': {}
                }
            channel_item_data[key]['total'] += sale_amt
            
            if yyyymm_val not in channel_item_data[key]['months']:
                channel_item_data[key]['months'][yyyymm_val] = 0
            channel_item_data[key]['months'][yyyymm_val] += sale_amt
        
        # 월별 총 매출 (k 단위)
        monthly_totals_k = {k: round(v['total'] / 1000, 0) for k, v in sorted(monthly_data.items())}
        
        # 채널별 총 매출 및 월별 추이 (k 단위)
        channel_summary = {}
        for chnl_nm, data in channel_data.items():
            channel_summary[chnl_nm] = {
                'total': round(data['total'] / 1000, 0),
                'months': {k: round(v / 1000, 0) for k, v in sorted(data['months'].items())},
                'top_items': sorted(
                    [{'item_std': k, 'total': round(v / 1000, 0)} for k, v in data['items'].items()],
                    key=lambda x: x['total'], reverse=True
                )[:5]
            }
        
        # 아이템별 총 매출 및 월별 추이 (k 단위)
        item_summary = {}
        for item_std, data in item_data.items():
            item_summary[item_std] = {
                'total': round(data['total'] / 1000, 0),
                'months': {k: round(v / 1000, 0) for k, v in sorted(data['months'].items())},
                'top_channels': sorted(
                    [{'chnl_nm': k, 'total': round(v / 1000, 0)} for k, v in data['channels'].items()],
                    key=lambda x: x['total'], reverse=True
                )[:5]
            }
        
        # 채널별 정렬 (총 매출 기준 내림차순)
        channel_summary_sorted = dict(sorted(channel_summary.items(), key=lambda x: x[1]['total'], reverse=True))
        
        # 아이템별 정렬 (총 매출 기준 내림차순)
        item_summary_sorted = dict(sorted(item_summary.items(), key=lambda x: x[1]['total'], reverse=True))
        
        # LLM 분석 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 대만 채널/아이템 전략 전문가야. 월별 채널별 아이템별 매출 추세 분석을 수행해줘.

**분석 기간**: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 ({yyyymm_start}~{yyyymm_end}) (최근 12개월)

**월별 총 매출(V+) 추이** (모든 금액은 K 단위, 천 단위):
{json_dumps_safe(monthly_totals_k, ensure_ascii=False, indent=2)}

**채널별 매출(V+) 데이터** (모든 금액은 K 단위, 천 단위):
{json_dumps_safe(channel_summary_sorted, ensure_ascii=False, indent=2)}

**아이템별 매출(V+) 데이터** (모든 금액은 K 단위, 천 단위):
{json_dumps_safe(item_summary_sorted, ensure_ascii=False, indent=2)}

<분석 목표>
1. 월별 주요인사이트: 각 월별 매출(V+) 변화와 주요 특징을 분석
2. 채널-아이템 트렌드: 채널별/아이템별 성장/감소 추세와 채널-아이템 조합 분석
3. 전략 포인트: 데이터를 바탕으로 한 구체적인 전략 제안

<요구사항>
{get_common_json_requirement()}

{{
  "title": "월별 채널별 아이템별 매출 추세 분석",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "월별 주요 인사이트",
      "ai_text": "각 월별 매출(V+) 변화와 주요 특징을 분석한 내용. 월별 총 매출(V+) 추이를 바탕으로 성장/감소 패턴, 계절성, 특이사항 등을 분석해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "채널-아이템 트렌드",
      "ai_text": "채널별/아이템별 성장/감소 추세와 채널-아이템 조합 분석 내용. 각 채널의 주요 아이템과 각 아이템의 주요 채널을 분석하고, 강세 조합과 약세 조합을 구체적으로 언급해줘."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "전략 포인트",
      "ai_text": "데이터를 바탕으로 한 구체적인 전략 제안 내용. 채널별/아이템별 최적화 방안, 마케팅 전략, 리스크 관리 등 실행 가능한 전략을 제시해줘."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 월별 주요인사이트: 각 월의 특징과 변화 원인을 분석
- 채널-아이템 트렌드: 채널별 주요 아이템, 아이템별 주요 채널, 성장률, 비중 변화를 분석
- 전략 포인트: 실행 가능한 구체적인 전략 제안

{get_common_prompt_footer()}
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱
        analysis_data = parse_llm_json_response(analysis_response, "월별 채널별 아이템별 매출 추세 분석")
        # 기본 구조 보완
        if len(analysis_data.get('sections', [])) < 3:
            analysis_data['sections'].extend([
                {"div": "종합분석-2", "sub_title": "채널-아이템 트렌드", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "전략 포인트", "ai_text": ""}
            ])
        
        # JSON 데이터 구성
        json_data = {
            'country': 'TW',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm_end,
            'key': '월별채널별아이템별매출추세',
            'analysis_data': analysis_data,
            'summary': {
                'total_sales': round(total_sales / 1000, 0),
                'unique_channels': unique_channels,
                'unique_items': unique_items,
                'unique_months': unique_months,
                'analysis_period': f"{yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 (최근 12개월)"
            },
            'monthly_totals': monthly_totals_k,
            'channel_summary': channel_summary_sorted,
            'item_summary': item_summary_sorted,
            'raw_data': {
                'sample_records': [dict(r) for r in records[:50]],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]
        filename = f"TW_{yyyymm_short}_{brd_cd}_월별채널별아이템별매출추세분석"
        save_json(json_data, filename)
        
        # Markdown도 저장
        markdown_content = f"# {json_data['analysis_data'].get('title', '월별 채널별 아이템별 매출 추세 분석')}\n\n"
        for section in json_data['analysis_data'].get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 월별 채널별 아이템별 매출 추세 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_store_profit_categories(yyyymm, brd_cd):
    """오프라인매장현황분석 (AI 종합분석)"""
    print(f"\n{'='*60}")
    print(f"오프라인 매장 현황 분석 (AI 종합분석) 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # 새로운 통합 쿼리 사용 (카테고리별 분류 포함)
        profit_sql = get_store_profit_query(yyyymm, brd_cd)
        print(f"[DEBUG] 실행 쿼리:\n{profit_sql[:500]}...\n")  # 쿼리 처음 500자만 출력
        profit_df = run_query(profit_sql, engine)
        print(f"[DEBUG] 쿼리 결과 DataFrame shape: {profit_df.shape}")
        print(f"[DEBUG] 컬럼명: {profit_df.columns}")
        profit_records = profit_df.to_dicts()
        
        if not profit_records:
            print("데이터가 없습니다.")
            return None
        
        # 디버깅: 첫 번째 레코드 확인
        if profit_records:
            print(f"[DEBUG] 첫 번째 레코드의 키: {list(profit_records[0].keys())}")
            sample_record = profit_records[0]
            print(f"[DEBUG] 샘플 데이터 (첫 번째 레코드):")
            for key, value in list(sample_record.items())[:10]:  # 처음 10개만 출력
                print(f"  {key}: {value} (type: {type(value).__name__})")
        
        # 데이터베이스에서 반환되는 컬럼명은 대소문자가 다를 수 있으므로 대소문자 모두 확인하는 헬퍼 함수
        def get_value(record, key, default=0):
            # 소문자, 대문자, 원본 모두 시도
            if key.lower() in record:
                value = record[key.lower()]
            elif key.upper() in record:
                value = record[key.upper()]
            elif key in record:
                value = record[key]
            else:
                value = None
            
            if value is None or value == '':
                return float(default)
            try:
                return float(value)
            except (ValueError, TypeError):
                return float(default)
        
        def get_str_value(record, key, default=''):
            # 소문자, 대문자, 원본 모두 시도
            if key.lower() in record:
                return str(record[key.lower()] or default)
            elif key.upper() in record:
                return str(record[key.upper()] or default)
            elif key in record:
                return str(record[key] or default)
            else:
                return default
        
        # 쿼리 결과를 store_profit_data로 변환
        # 쿼리에서 이미 온라인 제외됨 (mgmt_chnl_nm <> '온라인')
        store_profit_data = []
        for record in profit_records:
            # 쿼리에서 반환된 값들은 모두 HKD 단위 (원본 값)
            vat_exc_sale_amt_cy = get_value(record, 'vat_exc_sale_amt_cy', 0)
            vat_exc_sale_amt_py = get_value(record, 'vat_exc_sale_amt_py', 0)
            dcst_cy = get_value(record, 'dcst_cy', 0)
            dcst_py = get_value(record, 'dcst_py', 0)
            cogs_cy = get_value(record, 'cogs_cy', 0)
            cogs_py = get_value(record, 'cogs_py', 0)
            direct_profit = get_value(record, 'dprft', 0)  # 직접이익 (HKD)
            direct_profit_py = get_value(record, 'dprft_py', 0)  # 전년 직접이익 (HKD)
            profit_yoy_pct = get_value(record, 'yoy', 0)
            profit_ratio = get_value(record, 'profit_ratio', 0)
            
            store_profit_data.append({
                'SHOP_CD': get_str_value(record, 'shop_cd', ''),
                'SHOP_NM': get_str_value(record, 'shop_nm', ''),
                'CNTRY_CD': get_str_value(record, 'cntry_cd', ''),
                'MGMT_CHNL_NM': get_str_value(record, 'mgmt_chnl_nm', ''),
                'AREA_SQM': get_value(record, 'area_sqm_cy', 0),
                'VAT_EXC_SALE_AMT': vat_exc_sale_amt_cy,  # 실판가(V-)
                'VAT_EXC_SALE_AMT_PY': vat_exc_sale_amt_py,
                'DCST': dcst_cy,  # 직접비
                'DCST_PY': dcst_py,
                'COGS': cogs_cy,  # 원가
                'COGS_PY': cogs_py,
                'DIRECT_PROFIT': direct_profit,
                'DIRECT_PROFIT_PY': direct_profit_py,
                'PROFIT_YOY_PCT': profit_yoy_pct,
                'PROFIT_RATIO': profit_ratio,
                'SHOP_DIV': get_str_value(record, 'shop_div', ''),
                'SHOP_AREA_DIV': get_str_value(record, 'shop_area_div', ''),
                'RANK': int(get_value(record, 'rank', 0))
            })
        
        # 매장 카테고리별 분류 (당해 직접이익이 0이거나 매출이 0인 매장 제외)
        large_normal_stores = [
            s for s in store_profit_data 
            if s.get('SHOP_AREA_DIV', '') == '대형정상' 
            and s.get('DIRECT_PROFIT', 0) > 0 
            and s.get('VAT_EXC_SALE_AMT', 0) > 0
        ]
        small_normal_stores = [
            s for s in store_profit_data 
            if s.get('SHOP_AREA_DIV', '') == '중소형정상' 
            and s.get('DIRECT_PROFIT', 0) > 0 
            and s.get('VAT_EXC_SALE_AMT', 0) > 0
        ]
        outlet_stores = [
            s for s in store_profit_data 
            if s.get('SHOP_AREA_DIV', '') == '아울렛' 
            and s.get('DIRECT_PROFIT', 0) > 0 
            and s.get('VAT_EXC_SALE_AMT', 0) > 0
        ]
        
        # 전체 요약 통계 계산
        total_stores = len(store_profit_data)
        tw_stores = [s for s in store_profit_data if s['CNTRY_CD'] == 'TW']
        
        total_profit = sum(s['DIRECT_PROFIT'] for s in store_profit_data)
        
        profitable_stores = [s for s in store_profit_data if s['DIRECT_PROFIT'] > 0]
        loss_stores = [s for s in store_profit_data if s['DIRECT_PROFIT'] <= 0]
        
        improving_stores = [s for s in store_profit_data if s['PROFIT_YOY_PCT'] > 0]
        declining_stores = [s for s in store_profit_data if s['PROFIT_YOY_PCT'] < 0]
        
        # 카테고리별 통계 계산
        def calc_category_stats(stores):
            if not stores:
                return {
                    'count': 0,
                    'total_profit_k': 0,
                    'avg_profit_k': 0,
                    'profitable_count': 0,
                    'loss_count': 0,
                    'avg_profit_ratio': 0
                }
            total_profit = sum(s['DIRECT_PROFIT'] for s in stores)
            profitable = len([s for s in stores if s['DIRECT_PROFIT'] > 0])
            avg_profit_ratio = sum(s.get('PROFIT_RATIO', 0) for s in stores) / len(stores) if stores else 0
            return {
                'count': len(stores),
                'total_profit_k': round(total_profit / 1000, 0),
                'avg_profit_k': round(total_profit / len(stores) / 1000, 0) if stores else 0,
                'profitable_count': profitable,
                'loss_count': len(stores) - profitable,
                'avg_profit_ratio': round(avg_profit_ratio, 1)
            }
        
        large_normal_stats = calc_category_stats(large_normal_stores)
        small_normal_stats = calc_category_stats(small_normal_stores)
        outlet_stats = calc_category_stats(outlet_stores)
        
        # 데이터 포맷팅 (K 단위)
        def format_store_data(stores):
            return [{
                'SHOP_NM': s['SHOP_NM'],
                'CNTRY_CD': s['CNTRY_CD'],
                'MGMT_CHNL_NM': s.get('MGMT_CHNL_NM', ''),
                'AREA_SQM': s.get('AREA_SQM', 0),
                'DIRECT_PROFIT_K': round(s['DIRECT_PROFIT'] / 1000, 0),
                'PROFIT_YOY_PCT': s['PROFIT_YOY_PCT'],
                'PROFIT_RATIO': s.get('PROFIT_RATIO', 0),
                'SHOP_DIV': s.get('SHOP_DIV', ''),
                'SHOP_AREA_DIV': s.get('SHOP_AREA_DIV', '')
            } for s in stores]
        
        stores_formatted = format_store_data(store_profit_data)
        
        # 상위/하위 매장 선정
        top_stores = sorted(store_profit_data, key=lambda x: x['DIRECT_PROFIT'], reverse=True)[:10]
        bottom_stores = sorted(store_profit_data, key=lambda x: x['DIRECT_PROFIT'])[:10]
        
        print(f"오프라인 매장 수: {total_stores}개 (대만: {len(tw_stores)}개)")
        print(f"  - 대형 정상 매장: {large_normal_stats['count']}개")
        print(f"  - 중소형 정상 매장: {small_normal_stats['count']}개")
        print(f"  - 아울렛 매장: {outlet_stats['count']}개")
        print(f"흑자 매장: {len(profitable_stores)}개, 적자 매장: {len(loss_stores)}개")
        print(f"직접이익 성장 매장: {len(improving_stores)}개, 역성장 매장: {len(declining_stores)}개")
        print(f"총 직접이익: {total_profit:,.0f} HKD ({total_profit/1000:.0f} K)")
        
        # DPRFT를 사용한 데이터 포맷팅
        def format_store_data_with_dprft(stores):
            return [{
                'SHOP_NM': s['SHOP_NM'],
                'CNTRY_CD': s['CNTRY_CD'],
                'MGMT_CHNL_NM': s.get('MGMT_CHNL_NM', ''),
                'AREA_SQM': s.get('AREA_SQM', 0),
                'DPRFT': round(s['DIRECT_PROFIT'] / 1000, 0),  # 직접이익 (K 단위)
                'PROFIT_YOY_PCT': s['PROFIT_YOY_PCT'],
                'PROFIT_RATIO': s.get('PROFIT_RATIO', 0),
                'SHOP_DIV': s.get('SHOP_DIV', ''),
                'SHOP_AREA_DIV': s.get('SHOP_AREA_DIV', '')
            } for s in stores]
        
        stores_formatted = format_store_data_with_dprft(store_profit_data)
        top_stores_formatted = format_store_data_with_dprft(top_stores)
        bottom_stores_formatted = format_store_data_with_dprft(bottom_stores)
        
        # LLM 프롬프트 생성 (2가지 주제, 각 5줄)
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 대만 오프라인 매장 수익성 전문가야. 오프라인 매장 전체의 직접이익 데이터를 종합 분석하여 핵심 인사이트를 간략하게 5줄씩 정리해줘.

**중요**: 아래 데이터는 당해 직접이익이 0보다 크고 매출이 0보다 큰 매장만 포함되어 있습니다. 휴점 또는 운영 중단 매장은 제외되었습니다.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 총 오프라인 매장 수: {total_stores}개 (대만: {len(tw_stores)}개, 당해 직접이익 > 0 및 매출 > 0인 매장만)
- 총 직접이익: {round(total_profit/1000, 0):,.0f} K
- 흑자 매장: {len(profitable_stores)}개 ({round(len(profitable_stores)/total_stores*100, 1)}%)
- 적자 매장: {len(loss_stores)}개 ({round(len(loss_stores)/total_stores*100, 1)}%)
- 직접이익 성장 매장: {len(improving_stores)}개
- 직접이익 역성장 매장: {len(declining_stores)}개

**카테고리별 요약**
- 대형 정상 매장 (40평 이상): {large_normal_stats['count']}개, 총 직접이익 {large_normal_stats['total_profit_k']} K, 평균 직접이익 {large_normal_stats['avg_profit_k']} K/매장, 흑자 {large_normal_stats['profitable_count']}개, 적자 {large_normal_stats['loss_count']}개, 평균 직접이익률 {large_normal_stats['avg_profit_ratio']}%
- 중소형 정상 매장 (40평 미만): {small_normal_stats['count']}개, 총 직접이익 {small_normal_stats['total_profit_k']} K, 평균 직접이익 {small_normal_stats['avg_profit_k']} K/매장, 흑자 {small_normal_stats['profitable_count']}개, 적자 {small_normal_stats['loss_count']}개, 평균 직접이익률 {small_normal_stats['avg_profit_ratio']}%
- 아울렛 매장: {outlet_stats['count']}개, 총 직접이익 {outlet_stats['total_profit_k']} K, 평균 직접이익 {outlet_stats['avg_profit_k']} K/매장, 흑자 {outlet_stats['profitable_count']}개, 적자 {outlet_stats['loss_count']}개, 평균 직접이익률 {outlet_stats['avg_profit_ratio']}%

**상위 10개 매장 (직접이익 기준)**
{json_dumps_safe(top_stores_formatted, ensure_ascii=False, indent=2)}

**하위 10개 매장 (직접이익 기준)**
{json_dumps_safe(bottom_stores_formatted, ensure_ascii=False, indent=2)}

**전체 오프라인 매장 데이터**
{json_dumps_safe(stores_formatted, ensure_ascii=False, indent=2)}

<분석 목표>
다음 2가지 주제로 종합 분석을 수행해줘 (각 주제당 정확히 5줄로 작성):

1. **오프라인 매장 직접이익 현황 및 성과 분석**
   - 전체 오프라인 매장의 직접이익 구조와 분포 분석
   - 카테고리별(대형 정상/중소형 정상/아울렛) 직접이익 성과 비교
   - 흑자/적자 매장의 특징과 패턴 분석
   - 상위/하위 매장의 성과 요인 분석

2. **전년 대비 변화 원인 및 개선 방안**
   - 직접이익 성장/역성장 매장의 주요 원인 분석
   - 카테고리별 전년 대비 성과 변화 분석
   - 적자 매장의 개선 방안 및 우수 매장 벤치마킹 포인트

<요구사항>
{get_common_json_requirement()}

{{
  "title": "오프라인 매장 현황 분석",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "오프라인 매장 직접이익 현황 및 성과 분석",
      "ai_text": "전체 오프라인 매장의 직접이익 구조와 분포를 분석하고, 카테고리별 성과를 비교 분석한 내용. 흑자/적자 매장의 특징과 패턴, 상위/하위 매장의 성과 요인을 구체적으로 분석한 내용을 정확히 5줄로 요약해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "전년 대비 변화 원인 및 개선 방안",
      "ai_text": "직접이익 성장/역성장 매장의 주요 원인을 분석하고, 카테고리별 전년 대비 성과 변화를 분석한 내용. 적자 매장의 개선 방안 및 우수 매장 벤치마킹 포인트를 제시한 내용을 정확히 5줄로 요약해줘."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 모든 수치는 K 단위 정수로 표기
- 직접이익을 표시할 때는 "직접이익" 용어를 사용해줘 (예: "Nanfang TS MALL의 직접이익은 192 K", "TAIMALL이 직접이익 221 K로 압도적 1위")
- DPRFT라는 약어 대신 "직접이익"이라는 용어를 우선적으로 사용해줘. 필요시 괄호로 DPRFT를 추가할 수 있음 (예: "직접이익(DPRFT)")
- 각 섹션의 ai_text는 반드시 정확히 5줄로 작성해줘. 각 줄은 구체적인 수치와 함께 작성하고, 줄바꿈은 \\n을 사용해줘.
- 예시 형식: "1줄 내용\\n2줄 내용\\n3줄 내용\\n4줄 내용\\n5줄 내용"
- 각 줄은 독립적인 인사이트를 담고 있어야 하며, 구체적인 매장명이나 수치를 포함해줘.
- 구체적인 매장명, 지역, 채널, 수치를 포함하여 작성
- 데이터 기반의 객관적 분석과 실행 가능한 구체적인 개선 방안 제시
- 카테고리별(대형 정상/중소형 정상/아울렛) 차별화된 분석과 전략을 명확히 제시

{get_common_prompt_footer()}
"""
        
        # LLM 호출
        analysis_response = call_llm(prompt, max_tokens=4000)
        analysis_data = parse_llm_json_response(analysis_response, "AI 종합분석")
        
        # 정확히 2개 섹션만 유지 (종합분석-1, 종합분석-2)
        sections = analysis_data.get('sections', [])
        if len(sections) < 2:
            # 섹션이 부족하면 빈 섹션 추가
            while len(sections) < 2:
                idx = len(sections) + 1
                sections.append({
                    "div": f"종합분석-{idx}",
                    "sub_title": f"종합분석 {idx}",
                    "ai_text": ""
                })
        elif len(sections) > 2:
            # 섹션이 많으면 처음 2개만 유지
            sections = sections[:2]
        
        # DIV 값 명시적으로 설정
        sections[0]['div'] = '종합분석-1'
        if len(sections) > 1:
            sections[1]['div'] = '종합분석-2'
        
        analysis_data['sections'] = sections
        
        # JSON 데이터 구성
        json_data = {
            'country': 'TW',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '오프라인매장현황분석',
            'sub_key': '',
            'analysis_data': analysis_data,
            'summary': {
                'total_stores': total_stores,
                'tw_stores': len(tw_stores),
                'profitable_stores': len(profitable_stores),
                'loss_stores': len(loss_stores),
                'improving_stores': len(improving_stores),
                'declining_stores': len(declining_stores),
                'total_profit_k': round(total_profit / 1000, 0),
                'large_normal_stores': large_normal_stats,
                'small_normal_stores': small_normal_stats,
                'outlet_stores': outlet_stats,
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'category_stores': {
                'large_normal': format_store_data(large_normal_stores),
                'small_normal': format_store_data(small_normal_stores),
                'outlet': format_store_data(outlet_stores)
            },
            'top_stores': format_store_data(top_stores),
            'bottom_stores': format_store_data(bottom_stores),
            'store_data': stores_formatted
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]
        filename = f"TW_{yyyymm_short}_{brd_cd}_오프라인매장현황분석_AI종합분석"
        save_json(json_data, filename)
        
        # Markdown 저장
        markdown_content = f"# {analysis_data.get('title', 'AI 종합분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 오프라인 매장 현황 분석 (AI 종합분석) 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_store_profit_by_category(yyyyymm, brd_cd):
    """오프라인매장현황분석 1개 파일 (key=오프라인매장현황분석, sub_key=null) → 섹션 3개: 대형 정상 매장, 중소형 정상 매장, 아울렛 매장 → JSON 1개 + MD 1개"""
    print(f"\n{'='*60}")
    print(f"오프라인 매장 현황 유형별 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")

    engine = get_db_engine()
    try:
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        analysis_period_str = f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"

        # 새로운 통합 쿼리 사용 (카테고리별 분류 포함)
        profit_sql = get_store_profit_query(yyyymm, brd_cd)
        print(f"[DEBUG] 실행 쿼리:\n{profit_sql[:500]}...\n")  # 쿼리 처음 500자만 출력
        profit_df = run_query(profit_sql, engine)
        print(f"[DEBUG] 쿼리 결과 DataFrame shape: {profit_df.shape}")
        print(f"[DEBUG] 컬럼명: {profit_df.columns}")
        profit_records = profit_df.to_dicts()

        if not profit_records:
            print("데이터가 없습니다.")
            return None
        
        # 디버깅: 첫 번째 레코드 확인
        if profit_records:
            print(f"[DEBUG] 첫 번째 레코드의 키: {list(profit_records[0].keys())}")
            sample_record = profit_records[0]
            print(f"[DEBUG] 샘플 데이터 (첫 번째 레코드):")
            for key, value in list(sample_record.items())[:10]:  # 처음 10개만 출력
                print(f"  {key}: {value} (type: {type(value).__name__})")

        # 데이터베이스에서 반환되는 컬럼명은 대소문자가 다를 수 있으므로 대소문자 모두 확인하는 헬퍼 함수
        def get_value(record, key, default=0):
            # 소문자, 대문자, 원본 모두 시도
            if key.lower() in record:
                value = record[key.lower()]
            elif key.upper() in record:
                value = record[key.upper()]
            elif key in record:
                value = record[key]
            else:
                value = None
            
            if value is None or value == '':
                return float(default)
            try:
                return float(value)
            except (ValueError, TypeError):
                return float(default)
        
        def get_str_value(record, key, default=''):
            # 소문자, 대문자, 원본 모두 시도
            if key.lower() in record:
                return str(record[key.lower()] or default)
            elif key.upper() in record:
                return str(record[key.upper()] or default)
            elif key in record:
                return str(record[key] or default)
            else:
                return default

        # 쿼리 결과를 store_profit_data로 변환
        store_profit_data = []
        for record in profit_records:
            # 쿼리에서 반환된 값들은 모두 HKD 단위 (원본 값)
            vat_exc_sale_amt_cy = get_value(record, 'vat_exc_sale_amt_cy', 0)
            vat_exc_sale_amt_py = get_value(record, 'vat_exc_sale_amt_py', 0)
            dcst_cy = get_value(record, 'dcst_cy', 0)
            dcst_py = get_value(record, 'dcst_py', 0)
            cogs_cy = get_value(record, 'cogs_cy', 0)
            cogs_py = get_value(record, 'cogs_py', 0)
            direct_profit = get_value(record, 'dprft', 0)  # 직접이익 (HKD)
            direct_profit_py = get_value(record, 'dprft_py', 0)  # 전년 직접이익 (HKD)
            profit_yoy_pct = get_value(record, 'yoy', 0)
            profit_ratio = get_value(record, 'profit_ratio', 0)
            shop_area_div = get_str_value(record, 'shop_area_div', '')
            
            store_profit_data.append({
                'SHOP_CD': get_str_value(record, 'shop_cd', ''),
                'SHOP_NM': get_str_value(record, 'shop_nm', ''),
                'CNTRY_CD': get_str_value(record, 'cntry_cd', ''),
                'MGMT_CHNL_NM': get_str_value(record, 'mgmt_chnl_nm', ''),
                'AREA_SQM': get_value(record, 'area_sqm_cy', 0),
                'VAT_EXC_SALE_AMT': vat_exc_sale_amt_cy,  # 실판가(V-)
                'VAT_EXC_SALE_AMT_PY': vat_exc_sale_amt_py,
                'DCST': dcst_cy,  # 직접비
                'DCST_PY': dcst_py,
                'COGS': cogs_cy,  # 원가
                'COGS_PY': cogs_py,
                'DIRECT_PROFIT': direct_profit,
                'DIRECT_PROFIT_PY': direct_profit_py,
                'PROFIT_YOY_PCT': profit_yoy_pct,
                'PROFIT_RATIO': profit_ratio,
                'SHOP_AREA_DIV': shop_area_div,
                'RANK': int(get_value(record, 'rank', 0))
            })

        offline_stores = store_profit_data  # 쿼리에서 이미 온라인 제외됨
        if not offline_stores:
            print("오프라인 매장 데이터가 없습니다.")
            return None

        def format_store_data_for_category(stores):
            return [{
                'SHOP_NM': s['SHOP_NM'],
                'CNTRY_CD': s['CNTRY_CD'],
                'SHOP_AREA_DIV': s.get('SHOP_AREA_DIV', ''),
                'AREA_SQM': s.get('AREA_SQM', 0),
                'DIRECT_PROFIT_K': round(s['DIRECT_PROFIT'] / 1000, 0),
                'PROFIT_YOY_PCT': s['PROFIT_YOY_PCT'],
                'PROFIT_RATIO': s.get('PROFIT_RATIO', 0),
            } for s in stores]

        # 3가지 카테고리로 분류 (shop_area_div 사용)
        # shop_area_div: '아울렛', '대형정상', '중소형정상'
        categories = [
            ('대형정상', '대형 정상 매장', '40평 이상이면서 채널이 정상인 매장', lambda s: s.get('SHOP_AREA_DIV', '') == '대형정상'),
            ('중소형정상', '중소형 정상 매장', '40평 미만이면서 채널이 정상인 매장', lambda s: s.get('SHOP_AREA_DIV', '') == '중소형정상'),
            ('아울렛', '아울렛 매장', '아울렛 채널 매장', lambda s: s.get('SHOP_AREA_DIV', '') == '아울렛'),
        ]

        all_sections = []
        for div_value, section_sub_title, category_name, pred in categories:
            # 카테고리별 필터링 + 당해 직접이익이 0이거나 매출이 0인 매장 제외
            filtered = [
                s for s in offline_stores 
                if pred(s) 
                and s.get('DIRECT_PROFIT', 0) > 0 
                and s.get('VAT_EXC_SALE_AMT', 0) > 0
            ]
            
            if not filtered:
                print(f"  [SKIP] {section_sub_title}: 매장 0개 (데이터 없음 또는 당해 직접이익/매출이 0인 매장만 존재)")
                continue
            
            store_list_formatted = format_store_data_for_category(filtered)
            
            # 카테고리별 요약 통계
            total_stores = len(filtered)
            total_profit = sum(s['DIRECT_PROFIT'] for s in filtered)
            avg_profit_per_store = round(total_profit / total_stores / 1000, 0) if total_stores > 0 else 0
            avg_profit_ratio = round(sum(s.get('PROFIT_RATIO', 0) for s in filtered) / total_stores, 1) if total_stores > 0 else 0

            prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 대만 오프라인 매장 수익성 전문가야. 아래는 「{category_name}」만 모은 데이터야. 이 유형 매장에 대한 핵심 인사이트를 정확히 5줄로 요약해줘.

**중요**: 아래 데이터는 당해 직접이익이 0보다 크고 매출이 0보다 큰 매장만 포함되어 있습니다. 휴점 또는 운영 중단 매장은 제외되었습니다.

**분석 기간**: {analysis_period_str} (당해 {yyyymm} vs 전년 {yyyymm_py})

**해당 유형 매장 수**: {total_stores}개 (당해 직접이익 > 0 및 매출 > 0인 매장만)
**평균 직접이익**: {avg_profit_per_store} K/매장
**평균 직접이익률**: {avg_profit_ratio}%

**매장 데이터 (직접이익 K 단위, 전년대비 %, 직접이익률, 면적)**
{json_dumps_safe(store_list_formatted, ensure_ascii=False, indent=2)}

<요구사항>
{get_common_json_requirement()}

{{
  "title": "{category_name} 분석",
  "sections": [
    {{
      "div": "{div_value}",
      "sub_title": "{section_sub_title} 인사이트",
      "ai_text": "이 유형 매장의 핵심 인사이트를 정확히 5줄로 요약해줘. 각 줄은 구체적인 수치와 함께 작성하고, 매장 특징, 성과, 문제점, 개선 방향 등을 포함해줘. 직접이익을 언급할 때는 '직접이익' 또는 'DPRFT' 용어를 사용하되, 일반적으로는 '직접이익'으로 표기해줘."
    }}
  ]
}}

- ai_text는 반드시 5줄로 작성해줘. 각 줄은 구체적인 수치와 함께 작성하고, 줄바꿈은 \\n을 사용해줘.
- 예시 형식: "1줄 내용\\n2줄 내용\\n3줄 내용\\n4줄 내용\\n5줄 내용"
- 각 줄은 독립적인 인사이트를 담고 있어야 하며, 구체적인 매장명이나 수치를 포함해줘.
- 직접이익을 표시할 때는 "직접이익" 용어를 사용해줘 (예: "Nanfang TS MALL의 직접이익은 192 K", "TAIMALL이 직접이익 221 K로 압도적 1위")
- DPRFT라는 약어 대신 "직접이익"이라는 용어를 우선적으로 사용해줘. 필요시 괄호로 DPRFT를 추가할 수 있음 (예: "직접이익(DPRFT)")

<작성 가이드라인>
{get_common_prompt_guidelines()}

{get_common_prompt_footer()}
"""
            analysis_response = call_llm(prompt, max_tokens=2000)
            analysis_data = parse_llm_json_response(analysis_response, f"{category_name} 분석")
            sections = analysis_data.get('sections', [])
            
            # 첫 번째 섹션의 ai_text 사용
            ai_text = sections[0].get('ai_text', '') if sections else ""

            all_sections.append({
                "div": div_value,  # DIV 값을 카테고리명으로 설정 (대형정상, 중소형정상, 아울렛)
                "sub_title": section_sub_title,
                "ai_text": ai_text,
            })
            print(f"  [OK] {section_sub_title}: 매장 {len(filtered)}개")

        analysis_data = {
            "title": "오프라인 매장 현황 분석 (유형별)",
            "sections": all_sections,
        }

        json_data = {
            'country': 'TW',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '오프라인매장현황분석',
            'sub_key': '',
            'analysis_data': analysis_data,
            'summary': {
                'total_stores': len(offline_stores),
                'analysis_period': analysis_period_str,
            },
            'store_data': format_store_data_for_category(offline_stores),
        }

        yyyymm_short = yyyymm[2:]
        filename = f"TW_{yyyymm_short}_{brd_cd}_오프라인매장별현황분석_카테고리별_AI종합분석"
        save_json(json_data, filename)

        markdown_content = f"# {analysis_data.get('title', '오프라인 매장 현황 분석 (유형별)')}\n\n"
        for section in all_sections:
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)

        print(f"[OK] 오프라인 매장 현황 유형별 분석 완료 (1개 파일)\n")
        return None
    finally:
        engine.dispose()

def get_online_store_profit_query(yyyymm, brd_cd):
    """온라인 매장별 직접이익 분석 쿼리 (대만 TW, TWD→HKD 환율 적용)
    
    Returns:
        SQL query for online stores only that includes:
        - shop_cd, shop_nm, mgmt_chnl_nm
        - vat_exc_sale_amt_cy, vat_exc_sale_amt_py (실판가 V-)
        - dcst_cy, dcst_py (직접비)
        - cogs_cy, cogs_py (원가)
        - dprft, dprft_py (직접이익)
        - yoy (year-over-year percentage)
        - profit_ratio (direct profit rate)
        - shop_div (흑자/성장, 흑자/역성장, 적자/성장, 적자/역성장)
    """
    # Calculate previous year month
    current_year = int(yyyymm[:4])
    current_month = int(yyyymm[4:6])
    previous_year = current_year - 1
    yyyymm_py = f"{previous_year:04d}{current_month:02d}"
    
    # Brand filter for main query
    if brd_cd == 'M':
        brd_filter_main = "('M', 'I')"
    else:
        brd_filter_main = f"('{brd_cd}')"
    
    return f"""
with param as (
    select 'CY' as div, '{yyyymm}' as std_end_yyyymm
    union all
    select 'PY' as div, '{yyyymm_py}' as std_end_yyyymm
),
exchn_rate as (
    select source_crncy, exchn_rate
    from comm.hst_exchn_rate a
    join param p on p.std_end_yyyymm between a.efct_start_yyyymm and a.efct_end_yyyymm
        and p.div = 'CY'
    where 1=1
        and target_crncy = 'HKD'
    union all
    select 'HKD', 1
),
raw as (
    select div,
           CNTRY_CD,
           BRD_CD,
           MGMT_CHNL_NM,
           local_shop_cd,
           shop_nm,
           sum(vat_exc_sale_amt) as vat_exc_sale_amt,
           sum(dcst) as dcst,
           sum(cogs) as cogs
    from (
        select p.div,
               b.cntry_cd,
               a.brd_cd,
               b.mgmt_chnl_nm,
               a.local_shop_cd,
               replace(b.local_shop_nm, a.local_shop_cd || ' ', '') as shop_nm,
               sum(vat_exc_act_sale_amt * x.exchn_rate) vat_exc_sale_amt,
               0 dcst,
               0 cogs
        from sap_fnf.dm_hmd_ivtr_shop_prdt_m a
        left join sap_fnf.mst_hmd_shop b on a.local_shop_cd = b.local_shop_cd
            and a.brd_cd = b.brd_cd
            and b.type_nm = 'SHOP'
        join param p on a.yyyymm = p.std_end_yyyymm
        join exchn_rate x on a.currency = x.source_crncy
        group by p.div, b.cntry_cd, a.brd_cd, b.mgmt_chnl_nm, a.local_shop_cd, b.local_shop_nm
        union all
        select p.div,
               b.cntry_cd,
               a.brd_cd,
               b.mgmt_chnl_nm,
               a.local_shop_cd,
               replace(b.local_shop_nm, a.local_shop_cd || ' ', '') as shop_nm,
               0 vat_exc_sale_amt,
               sum(case when cst_type = 'DCST' then amt * x.exchn_rate else 0 end) dcst,
               sum(case when cst_type = 'COGS' then amt * x.exchn_rate else 0 end) cogs
        from sap_fnf.dm_hmd_cst_shop_m a
        left join sap_fnf.mst_hmd_shop b on a.local_shop_cd = b.local_shop_cd
            and a.brd_cd = b.brd_cd
            and b.type_nm = 'SHOP'
        join param p on a.yyyymm = p.std_end_yyyymm
        join exchn_rate x on a.currency = x.source_crncy
        group by p.div, b.cntry_cd, a.brd_cd, b.mgmt_chnl_nm, a.local_shop_cd, b.local_shop_nm
    )
    group by div, CNTRY_CD, BRD_CD, MGMT_CHNL_NM, local_shop_cd, shop_nm
),
main as (
    select a.cntry_cd,
           a.local_shop_cd shop_cd,
           a.shop_nm,
           a.mgmt_chnl_nm,
           sum(case when a.div = 'CY' then a.vat_exc_sale_amt else 0 end) vat_exc_sale_amt_cy,
           sum(case when a.div = 'PY' then a.vat_exc_sale_amt else 0 end) vat_exc_sale_amt_py,
           sum(case when a.div = 'CY' then a.dcst else 0 end) as dcst_cy,
           sum(case when a.div = 'PY' then a.dcst else 0 end) as dcst_py,
           sum(case when a.div = 'CY' then a.cogs else 0 end) as cogs_cy,
           sum(case when a.div = 'PY' then a.cogs else 0 end) as cogs_py
    from raw a
    where 1=1
        and cntry_cd in ('TW')
        and a.brd_cd in {brd_filter_main}
        and mgmt_chnl_nm = '온라인'
    group by a.cntry_cd, a.local_shop_cd, a.mgmt_chnl_nm, a.shop_nm
)
select cntry_cd,
       shop_cd,
       shop_nm,
       mgmt_chnl_nm,
       vat_exc_sale_amt_cy,
       vat_exc_sale_amt_py,
       dcst_cy,
       dcst_py,
       cogs_cy,
       cogs_py,
       (dcst_cy + cogs_cy) as dcst_cogs_cy,
       (dcst_py + cogs_py) as dcst_cogs_py,
       (vat_exc_sale_amt_cy - dcst_cogs_cy) as dprft,
       (vat_exc_sale_amt_py - dcst_cogs_py) as dprft_py,
       case when dprft_py = 0 then 0 else round(dprft / dprft_py * 100) end yoy,
       case when vat_exc_sale_amt_cy = 0 then 0 
            else round((vat_exc_sale_amt_cy - dcst_cogs_cy) / vat_exc_sale_amt_cy * 100, 1) end as profit_ratio,
       row_number() over(order by dprft desc) as rank,
       case when dprft > 0 and yoy > 100 then '흑자/성장'
            when dprft > 0 and yoy <= 100 then '흑자/역성장'
            when dprft <= 0 and yoy > 100 then '적자/성장'
            when dprft <= 0 and yoy <= 100 then '적자/역성장'
       end as shop_div
from main
order by rank
"""

def analyze_online_store_profit(yyyymm, brd_cd):
    """온라인 매장 직접이익 분석"""
    print(f"\n{'='*60}")
    print(f"온라인 매장 직접이익 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # 온라인 매장 쿼리 실행
        profit_sql = get_online_store_profit_query(yyyymm, brd_cd)
        profit_df = run_query(profit_sql, engine)
        profit_records = profit_df.to_dicts()
        
        if not profit_records:
            print("온라인 매장 데이터가 없습니다.")
            return None
        
        # 데이터베이스에서 반환되는 컬럼명은 대소문자가 다를 수 있으므로 대소문자 모두 확인하는 헬퍼 함수
        def get_value(record, key, default=0):
            if key.lower() in record:
                value = record[key.lower()]
            elif key.upper() in record:
                value = record[key.upper()]
            elif key in record:
                value = record[key]
            else:
                value = None
            
            if value is None or value == '':
                return float(default)
            try:
                return float(value)
            except (ValueError, TypeError):
                return float(default)
        
        def get_str_value(record, key, default=''):
            if key.lower() in record:
                return str(record[key.lower()] or default)
            elif key.upper() in record:
                return str(record[key.upper()] or default)
            elif key in record:
                return str(record[key] or default)
            else:
                return default
        
        # 쿼리 결과를 store_profit_data로 변환
        store_profit_data = []
        for record in profit_records:
            vat_exc_sale_amt_cy = get_value(record, 'vat_exc_sale_amt_cy', 0)
            vat_exc_sale_amt_py = get_value(record, 'vat_exc_sale_amt_py', 0)
            dcst_cy = get_value(record, 'dcst_cy', 0)
            dcst_py = get_value(record, 'dcst_py', 0)
            cogs_cy = get_value(record, 'cogs_cy', 0)
            cogs_py = get_value(record, 'cogs_py', 0)
            direct_profit = get_value(record, 'dprft', 0)
            direct_profit_py = get_value(record, 'dprft_py', 0)
            profit_yoy_pct = get_value(record, 'yoy', 0)
            profit_ratio = get_value(record, 'profit_ratio', 0)
            
            store_profit_data.append({
                'SHOP_CD': get_str_value(record, 'shop_cd', ''),
                'SHOP_NM': get_str_value(record, 'shop_nm', ''),
                'CNTRY_CD': get_str_value(record, 'cntry_cd', ''),
                'MGMT_CHNL_NM': get_str_value(record, 'mgmt_chnl_nm', ''),
                'VAT_EXC_SALE_AMT': vat_exc_sale_amt_cy,
                'VAT_EXC_SALE_AMT_PY': vat_exc_sale_amt_py,
                'DCST': dcst_cy,
                'DCST_PY': dcst_py,
                'COGS': cogs_cy,
                'COGS_PY': cogs_py,
                'DIRECT_PROFIT': direct_profit,
                'DIRECT_PROFIT_PY': direct_profit_py,
                'PROFIT_YOY_PCT': profit_yoy_pct,
                'PROFIT_RATIO': profit_ratio,
                'SHOP_DIV': get_str_value(record, 'shop_div', ''),
                'RANK': int(get_value(record, 'rank', 0))
            })
        
        # 전체 요약 통계 계산
        total_stores = len(store_profit_data)
        total_profit = sum(s['DIRECT_PROFIT'] for s in store_profit_data)
        total_sales = sum(s['VAT_EXC_SALE_AMT'] for s in store_profit_data)
        total_dcst = sum(s['DCST'] for s in store_profit_data)
        total_cogs = sum(s['COGS'] for s in store_profit_data)
        
        profitable_stores = [s for s in store_profit_data if s['DIRECT_PROFIT'] > 0]
        loss_stores = [s for s in store_profit_data if s['DIRECT_PROFIT'] <= 0]
        improving_stores = [s for s in store_profit_data if s['PROFIT_YOY_PCT'] > 0]
        declining_stores = [s for s in store_profit_data if s['PROFIT_YOY_PCT'] < 0]
        
        print(f"온라인 매장 수: {total_stores}개")
        print(f"총 실판가(V-): {total_sales:,.0f} HKD ({total_sales/1000:.0f} K)")
        print(f"총 원가: {total_cogs:,.0f} HKD ({total_cogs/1000:.0f} K)")
        print(f"총 직접비: {total_dcst:,.0f} HKD ({total_dcst/1000:.0f} K)")
        print(f"총 직접이익: {total_profit:,.0f} HKD ({total_profit/1000:.0f} K)")
        print(f"흑자 매장: {len(profitable_stores)}개, 적자 매장: {len(loss_stores)}개")
        
        # 데이터 포맷팅
        def format_store_data(stores):
            return [{
                'SHOP_NM': s['SHOP_NM'],
                'CNTRY_CD': s['CNTRY_CD'],
                'MGMT_CHNL_NM': s.get('MGMT_CHNL_NM', ''),
                'VAT_EXC_SALE_AMT_K': round(s['VAT_EXC_SALE_AMT'] / 1000, 0),
                'COGS_K': round(s['COGS'] / 1000, 0),
                'DCST_K': round(s['DCST'] / 1000, 0),
                'DIRECT_PROFIT_K': round(s['DIRECT_PROFIT'] / 1000, 0),
                'PROFIT_YOY_PCT': s['PROFIT_YOY_PCT'],
                'PROFIT_RATIO': s.get('PROFIT_RATIO', 0),
                'SHOP_DIV': s.get('SHOP_DIV', ''),
            } for s in stores]
        
        stores_formatted = format_store_data(store_profit_data)
        top_stores = sorted(store_profit_data, key=lambda x: x['DIRECT_PROFIT'], reverse=True)[:10]
        top_stores_formatted = format_store_data(top_stores)
        
        # LLM 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 대만 온라인 매장 수익성 전문가야. 온라인 매장의 실판가(V-), 원가, 직접비, 직접이익 데이터를 종합 분석하여 핵심 인사이트를 간략하게 8줄로 정리해줘.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 총 온라인 매장 수: {total_stores}개
- 총 실판가(V-): {round(total_sales/1000, 0):,.0f} K
- 총 원가: {round(total_cogs/1000, 0):,.0f} K
- 총 직접비: {round(total_dcst/1000, 0):,.0f} K
- 총 직접이익: {round(total_profit/1000, 0):,.0f} K
- 흑자 매장: {len(profitable_stores)}개 ({round(len(profitable_stores)/total_stores*100, 1) if total_stores > 0 else 0}%)
- 적자 매장: {len(loss_stores)}개 ({round(len(loss_stores)/total_stores*100, 1) if total_stores > 0 else 0}%)
- 직접이익 성장 매장: {len(improving_stores)}개
- 직접이익 역성장 매장: {len(declining_stores)}개

**상위 10개 매장 (직접이익 기준)**
{json_dumps_safe(top_stores_formatted, ensure_ascii=False, indent=2)}

**전체 온라인 매장 데이터**
{json_dumps_safe(stores_formatted, ensure_ascii=False, indent=2)}

<요구사항>
{get_common_json_requirement()}

{{
  "title": "온라인채널인사이트",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "온라인 매장 직접이익 현황 및 성과 분석",
      "ai_text": "온라인 매장의 실판가(V-), 원가, 직접비, 직접이익을 종합 분석하여 핵심 인사이트를 정확히 8줄로 요약해줘. 각 줄은 구체적인 수치와 함께 작성하고, 매장별 특징, 성과, 문제점, 개선 방향, 전년 대비 변화 등을 포함해줘."
    }}
  ]
}}

- ai_text는 반드시 8줄로 작성해줘. 각 줄은 구체적인 수치와 함께 작성하고, 줄바꿈은 \\n을 사용해줘.
- 예시 형식: "1줄 내용\\n2줄 내용\\n3줄 내용\\n4줄 내용\\n5줄 내용\\n6줄 내용\\n7줄 내용\\n8줄 내용"
- 각 줄은 독립적인 인사이트를 담고 있어야 하며, 구체적인 매장명이나 수치를 포함해줘.
- 직접이익을 표시할 때는 "직접이익" 용어를 사용해줘
- 실판가를 표시할 때는 "실판가(V-)" 또는 "매출액(V-)" 용어를 사용해줘
- 매장별 상세 분석, 원가/직접비 구조 분석, 전년 대비 성장률 분석 등을 포함해줘

<작성 가이드라인>
{get_common_prompt_guidelines()}

{get_common_prompt_footer()}
"""
        
        # LLM 호출
        analysis_response = call_llm(prompt, max_tokens=2000)
        analysis_data = parse_llm_json_response(analysis_response, "온라인채널인사이트")
        
        # div 값을 명시적으로 '종합분석-1'로 설정
        sections = analysis_data.get('sections', [])
        if sections:
            sections[0]['div'] = '종합분석-1'
        analysis_data['sections'] = sections
        
        # JSON 데이터 구성
        json_data = {
            'country': 'TW',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'key': '온라인채널분석',
            'sub_key': '',
            'analysis_data': analysis_data,
            'summary': {
                'total_stores': total_stores,
                'total_sales_k': round(total_sales / 1000, 0),
                'total_cogs_k': round(total_cogs / 1000, 0),
                'total_dcst_k': round(total_dcst / 1000, 0),
                'total_profit_k': round(total_profit / 1000, 0),
                'profitable_stores': len(profitable_stores),
                'loss_stores': len(loss_stores),
                'improving_stores': len(improving_stores),
                'declining_stores': len(declining_stores),
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'top_stores': top_stores_formatted,
            'store_data': stores_formatted
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]
        filename = f"TW_{yyyymm_short}_{brd_cd}_온라인매장직접이익분석"
        save_json(json_data, filename)
        
        # Markdown 저장
        markdown_content = f"# {analysis_data.get('title', '온라인채널인사이트')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 온라인 매장 직접이익 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def get_monthly_item_stock_query(yyyymm_end, brd_cd):
    """월별 아이템별 재고 추세 쿼리 (최근 12개월)"""
    # BRD_CD: M 요청 시 I 포함 (I는 M으로 표시)
    if brd_cd == 'M':
        brd_filter = "a.brd_cd in ('M', 'I')"
    else:
        brd_filter = f"a.brd_cd = '{brd_cd}'"
    
    return f"""
with param as (
    select 'CY' as div
        , '{yyyymm_end}' as std_start_yyyymm
        , '{yyyymm_end}' as std_end_yyyymm
    union all
    select 'PY' as div
        , '{int(yyyymm_end[:4]) - 1}{yyyymm_end[4:6]}' as std_start_yyyymm
        , '{int(yyyymm_end[:4]) - 1}{yyyymm_end[4:6]}' as std_end_yyyymm
)
-- cy_item : 당해 아이템 구분 기준
, cy_item as (
    select a.prdt_cd
            , a.sesn
            , a.prdt_hrrc1_nm
            , a.prdt_hrrc2_nm
            , a.prdt_hrrc3_nm
         , a.MIDDLE_CLASS_CD
         , a.ITEM_CD
            , case
                --------------------------------------------------
                -- ACC 분류
                --------------------------------------------------
                when a.prdt_hrrc1_nm='ACC' and a.prdt_hrrc2_nm='Headwear'
                    then  '모자'
                when a.prdt_hrrc1_nm='ACC' and a.prdt_hrrc2_nm='Shoes'
                    then  '신발'
                when a.prdt_hrrc1_nm='ACC' and a.prdt_hrrc2_nm='Bag'
                    then  '가방'
                when a.prdt_hrrc1_nm='ACC' and a.prdt_hrrc2_nm='Acc_etc'
                    then  '기타 ACC'
                --------------------------------------------------
                -- 의류 분류
                --------------------------------------------------
                -- 당시즌 (SN 통합)
                when a.prdt_hrrc1_nm='의류' and param.STD_END_YYYYMM between b.start_yyyymm and b.end_yyyymm
                    then replace(a.sesn, 'N', 'S') || ' ' || a.prdt_hrrc1_nm
                --------------------------------------------------
                -- 전시즌 (조회기준 월이 9~2일때만 존재)
                when a.prdt_hrrc1_nm='의류' and right(param.STD_END_YYYYMM, 2)::int in (9,10,11,12,1,2)
                        and TO_CHAR(ADD_MONTHS(TO_DATE(param.STD_END_YYYYMM, 'YYYYMM'), -6), 'YYYYMM') between b.start_yyyymm and b.end_yyyymm
                    then replace(a.sesn, 'N', 'S') || ' ' || a.prdt_hrrc1_nm
                --------------------------------------------------
                -- 차기시즌
                when a.prdt_hrrc1_nm='의류' and b.START_YYYYMM > param.STD_END_YYYYMM
                        then '차기시즌 ' || a.prdt_hrrc1_nm
                --------------------------------------------------
                --------------------------------------------------
                -- 과시즌
                    when a.prdt_hrrc1_nm='의류' and to_char(add_months(to_date(param.STD_END_YYYYMM, 'YYYYMM'), -12), 'YYYYMM') >= b.start_yyyymm
                            then  '과시즌' || right(replace(a.sesn, 'N', 'S'),1) || ' ' || a.prdt_hrrc1_nm
                    else '미지정' end as item_std

    from sap_fnf.mst_prdt a
    left join comm.mst_sesn b
        on a.sesn = b.sesn
    join param
        on param.div = 'CY'
    where 1=1
    and a.sesn <> 'X'
)
-- 채널 기준
, channel_std as (
    select local_shop_cd
        , cntry_cd
        , local_shop_nm
        , brd_cd
        , decode(cntry_cd, 'HK', '홍콩', 'MO', '마카오', 'TW', '대만') as cntry_nm
        , mgmt_chnl_nm as chnl_nm
    from sap_fnf.mst_hmd_shop
    where cntry_cd in ('TW')
      and type_nm = 'SHOP'
    group by local_shop_cd
            , cntry_cd
            , local_shop_nm
            , mgmt_chnl_nm
            , brd_cd
)
-- 환율
, exchn_rate as (
    select source_crncy
        , exchn_rate
    from comm.hst_exchn_rate a
    join param p
    on p.std_end_yyyymm between a.efct_start_yyyymm and a.efct_end_yyyymm
    and p.div = 'CY'
    where 1=1
    and target_crncy = 'HKD'
    union all
    select 'HKD', 1
)
-- 최종쿼리
select a.yyyymm
        , ci.item_std
        , sum(a.tag_stock_amt) as amt
    from sap_fnf.prep_hmd_stock a
        join param p
        on p.div = 'CY'
        join (select distinct sesn, middle_class_cd, item_cd, item_std
            from cy_item
            ) ci
        on a.sesn = ci.sesn
        and a.ctgr = ci.middle_class_cd
        and a.sub_ctgr = ci.item_cd
    where a.cntry_cd in ('TW')
    and {brd_filter}
    and a.yyyymm between to_char(add_months(to_date(p.STD_END_YYYYMM, 'YYYYMM'), -11), 'YYYYMM') and p.std_end_yyyymm -- 최근 1년
group by a.yyyymm, ci.item_std
order by a.yyyymm desc, ci.item_std
"""

def analyze_monthly_item_stock_trend(yyyymm, brd_cd):
    """월별 아이템별 재고 추세 분석 (최근 12개월)"""
    print(f"\n{'='*60}")
    print(f"월별 아이템별 재고 추세 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (최근 12개월)
        analysis_year = int(yyyymm[:4])
        analysis_month = int(yyyymm[4:6])
        
        # 최근 12개월 계산 (yyyymm 포함하여 12개월 전까지)
        # 예: 202601 -> 202502부터 202601까지 (2025년 2월~2026년 1월)
        if analysis_month == 12:
            start_year = analysis_year - 1
            start_month = 1
        else:
            start_year = analysis_year - 1
            start_month = analysis_month + 1
        
        yyyymm_start = f"{start_year:04d}{start_month:02d}"
        yyyymm_end = yyyymm
        
        print(f"분석 기간: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 (최근 12개월)")
        
        # SQL 쿼리 실행
        sql = get_monthly_item_stock_query(yyyymm_end, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_stock = sum(float(r.get('AMT', 0) or 0) for r in records)
        unique_items = len(set(r.get('ITEM_STD', '') for r in records if r.get('ITEM_STD')))
        unique_months = len(set(r.get('YYYYMM', '') for r in records))
        
        print(f"총 재고액: {total_stock:,.0f} HKD ({total_stock/1000:.0f} K)")
        print(f"아이템 수: {unique_items}개")
        print(f"분석 월 수: {unique_months}개월")
        
        # 데이터 가공: 월별/아이템별 집계
        monthly_data = {}
        item_data = {}
        
        for r in records:
            yyyymm_val = r.get('YYYYMM', '')
            item_std = r.get('ITEM_STD', '기타')
            stock_amt = float(r.get('AMT', 0) or 0)
            
            # 월별 데이터 집계
            if yyyymm_val not in monthly_data:
                monthly_data[yyyymm_val] = {'total': 0, 'items': {}}
            monthly_data[yyyymm_val]['total'] += stock_amt
            
            if item_std not in monthly_data[yyyymm_val]['items']:
                monthly_data[yyyymm_val]['items'][item_std] = 0
            monthly_data[yyyymm_val]['items'][item_std] += stock_amt
            
            # 아이템별 데이터 집계
            if item_std not in item_data:
                item_data[item_std] = {'total': 0, 'months': {}}
            item_data[item_std]['total'] += stock_amt
            
            if yyyymm_val not in item_data[item_std]['months']:
                item_data[item_std]['months'][yyyymm_val] = 0
            item_data[item_std]['months'][yyyymm_val] += stock_amt
        
        # 월별 총 재고 (k 단위)
        monthly_totals_k = {k: round(v['total'] / 1000, 0) for k, v in sorted(monthly_data.items())}
        
        # 아이템별 총 재고 및 월별 추이 (k 단위)
        item_summary = {}
        for item_std, data in item_data.items():
            item_summary[item_std] = {
                'total': round(data['total'] / 1000, 0),
                'months': {k: round(v / 1000, 0) for k, v in sorted(data['months'].items())}
            }
        
        # 아이템별 정렬 (총 재고 기준 내림차순)
        item_summary_sorted = dict(sorted(item_summary.items(), key=lambda x: x[1]['total'], reverse=True))
        
        # LLM 분석 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 대만 재고 관리 전문가야. 월별 아이템별 재고 추세 분석을 수행해줘.

**분석 기간**: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 ({yyyymm_start}~{yyyymm_end}) (최근 12개월)

**월별 총 재고 추이** (모든 금액은 K 단위, 천 단위):
{json_dumps_safe(monthly_totals_k, ensure_ascii=False, indent=2)}

**아이템별 재고 데이터** (모든 금액은 K 단위, 천 단위):
{json_dumps_safe(item_summary_sorted, ensure_ascii=False, indent=2)}

<분석 목표>
1. 월별 주요인사이트: 각 월별 재고 변화와 주요 특징을 분석
2. 아이템 트렌드: 아이템별 재고 추세와 변화 패턴 분석
3. 전략 포인트: 데이터를 바탕으로 한 구체적인 재고 관리 전략 제안

<요구사항>
{get_common_json_requirement()}

{{
  "title": "월별 아이템별 재고 추세 분석",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "월별 주요 인사이트",
      "ai_text": "각 월별 재고 변화와 주요 특징을 분석한 내용. 월별 총 재고 추이를 바탕으로 증가/감소 패턴, 계절성, 특이사항 등을 분석해줘."
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "아이템 트렌드",
      "ai_text": "아이템별 재고 추세와 변화 패턴 분석 내용. 각 아이템의 월별 추이를 분석하고, 재고가 많은 아이템과 적은 아이템을 구체적으로 언급해줘."
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "전략 포인트",
      "ai_text": "데이터를 바탕으로 한 구체적인 재고 관리 전략 제안 내용. 아이템별 최적화 방안, 재고 회전율 개선, 리스크 관리 등 실행 가능한 전략을 제시해줘."
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- 월별 주요인사이트: 각 월의 특징과 변화 원인을 분석
- 아이템 트렌드: 아이템별 재고 추세, 비중 변화, 아이템 간 비교를 분석
- 전략 포인트: 실행 가능한 구체적인 재고 관리 전략 제안

{get_common_prompt_footer()}
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱
        analysis_data = parse_llm_json_response(analysis_response, "월별 아이템별 재고 추세 분석")
        # 기본 구조 보완
        if len(analysis_data.get('sections', [])) < 3:
            analysis_data['sections'].extend([
                {"div": "종합분석-2", "sub_title": "아이템 트렌드", "ai_text": ""},
                {"div": "종합분석-3", "sub_title": "전략 포인트", "ai_text": ""}
            ])
        
        # JSON 데이터 구성
        json_data = {
            'country': 'TW',
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm_end,
            'key': '월별아이템별재고추세',
            'analysis_data': analysis_data,
            'summary': {
                'total_stock': round(total_stock / 1000, 0),
                'unique_items': unique_items,
                'unique_months': unique_months,
                'analysis_period': f"{yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 (최근 12개월)"
            },
            'monthly_totals': monthly_totals_k,
            'item_summary': item_summary_sorted,
            'raw_data': {
                'sample_records': [dict(r) for r in records[:50]],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        yyyymm_short = yyyymm[2:]
        filename = f"TW_{yyyymm_short}_{brd_cd}_월별아이템별재고추세분석"
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
    # M과 I는 함께 분석, X는 따로 분석
    brands_to_analyze = [
        ('M', 'I'),  # MLB + MLB KIDS (함께 분석)
        'X',         # DISCOVERY (따로 분석)
    ]
    
    # 기간별, 브랜드별 분석 실행
    for yyyymm in yyyymm_list:
        print(f"\n{'='*60}")
        print(f"기간 분석 시작: {yyyymm} ({yyyymm[:4]}년 {yyyymm[4:6]}월)")
        print(f"{'='*60}\n")
        
        for brand_item in brands_to_analyze:
            # 브랜드 그룹 처리
            if isinstance(brand_item, tuple):
                # M과 I를 함께 분석 (M으로 통합하여 처리, 쿼리에서 I 포함)
                brd_cd = 'M'
                brand_names = ' + '.join([BRAND_CODE_MAP.get(b, b) for b in brand_item])
                print(f"\n{'='*60}")
                print(f"브랜드 분석 시작: {brand_names} (M+I 통합)")
                print(f"{'='*60}\n")
            else:
                # 단일 브랜드 (X 등)
                brd_cd = brand_item
                brand_names = BRAND_CODE_MAP.get(brd_cd, brd_cd)
                print(f"\n{'='*60}")
                print(f"브랜드 분석 시작: {brd_cd} ({brand_names})")
                print(f"{'='*60}\n")
            
            try:
                # 분석 실행 (원하는 분석만 주석 해제)
                analyze_channel_sales(yyyymm, brd_cd)  # 실판매출_채널별매출분석
                analyze_store_profit(yyyymm, brd_cd)  # 영업이익_매장별직접이익
                analyze_operating_expense(yyyymm, brd_cd)  # 영업비_AI종합분석
                analyze_discount_rate_overall(yyyymm, brd_cd)  # 할인율_AI종합분석
                analyze_store_efficiency_overall(yyyymm, brd_cd)  # 매장효율성_AI종합분석 (평당매출 포함)
                analyze_monthly_channel_sales_trend(yyyymm, brd_cd)  # 월별 채널별 매출 추세 분석 (최근 12개월)
                analyze_monthly_channel_item_sales_trend(yyyymm, brd_cd)  # 월별 채널별 아이템별 매출 추세 분석 (최근 12개월)
                analyze_monthly_item_stock_trend(yyyymm, brd_cd)  # 월별 아이템별 재고 추세 분석 (최근 12개월)
                analyze_store_profit_categories(yyyymm, brd_cd)  # 오프라인매장현황분석 (AI 종합분석)
                analyze_store_profit_by_category(yyyymm, brd_cd)  # 오프라인매장현황분석 1개 (섹션 3개: 대형정상, 중소형정상, 아울렛)
                analyze_online_store_profit(yyyymm, brd_cd)  # 온라인매장직접이익분석
                pass  # 주석 처리된 함수가 없을 경우를 위한 pass
            except Exception as e:
                print(f"[ERROR] 브랜드 {brd_cd} 분석 중 오류 발생: {e}")
                print(f"[ERROR] 다음 브랜드로 계속 진행합니다...\n")
                continue
    
    # 종료 시간 기록 및 토큰 사용량 출력
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    total_tokens = get_total_tokens()
    
    print(f"\n{'='*60}")
    print(f"분석 종료 시간: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"총 소요 시간: {elapsed_time}")
    print(f"총 토큰 사용량: 입력 {total_tokens['input']:,} 토큰, 출력 {total_tokens['output']:,} 토큰, 합계 {total_tokens['input'] + total_tokens['output']:,} 토큰")
    print(f"{'='*60}\n")
