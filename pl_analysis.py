"""
간단한 분석 도구 - 모든 기능이 하나의 파일에 통합됨
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

OUTPUT_JSON_PATH = './output/json'
OUTPUT_MD_PATH = './output/md'

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
- 모든 금액은 백만원 단위로 표시 (원본 데이터를 1,000,000으로 나누어 표기)
- 단위는 백만원, 3자리마다 쉼표 표기
- ⚠️ **중요: 백만원 단위 표시 시 반드시 정수로 표기하고 소수점을 사용하지 말 것**
  - 올바른 예: 1,234백만원, 588백만원, 1,378백만원
  - 잘못된 예: 1,234.56백만원, 588.67백만원, 1,378.0백만원 (절대 사용 금지)
  - 소수점이 있는 경우 반올림하여 정수로 표기 (예: 588.67 → 589백만원, 1,378.0 → 1,378백만원)
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
    file_path = os.path.join(OUTPUT_MD_PATH, f"{filename}.md")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
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

def save_json(data, filename):
    """JSON 파일 저장"""
    file_path = os.path.join(OUTPUT_JSON_PATH, f"{filename}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, cls=DecimalEncoder)
    print(f"[OK] JSON 저장: {file_path}")
    return file_path

# ============================================================================
# SQL 쿼리 예시
# ============================================================================
def get_channel_sales_cypy_query(yyyymm, yyyymm_py, brd_cd):
    """채널별 매출 top3 분석 쿼리 (당해/전년 동월 비교) - 4-1-1-1용"""
    return f"""
    WITH raw AS (
        SELECT pst_yyyymm,
               CASE 
                   WHEN b.mgmt_chnl_cd = '4' THEN '자사몰'
                   WHEN b.mgmt_chnl_cd = '5' THEN '제휴몰'
                   WHEN b.mgmt_chnl_cd IN ('3', '11', 'C3') THEN '직영점'
                   WHEN b.mgmt_chnl_nm LIKE '아울렛%' THEN '아울렛'
                   ELSE b.mgmt_chnl_nm
               END AS chnl_nm,
               c.prdt_hrrc3_nm AS class3,
               SUM(a.act_sale_amt) AS sale_amt
        FROM sap_fnf.dm_pl_shop_prdt_m a
        JOIN sap_fnf.mst_shop b 
            ON a.brd_cd = b.brd_cd
           AND a.shop_cd = b.sap_shop_cd
        JOIN sap_fnf.mst_prdt c
            ON a.prdt_cd = c.prdt_cd
        WHERE 1=1
          AND a.corp_cd = '1000'
          AND a.brd_cd = '{brd_cd}'
          AND a.chnl_cd NOT IN ('0', '8', '9', '99')
          AND a.pst_yyyymm IN ('{yyyymm}', '{yyyymm_py}')
        GROUP BY 1, 2, 3
    ), main AS (
        SELECT pst_yyyymm,
               chnl_nm,
               class3,
               sale_amt,
               sale_amt_chnl_ttl,
               DENSE_RANK() OVER(PARTITION BY pst_yyyymm ORDER BY sale_amt_chnl_ttl DESC) AS in_yymm_rnk,
               DENSE_RANK() OVER(PARTITION BY pst_yyyymm, chnl_nm ORDER BY sale_amt DESC) AS in_chnl_rnk
        FROM (
            SELECT pst_yyyymm,
                   chnl_nm,
                   class3,
                   sale_amt,
                   SUM(sale_amt) OVER(PARTITION BY pst_yyyymm, chnl_nm) AS sale_amt_chnl_ttl
            FROM raw
        )
    )
    SELECT pst_yyyymm,
           chnl_nm,
           class3,
           sale_amt,
           sale_amt_chnl_ttl,
           CASE WHEN sale_amt_chnl_ttl = 0 THEN 0 ELSE ROUND(sale_amt / sale_amt_chnl_ttl * 100) END AS sale_ratio,
           in_yymm_rnk,
           in_chnl_rnk
    FROM main 
    ORDER BY pst_yyyymm DESC, in_yymm_rnk, in_chnl_rnk
    """

def get_gender_purchase_pattern_query(yyyymm, yyyymm_py, brd_cd):
    """성별 구매 패턴 분석 쿼리 (당해/전년 동월 비교) - 4-1-3-1용"""
    # 년월을 날짜로 변환
    current_year = int(yyyymm[:4])
    current_month = int(yyyymm[4:6])
    previous_year = int(yyyymm_py[:4])
    previous_month = int(yyyymm_py[4:6])
    
    current_start = f"{current_year}-{current_month:02d}-01"
    current_end = f"{current_year}-{current_month:02d}-{28 if current_month == 2 else 30 if current_month in [4,6,9,11] else 31}"
    previous_start = f"{previous_year}-{previous_month:02d}-01"
    previous_end = f"{previous_year}-{previous_month:02d}-{28 if previous_month == 2 else 30 if previous_month in [4,6,9,11] else 31}"
    
    return f"""
    SELECT
      TO_CHAR(a.PST_DT, 'YYYY-MM') AS YYYY_MM,
      b.SEX_NM,
      PRDT_HRRC1_NM,
      PRDT_HRRC2_NM,
      PRDT_HRRC3_NM,
      SUM(a.SALE_QTY) AS SALE_QTY,
      SUM(a.ACT_SALE_AMT) AS ACT_SALE_AMT
    FROM sap_fnf.dw_copa_d a
    JOIN sap_fnf.mst_prdt b
      ON a.prdt_cd = b.prdt_cd
    WHERE a.CHNL_CD NOT IN ('0','8','9','99')
      AND a.PRDT_CD IS NOT NULL
      AND a.PRDT_CD <> ''
      AND (
        a.PST_DT BETWEEN '{previous_start}' AND '{previous_end}'
        OR a.PST_DT BETWEEN '{current_start}' AND '{current_end}'
      )
      AND a.BRD_CD = '{brd_cd}'
      AND a.ACT_SALE_AMT <> 0
    GROUP BY
      TO_CHAR(a.PST_DT, 'YYYY-MM'),
      b.SEX_NM,
      PRDT_HRRC1_NM,
      PRDT_HRRC2_NM,
      PRDT_HRRC3_NM
    ORDER BY
      YYYY_MM,
      b.SEX_NM,
      PRDT_HRRC1_NM,
      PRDT_HRRC2_NM,
      PRDT_HRRC3_NM
    """


def get_gender_purchase_pattern_overall_query(yyyymm_start, yyyymm_end, brd_cd):
    """성별 구매 패턴 종합분석 쿼리 (12개월 추이) - 4-1-3-2용"""
    # 시작 년월을 날짜로 변환
    start_year = int(yyyymm_start[:4])
    start_month = int(yyyymm_start[4:6])
    start_date = f"{start_year}-{start_month:02d}-01"
    
    # 종료 년월을 날짜로 변환
    end_year = int(yyyymm_end[:4])
    end_month = int(yyyymm_end[4:6])
    end_day = 28 if end_month == 2 else 30 if end_month in [4,6,9,11] else 31
    end_date = f"{end_year}-{end_month:02d}-{end_day}"
    
    return f"""
    SELECT
      TO_CHAR(a.PST_DT, 'YYYY-MM') AS YYYY_MM,
      b.SEX_NM,
      PRDT_HRRC1_NM,
      PRDT_HRRC2_NM,
      PRDT_HRRC3_NM,
      SUM(a.SALE_QTY) AS SALE_QTY,
      SUM(a.ACT_SALE_AMT) AS ACT_SALE_AMT
    FROM sap_fnf.dw_copa_d a
    JOIN sap_fnf.mst_prdt b
      ON a.prdt_cd = b.prdt_cd
    WHERE a.CHNL_CD NOT IN ('0','8','9','99')
      AND a.PRDT_CD IS NOT NULL
      AND a.PRDT_CD <> ''
      AND a.PST_DT BETWEEN '{start_date}' AND '{end_date}'
      AND a.BRD_CD = '{brd_cd}'
      AND a.ACT_SALE_AMT <> 0
    GROUP BY
      TO_CHAR(a.PST_DT, 'YYYY-MM'),
      b.SEX_NM,
      PRDT_HRRC1_NM,
      PRDT_HRRC2_NM,
      PRDT_HRRC3_NM
    ORDER BY
      YYYY_MM,
      b.SEX_NM,
      PRDT_HRRC1_NM,
      PRDT_HRRC2_NM,
      PRDT_HRRC3_NM
    """

def get_category_profit_analysis_query(yyyymm, yyyymm_py, brd_cd):
    """카테고리별 수익성 분석 쿼리 (당해/전년 동월 비교) - 5-2-1-1용"""
    # 년월을 날짜로 변환
    current_year = int(yyyymm[:4])
    current_month = int(yyyymm[4:6])
    previous_year = int(yyyymm_py[:4])
    previous_month = int(yyyymm_py[4:6])
    
    current_start = f"{current_year}-{current_month:02d}-01"
    current_end = f"{current_year}-{current_month:02d}-{28 if current_month == 2 else 30 if current_month in [4,6,9,11] else 31}"
    previous_start = f"{previous_year}-{previous_month:02d}-01"
    previous_end = f"{previous_year}-{previous_month:02d}-{28 if previous_month == 2 else 30 if previous_month in [4,6,9,11] else 31}"
    
    return f"""
    SELECT
      TO_CHAR(a.PST_DT, 'YYYY-MM') AS YYYY_MM,
      b.PRDT_NM,
      PRDT_HRRC1_NM,
      PRDT_HRRC2_NM,
      PRDT_HRRC3_NM,
      SUM(a.SALE_QTY) AS SALE_QTY,
      SUM(a.ACT_SALE_AMT) AS ACT_SALE_AMT,
      SUM(VAT_EXC_ACT_SALE_AMT) - SUM(SALE_CMS) - SUM(ACT_COGS) - SUM(STK_ASST_APRCT_AMT) - SUM(VLTN_AMT) AS SALE_TTL_PRFT
    FROM sap_fnf.dw_copa_d a
    JOIN sap_fnf.mst_prdt b
      ON a.prdt_cd = b.prdt_cd
    WHERE a.CHNL_CD NOT IN ('0','8','9','99')
      AND a.PRDT_CD IS NOT NULL
      AND a.PRDT_CD <> ''
      AND (
        a.PST_DT BETWEEN '{previous_start}' AND '{previous_end}'
        OR a.PST_DT BETWEEN '{current_start}' AND '{current_end}'
      )
      AND a.BRD_CD = '{brd_cd}'
      AND a.ACT_SALE_AMT <> 0
    GROUP BY
      TO_CHAR(a.PST_DT, 'YYYY-MM'),
      PRDT_HRRC1_NM,
      PRDT_HRRC2_NM,
      PRDT_HRRC3_NM,
      PRDT_NM
    ORDER BY
      PRDT_HRRC1_NM,
      PRDT_HRRC2_NM,
      PRDT_HRRC3_NM,
      PRDT_NM
    """

def get_category_profit_overall_query(yyyymm_start, yyyymm_end, brd_cd):
    """카테고리별 수익성 종합분석 쿼리 (12개월 추이) - 5-2-1-2용"""
    # 시작 년월을 날짜로 변환
    start_year = int(yyyymm_start[:4])
    start_month = int(yyyymm_start[4:6])
    start_date = f"{start_year}-{start_month:02d}-01"
    
    # 종료 년월을 날짜로 변환
    end_year = int(yyyymm_end[:4])
    end_month = int(yyyymm_end[4:6])
    end_day = 28 if end_month == 2 else 30 if end_month in [4,6,9,11] else 31
    end_date = f"{end_year}-{end_month:02d}-{end_day}"
    
    return f"""
    SELECT
      TO_CHAR(a.PST_DT, 'YYYY-MM') AS YYYY_MM,
      b.PRDT_NM,
      PRDT_HRRC1_NM,
      PRDT_HRRC2_NM,
      PRDT_HRRC3_NM,
      SUM(a.SALE_QTY) AS SALE_QTY,
      SUM(a.ACT_SALE_AMT) AS ACT_SALE_AMT,
      SUM(VAT_EXC_ACT_SALE_AMT) - SUM(SALE_CMS) - SUM(ACT_COGS) - SUM(STK_ASST_APRCT_AMT) - SUM(VLTN_AMT) AS SALE_TTL_PRFT
    FROM sap_fnf.dw_copa_d a
    JOIN sap_fnf.mst_prdt b
      ON a.prdt_cd = b.prdt_cd
    WHERE a.CHNL_CD NOT IN ('0','8','9','99')
      AND a.PRDT_CD IS NOT NULL
      AND a.PRDT_CD <> ''
      AND a.PST_DT BETWEEN '{start_date}' AND '{end_date}'
      AND a.BRD_CD = '{brd_cd}'
      AND a.ACT_SALE_AMT <> 0
    GROUP BY
      TO_CHAR(a.PST_DT, 'YYYY-MM'),
      PRDT_HRRC1_NM,
      PRDT_HRRC2_NM,
      PRDT_HRRC3_NM,
      PRDT_NM
    ORDER BY
      PRDT_HRRC1_NM,
      PRDT_HRRC2_NM,
      PRDT_HRRC3_NM,
      PRDT_NM
    """

def get_channel_sales_query(yyyymm_start, yyyymm_end, brd_cd):
    """채널별 매출 분석 쿼리 (12개월 추이)"""
    return f"""
    WITH raw AS (
        SELECT pst_yyyymm,
               CASE 
                   WHEN b.mgmt_chnl_cd = '4' THEN '자사몰'
                   WHEN b.mgmt_chnl_cd = '5' THEN '제휴몰'
                   WHEN b.mgmt_chnl_cd IN ('3', '11', 'C3') THEN '직영점'
                   WHEN b.mgmt_chnl_nm LIKE '아울렛%' THEN '아울렛'
                   ELSE b.mgmt_chnl_nm
               END AS chnl_nm,
               c.prdt_hrrc3_nm AS class3,
               SUM(a.act_sale_amt) AS sale_amt
        FROM sap_fnf.dm_pl_shop_prdt_m a
        JOIN sap_fnf.mst_shop b 
            ON a.brd_cd = b.brd_cd
           AND a.shop_cd = b.sap_shop_cd
        JOIN sap_fnf.mst_prdt c
            ON a.prdt_cd = c.prdt_cd
        WHERE 1=1
          AND a.corp_cd = '1000'
          AND a.brd_cd = '{brd_cd}'
          AND a.chnl_cd NOT IN ('0', '8', '9', '99')
          AND a.pst_yyyymm BETWEEN '{yyyymm_start}' AND '{yyyymm_end}'
        GROUP BY 1, 2, 3
    ), main AS (
        SELECT pst_yyyymm,
               chnl_nm,
               class3,
               sale_amt,
               sale_amt_chnl_ttl,
               DENSE_RANK() OVER(PARTITION BY pst_yyyymm ORDER BY sale_amt_chnl_ttl DESC) AS in_yymm_rnk,
               DENSE_RANK() OVER(PARTITION BY pst_yyyymm, chnl_nm ORDER BY sale_amt DESC) AS in_chnl_rnk
        FROM (
            SELECT pst_yyyymm,
                   chnl_nm,
                   class3,
                   sale_amt,
                   SUM(sale_amt) OVER(PARTITION BY pst_yyyymm, chnl_nm) AS sale_amt_chnl_ttl
            FROM raw
        )
    )
    SELECT pst_yyyymm,
           chnl_nm,
           class3,
           sale_amt,
           sale_amt_chnl_ttl,
           CASE WHEN sale_amt_chnl_ttl = 0 THEN 0 ELSE ROUND(sale_amt / sale_amt_chnl_ttl * 100) END AS sale_ratio,
           in_yymm_rnk,
           in_chnl_rnk
    FROM main 
    ORDER BY pst_yyyymm DESC, in_yymm_rnk, in_chnl_rnk
    """

def get_ad_expense_detail_query(yyyymm, yyyymm_py, brd_cd, ctgr1=None):
    """영업비 당해/전년 세부 내역 쿼리 (CTGR1별 또는 전체)"""
    ctgr1_filter = f"AND CTGR1 = '{ctgr1}'" if ctgr1 else ""
    return f"""
    SELECT PST_YYYYMM, CTGR1, CTGR2, CTGR3, GL_NM, SUM(TTL_USE_AMT) AS AD_TTL_AMT
    FROM SAP_FNF.DM_IDCST_CCTR_M
    WHERE BRD_CD = '{brd_cd}'
      AND PST_YYYYMM = '{yyyymm}'
      {ctgr1_filter}
    GROUP BY PST_YYYYMM, BRD_NM, CTGR1, CTGR2, CTGR3, GL_NM
    
    UNION ALL
    
    SELECT PST_YYYYMM, CTGR1, CTGR2, CTGR3, GL_NM, SUM(TTL_USE_AMT) AS AD_TTL_AMT
    FROM SAP_FNF.DM_IDCST_CCTR_M
    WHERE BRD_CD = '{brd_cd}'
      AND PST_YYYYMM = '{yyyymm_py}'
      {ctgr1_filter}
    GROUP BY PST_YYYYMM, BRD_NM, CTGR1, CTGR2, CTGR3, GL_NM
    ORDER BY AD_TTL_AMT DESC
    """

def get_ad_expense_trend_query(trend_months, brd_cd, ctgr1=None):
    """영업비 12개월 추세 세부 내역 쿼리 (CTGR1별 또는 전체)"""
    trend_months_str = "', '".join(trend_months)
    ctgr1_filter = f"AND CTGR1 = '{ctgr1}'" if ctgr1 else ""
    return f"""
    SELECT PST_YYYYMM,
           CTGR1,
           CTGR2,
           CTGR3,
           GL_NM,
           SUM(TTL_USE_AMT) AS TTL_USE_AMT
    FROM SAP_FNF.DM_IDCST_CCTR_M
    WHERE PST_YYYYMM IN ('{trend_months_str}')
      AND BRD_CD = '{brd_cd}'
      {ctgr1_filter}
    GROUP BY PST_YYYYMM, CTGR1, CTGR2, CTGR3, GL_NM
    ORDER BY PST_YYYYMM, TTL_USE_AMT DESC
    """



# ============================================================================
# 분석 함수
# ============================================================================
def analyze_channel_sales(yyyymm, brd_cd):
    """채널별_매출_top3_분석(당해_전년_주요변화)"""
    print(f"\n{'='*60}")
    print(f"채널별 매출 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
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
        
        # SQL 쿼리 실행 (4-1-1-1용: 당해/전년 동월 비교)
        sql = get_channel_sales_cypy_query(yyyymm, yyyymm_py, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('SALE_AMT', 0)) for r in records)
        unique_channels = len(set(r.get('CHNL_NM', '') for r in records))
        unique_items = len(set(r.get('CLASS3', '') for r in records))
        unique_months = len(set(r.get('PST_YYYYMM', '') for r in records))
        
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)")
        print(f"채널 수: {unique_channels}개")
        print(f"아이템 수: {unique_items}개")
        print(f"분석 월 수: {unique_months}개월")
        
        # 채널별 요약 데이터 생성
        channel_summary = {}
        for record in records:
            chnl_nm = record.get('CHNL_NM', '기타')
            month = record.get('PST_YYYYMM', '')
            sale_amt = float(record.get('SALE_AMT', 0))
            
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
            chnl_nm = record.get('CHNL_NM', '기타')
            class3 = record.get('CLASS3', '기타')
            sale_amt = float(record.get('SALE_AMT', 0))
            
            key = f"{chnl_nm}|{class3}"
            if key not in item_sales_by_channel:
                item_sales_by_channel[key] = {
                    'chnl_nm': chnl_nm,
                    'class3': class3,
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
                    'class3': item['class3'],
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
            month = record.get('PST_YYYYMM', '')
            sale_amt = float(record.get('SALE_AMT', 0))
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
            chnl_nm = record.get('CHNL_NM', '기타')
            month = record.get('PST_YYYYMM', '')
            
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
            current_data = [r for r in records if r.get('CHNL_NM') == chnl_nm and r.get('PST_YYYYMM') == yyyymm]
            previous_data = [r for r in records if r.get('CHNL_NM') == chnl_nm and r.get('PST_YYYYMM') == yyyymm_py]
            
            # 채널별 TOP 3 아이템 (당해 기준)
            current_items = sorted(current_data, key=lambda x: float(x.get('SALE_AMT', 0)), reverse=True)[:3]
            
            channel_comparison[chnl_nm] = {
                'current_top3': [
                    {
                        'class3': item.get('CLASS3', ''),
                        'sale_amt': round(float(item.get('SALE_AMT', 0)) / 1000000, 2),
                        'sale_ratio': float(item.get('SALE_RATIO', 0))
                    }
                    for item in current_items
                ],
                'current_total': round(sum(float(r.get('SALE_AMT', 0)) for r in current_data) / 1000000, 2),
                'previous_total': round(sum(float(r.get('SALE_AMT', 0)) for r in previous_data) / 1000000, 2)
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
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

각 채널별로 하나의 섹션을 만들어야 합니다. 채널 목록: {', '.join(valid_channels)}

{{
  "title": "채널별 매출 top3 분석 (당해 전년 주요변화)",
  "sections": [
    {{
      "div": "{{채널명}}",
      "sub_title": "{{채널명}} 전년대비 주요 변화",
      "ai_text": "각 {{채널명}} 당해 당월 매출 베스트 아이템 3개를 한 줄씩 전년대비 주요변화로 분석해줘. 채널별 데이터 요약의 current_top3와 current_total, previous_total을 참고하여 구체적인 변화율과 원인을 분석해줘. (예: • 운동모: 당해 신규 D핏 언스트럭쳐 볼캡 제품 +156.3% 폭증\\n • 숄더백: 클래식 모노그램 뉴 엠보 성수/한남점 폭발적 반응 +145.2%\\n • 햇 : 고딕버킷햇 제품 폭발적 성장 +120.1% 등)"
    }}
  ]
}}

<작성 가이드라인>
- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 백만원 단위로 표시하고 절대 변형하지 말 것
- 당해 채널별 TOP 3 매출 아이템과 그중 어떤 제품이 판매율이 좋았는지
- 전년대비 주요 변화 분석
- 단기 전략 방향과 중장기 전략 방향을 구체적으로 시사
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시 (예: "첫 번째 줄\\n두 번째 줄")
- ai_text 내에서 여러 문단이나 항목을 나눌 때는 \\n\\n을 사용
- 불릿 포인트나 리스트 항목 사이에는 \\n을 사용
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱 (마크다운 코드 블록 제거)
        analysis_response = analysis_response.strip()
        if analysis_response.startswith('```json'):
            analysis_response = analysis_response[7:]
        if analysis_response.startswith('```'):
            analysis_response = analysis_response[3:]
        if analysis_response.endswith('```'):
            analysis_response = analysis_response[:-3]
        analysis_response = analysis_response.strip()
        
        try:
            analysis_data = json.loads(analysis_response)
        except json.JSONDecodeError as e:
            print(f"[WARNING] JSON 파싱 실패: {e}")
            print(f"[WARNING] 응답 내용: {analysis_response[:500]}")
            # 기본 구조로 대체
            analysis_data = {
                "title": "채널별 매출 분석 (12개월 추이)",
                "sections": [
                    {"sub_title": "분석 결과", "ai_text": analysis_response}
                ]
            }
        
        # JSON 데이터 생성
        json_data = {
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
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
                        'PST_YYYYMM': r.get('PST_YYYYMM', ''),
                        'CHNL_NM': r.get('CHNL_NM', ''),
                        'CLASS3': r.get('CLASS3', ''),
                        'SALE_AMT': float(r.get('SALE_AMT', 0)),
                        'SALE_AMT_CHNL_TTL': float(r.get('SALE_AMT_CHNL_TTL', 0)),
                        'SALE_RATIO': float(r.get('SALE_RATIO', 0)),
                        'IN_YMM_RNK': int(r.get('IN_YMM_RNK', 0)),
                        'IN_CHNL_RNK': int(r.get('IN_CHNL_RNK', 0))
                    }
                    for r in records[:50]
                ],
                'total_records_count': len(records)
            },
            'trend_data': {
                'trend_months': sorted(list(set(r.get('PST_YYYYMM', '') for r in records))),
                'monthly_totals': monthly_totals_list,
                'monthly_details': [
                    {
                        'yyyymm': r.get('PST_YYYYMM', ''),
                        'chnl_nm': r.get('CHNL_NM', ''),
                        'class3': r.get('CLASS3', ''),
                        'sale_amt': round(float(r.get('SALE_AMT', 0)) / 1000000, 2),
                        'sale_ratio': float(r.get('SALE_RATIO', 0))
                    }
                    for r in records
                ]
            }
        }
        
        # 파일 저장
        filename = f"4-1-1-1.{brd_cd}_채널별_매출_top3_분석(당해_전년_주요변화)"
        save_json(json_data, filename)
        
        # Markdown도 저장 (analysis_data의 sections를 조합)
        markdown_content = f"# {analysis_data.get('title', '채널별 매출 분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_channel_sales_overall(yyyymm, brd_cd): 
    """채널별 매출 종합분석 (당해_전년 주요변화) - 4-1-1-2"""
    print(f"\n{'='*60}")
    print(f"채널별 매출 종합분석 시작 (4-1-1-2): {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
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
        sql = get_channel_sales_cypy_query(yyyymm, yyyymm_py, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        current_data = [r for r in records if r.get('PST_YYYYMM') == yyyymm]
        previous_data = [r for r in records if r.get('PST_YYYYMM') == yyyymm_py]
        
        total_sales_cy = sum(float(r.get('SALE_AMT', 0)) for r in current_data)
        total_sales_py = sum(float(r.get('SALE_AMT', 0)) for r in previous_data)
        total_sales = total_sales_cy + total_sales_py
        
        unique_channels = len(set(r.get('CHNL_NM', '') for r in records))
        unique_items = len(set(r.get('CLASS3', '') for r in records))
        
        print(f"전년 매출액: {total_sales_py:,.0f}원 ({total_sales_py/1000000:.2f}백만원)")
        print(f"당해 매출액: {total_sales_cy:,.0f}원 ({total_sales_cy/1000000:.2f}백만원)")
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)")
        print(f"채널 수: {unique_channels}개")
        print(f"아이템 수: {unique_items}개")
        
        # 채널별 요약 데이터 생성 (당해/전년 비교)
        channel_summary = {}
        for record in records:
            chnl_nm = record.get('CHNL_NM', '기타')
            month = record.get('PST_YYYYMM', '')
            sale_amt = float(record.get('SALE_AMT', 0))
            
            if chnl_nm not in channel_summary:
                channel_summary[chnl_nm] = {
                    'current_sales': 0,
                    'previous_sales': 0,
                    'all_items': []
                }
            
            if month == yyyymm:
                channel_summary[chnl_nm]['current_sales'] += sale_amt
            elif month == yyyymm_py:
                channel_summary[chnl_nm]['previous_sales'] += sale_amt
        
        # 채널별 상위 아이템 추출 (당해 기준)
        item_sales_by_channel = {}
        for record in current_data:
            chnl_nm = record.get('CHNL_NM', '기타')
            class3 = record.get('CLASS3', '기타')
            sale_amt = float(record.get('SALE_AMT', 0))
            
            key = f"{chnl_nm}|{class3}"
            if key not in item_sales_by_channel:
                item_sales_by_channel[key] = {
                    'chnl_nm': chnl_nm,
                    'class3': class3,
                    'total_sales': 0
                }
            item_sales_by_channel[key]['total_sales'] += sale_amt
        
        # 채널별로 전체 아이템 추출 (top3 제한 없음)
        for chnl_nm in channel_summary.keys():
            channel_items = [
                item for key, item in item_sales_by_channel.items()
                if item['chnl_nm'] == chnl_nm
            ]
            channel_items.sort(key=lambda x: x['total_sales'], reverse=True)
            # 모든 아이템 포함 (제한 없음)
            channel_summary[chnl_nm]['all_items'] = [
                {
                    'class3': item['class3'],
                    'total_sales': round(item['total_sales'] / 1000000, 2)
                }
                for item in channel_items
            ]
            channel_summary[chnl_nm]['current_sales'] = round(
                channel_summary[chnl_nm]['current_sales'] / 1000000, 2
            )
            channel_summary[chnl_nm]['previous_sales'] = round(
                channel_summary[chnl_nm]['previous_sales'] / 1000000, 2
            )
            if channel_summary[chnl_nm]['previous_sales'] > 0:
                channel_summary[chnl_nm]['change_pct'] = round(
                    ((channel_summary[chnl_nm]['current_sales'] - channel_summary[chnl_nm]['previous_sales']) / channel_summary[chnl_nm]['previous_sales'] * 100), 1
                )
            else:
                channel_summary[chnl_nm]['change_pct'] = 0
        
        # 채널별로 당해/전년 데이터 존재 여부 확인
        channel_data_check = {}
        for record in records:
            chnl_nm = record.get('CHNL_NM', '기타')
            month = record.get('PST_YYYYMM', '')
            
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
            channel for channel, check in channel_data_check.items()
            if check['has_current'] and check['has_previous']
        ]
        
        # LLM 프롬프트 생성 (JSON 형식 응답 요청)
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 채널 전략 전문가야. 브랜드 전체 채널을 종합적으로 분석하여 최고 성과 채널, 개선 필요 채널, 핵심 제안을 도출해줘.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 전년 매출액: {total_sales_py:,.0f}원 ({total_sales_py/1000000:.2f}백만원)
- 당해 매출액: {total_sales_cy:,.0f}원 ({total_sales_cy/1000000:.2f}백만원)
- 전년대비 변화: {round(((total_sales_cy - total_sales_py) / total_sales_py * 100) if total_sales_py != 0 else 0, 1)}%
- 분석 채널 수: {len(valid_channels)}개
- 분석 채널 목록: {', '.join(valid_channels)}
- 분석 아이템 수: {unique_items}개

**채널별 전체 데이터**
{json_dumps_safe({k: v for k, v in channel_summary.items() if k in valid_channels}, ensure_ascii=False, indent=2)}

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 모든 채널을 종합적으로 분석하여:
1. 최고 성과 채널: 매출 규모, 성장률, 전년대비 개선도 등을 종합하여 최고 성과를 보인 채널들을 식별
2. 개선 필요 채널: 매출 하락, 성장 둔화, 전년대비 악화 등이 있는 채널들을 식별하고 개선 방향 제시
3. 핵심 제안: 브랜드 전체 채널 포트폴리오 관점에서 즉시 실행 가능한 전략적 제안

<데이터 샘플>
{json_dumps_safe(records[:200], ensure_ascii=False, indent=2)}

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

{{
  "title": "브랜드별 채널 매출 종합분석",
  "sections": [
    {{
      "div": "overall-1",
      "sub_title": "최고 성과 채널",
      "ai_text": "최고 성과를 보인 채널들을 종합 분석 (최대 2줄)"
    }},
    {{
      "div": "overall-2",
      "sub_title": "개선 필요 채널",
      "ai_text": "개선이 필요한 채널들을 종합 분석 (최대 2줄)"
    }},
    {{
      "div": "overall-3",
      "sub_title": "핵심 제안",
      "ai_text": "브랜드 전체 채널 전략에 대한 핵심 제안 (최대 2줄)"
    }}
  ]
}}

<작성 가이드라인>
- 각 섹션의 ai_text는 최대 2줄을 넘지 않도록 간결하게 작성
- 숫자는 백만원 단위로 표시하고 절대 변형하지 말 것
- 모든 채널의 데이터를 종합적으로 분석 (특정 채널만이 아닌 전체 관점)
- 채널별 top3가 아니라 전체 채널을 종합적으로 분석
- 구체적인 채널명과 수치를 포함하여 실용적인 내용으로 작성
- 줄바꿈은 반드시 \\n을 사용하여 표시
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱 (마크다운 코드 블록 제거)
        analysis_response = analysis_response.strip()
        if analysis_response.startswith('```json'):
            analysis_response = analysis_response[7:]
        if analysis_response.startswith('```'):
            analysis_response = analysis_response[3:]
        if analysis_response.endswith('```'):
            analysis_response = analysis_response[:-3]
        analysis_response = analysis_response.strip()
        
        try:
            analysis_data = json.loads(analysis_response)
        except json.JSONDecodeError as e:
            print(f"[WARNING] JSON 파싱 실패: {e}")
            print(f"[WARNING] 응답 내용: {analysis_response[:500]}")
            # 기본 구조로 대체
            analysis_data = {
                "title": "브랜드별 채널 매출 종합분석",
                "sections": [
                    {"div": "overall-1", "sub_title": "최고 성과 채널", "ai_text": analysis_response},
                    {"div": "overall-2", "sub_title": "개선 필요 채널", "ai_text": ""},
                    {"div": "overall-3", "sub_title": "핵심 제안", "ai_text": ""}
                ]
            }
        
        # JSON 데이터 생성
        json_data = {
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'analysis_data': analysis_data,
            'summary': {
                'total_sales_cy': round(total_sales_cy / 1000000, 2),
                'total_sales_py': round(total_sales_py / 1000000, 2),
                'change_pct': round(((total_sales_cy - total_sales_py) / total_sales_py * 100) if total_sales_py != 0 else 0, 1),
                'unique_channels': unique_channels,
                'unique_items': unique_items,
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'channel_summary': channel_summary,
            'raw_data': {
                'sample_records': [
                    {
                        'PST_YYYYMM': r.get('PST_YYYYMM', ''),
                        'CHNL_NM': r.get('CHNL_NM', ''),
                        'CLASS3': r.get('CLASS3', ''),
                        'SALE_AMT': float(r.get('SALE_AMT', 0)),
                        'SALE_AMT_CHNL_TTL': float(r.get('SALE_AMT_CHNL_TTL', 0)),
                        'SALE_RATIO': float(r.get('SALE_RATIO', 0)),
                        'IN_YMM_RNK': int(r.get('IN_YMM_RNK', 0)),
                        'IN_CHNL_RNK': int(r.get('IN_CHNL_RNK', 0))
                    }
                    for r in records[:50]
                ],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장 (4-1-1-2로 저장)
        filename = f"4-1-1-2.{brd_cd}_브랜드별_채널_매출_종합분석(월)"
        save_json(json_data, filename)
        
        # Markdown도 저장 (analysis_data의 sections를 조합)
        markdown_content = f"# {analysis_data.get('title', '채널별 매출 분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_gender_purchase_pattern(yyyymm, brd_cd):
    """성별 구매 패턴 분석 (당해/전년 동월 비교) - 4-1-3-1"""
    print(f"\n{'='*60}")
    print(f"성별 구매 패턴 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
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
        sql = get_gender_purchase_pattern_query(yyyymm, yyyymm_py, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('ACT_SALE_AMT', 0)) for r in records)
        total_qty = sum(float(r.get('SALE_QTY', 0)) for r in records)
        unique_genders = len(set(r.get('SEX_NM', '') for r in records))
        unique_categories = len(set(r.get('PRDT_HRRC1_NM', '') for r in records))
        unique_items = len(set(r.get('PRDT_HRRC3_NM', '') for r in records))
        unique_months = len(set(r.get('YYYY_MM', '') for r in records))
        
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)")
        print(f"총 판매수량: {total_qty:,.0f}개")
        print(f"성별 수: {unique_genders}개")
        print(f"카테고리 수: {unique_categories}개")
        print(f"아이템 수: {unique_items}개")
        print(f"분석 월 수: {unique_months}개월")
        
        # 성별별 요약 데이터 생성
        gender_summary = {}
        for record in records:
            sex_nm = record.get('SEX_NM', '기타')
            month = record.get('YYYY_MM', '')
            sale_amt = float(record.get('ACT_SALE_AMT', 0))
            sale_qty = float(record.get('SALE_QTY', 0))
            
            if sex_nm not in gender_summary:
                gender_summary[sex_nm] = {
                    'total_sales': 0,
                    'total_qty': 0,
                    'months': {},
                    'top_items': []
                }
            
            gender_summary[sex_nm]['total_sales'] += sale_amt
            gender_summary[sex_nm]['total_qty'] += sale_qty
            
            if month not in gender_summary[sex_nm]['months']:
                gender_summary[sex_nm]['months'][month] = {'sales': 0, 'qty': 0}
            gender_summary[sex_nm]['months'][month]['sales'] += sale_amt
            gender_summary[sex_nm]['months'][month]['qty'] += sale_qty
        
        # 성별별 상위 아이템 추출
        item_sales_by_gender = {}
        for record in records:
            sex_nm = record.get('SEX_NM', '기타')
            class3 = record.get('PRDT_HRRC3_NM', '기타')
            sale_amt = float(record.get('ACT_SALE_AMT', 0))
            
            key = f"{sex_nm}|{class3}"
            if key not in item_sales_by_gender:
                item_sales_by_gender[key] = {
                    'sex_nm': sex_nm,
                    'class3': class3,
                    'total_sales': 0
                }
            item_sales_by_gender[key]['total_sales'] += sale_amt
        
        # 성별별로 상위 5개 아이템 추출
        for sex_nm in gender_summary.keys():
            gender_items = [
                item for key, item in item_sales_by_gender.items()
                if item['sex_nm'] == sex_nm
            ]
            gender_items.sort(key=lambda x: x['total_sales'], reverse=True)
            gender_summary[sex_nm]['top_items'] = [
                {
                    'class3': item['class3'],
                    'total_sales': round(item['total_sales'] / 1000000, 2)
                }
                for item in gender_items[:5]
            ]
            gender_summary[sex_nm]['total_sales'] = round(
                gender_summary[sex_nm]['total_sales'] / 1000000, 2
            )
            gender_summary[sex_nm]['total_qty'] = round(
                gender_summary[sex_nm]['total_qty'], 0
            )
        
        # 월별 합계 계산
        monthly_totals = {}
        for record in records:
            month = record.get('YYYY_MM', '')
            sale_amt = float(record.get('ACT_SALE_AMT', 0))
            if month not in monthly_totals:
                monthly_totals[month] = 0
            monthly_totals[month] += sale_amt
        
        monthly_totals_list = [
            {'yyyymm': month, 'total_amount': round(amount / 1000000, 2)}
            for month, amount in sorted(monthly_totals.items())
        ]
        
        # 성별별로 당해/전년 데이터 존재 여부 확인
        gender_data_check = {}
        for record in records:
            sex_nm = record.get('SEX_NM', '기타')
            month = record.get('YYYY_MM', '').replace('-', '')
            
            if sex_nm not in gender_data_check:
                gender_data_check[sex_nm] = {
                    'has_current': False,
                    'has_previous': False
                }
            
            if month == yyyymm:
                gender_data_check[sex_nm]['has_current'] = True
            elif month == yyyymm_py:
                gender_data_check[sex_nm]['has_previous'] = True
        
        # 당해/전년 데이터가 모두 있는 성별만 필터링
        valid_genders = [
            gender for gender, check in gender_data_check.items()
            if check['has_current'] and check['has_previous']
        ]
        
        # 성별별 데이터 요약 (당해/전년 비교용)
        gender_comparison = {}
        for sex_nm in valid_genders:
            current_data = [r for r in records if r.get('SEX_NM') == sex_nm and r.get('YYYY_MM', '').replace('-', '') == yyyymm]
            previous_data = [r for r in records if r.get('SEX_NM') == sex_nm and r.get('YYYY_MM', '').replace('-', '') == yyyymm_py]
            
            # 성별별 TOP 3 아이템 (당해 기준)
            current_items = sorted(current_data, key=lambda x: float(x.get('ACT_SALE_AMT', 0)), reverse=True)[:3]
            
            gender_comparison[sex_nm] = {
                'current_top3': [
                    {
                        'prdt_hrrc1_nm': item.get('PRDT_HRRC1_NM', ''),
                        'prdt_hrrc2_nm': item.get('PRDT_HRRC2_NM', ''),
                        'prdt_hrrc3_nm': item.get('PRDT_HRRC3_NM', ''),
                        'sale_amt': round(float(item.get('ACT_SALE_AMT', 0)) / 1000000, 2),
                        'sale_qty': float(item.get('SALE_QTY', 0))
                    }
                    for item in current_items
                ],
                'current_total': round(sum(float(r.get('ACT_SALE_AMT', 0)) for r in current_data) / 1000000, 2),
                'previous_total': round(sum(float(r.get('ACT_SALE_AMT', 0)) for r in previous_data) / 1000000, 2)
            }
        
        # 성별별 섹션 템플릿 생성
        gender_sections_template = ',\n    '.join([
            '{{\n      "div": "{gender}",\n      "sub_title": "{gender} 제품별 성별 구매 패턴 분석",\n      "ai_text": "각 {gender} 당해 당월 성별 제품 구매 패턴 분석을 해줘. 전년과 달라진 점도 분석해줘. (예: • 남성 고객은 아우터에 대한 구매 비중이 45.2%로 가장 높으며, 전년(43.1%) 대비 +2.1%p 상승하여 아우터 선호도가 강화되는 추세입니다. ACC 카테고리에서는 모자(36.5%)가 가장 인기 있으며 전년(32.8%) 대비 +3.7%p 증가했습니다. 계절성 아우터 상품 라인업 강화와 모자 신상품 출시를 통한 매출 확대 기회가 있습니다.)"\n    }}'.format(gender=gender)
            for gender in valid_genders
        ])
        
        # LLM 프롬프트 생성 (JSON 형식 응답 요청)
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 채널 전략 전문가야. 각 제품별 당해 당월 성별 제품 구매 패턴 분석을 해줘.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)
- 총 판매수량: {total_qty:,.0f}개
- 분석 가능한 성별 수: {len(valid_genders)}개
- 분석 성별 목록: {', '.join(valid_genders)}
- 분석 아이템 수: {unique_items}개

**성별별 데이터 요약**
{json_dumps_safe(gender_comparison, ensure_ascii=False, indent=2)}

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 당해 당월 성별 제품 구매 패턴 분석을 해줘. 전년과 달라진 점도 분석해줘.

**중요**: 위 "성별별 데이터 요약"에 있는 성별만 분석하면 됩니다. 데이터가 없는 성별은 분석하지 마세요.

<데이터 샘플>
{json_dumps_safe(records[:200], ensure_ascii=False, indent=2)}

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

각 성별별로 하나의 섹션을 만들어야 합니다. 성별 목록: {', '.join(valid_genders)}

{{
  "title": "제품별 성별 구매 패턴 분석 (당해 전년 주요변화)",
  "sections": [
    {gender_sections_template}
  ]
}}

<작성 가이드라인>
- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 백만원 단위로 표시하고 절대 변형하지 말 것
- 당해 제품별 성별 구매 패턴 분석
- 전년대비 주요 변화 분석
- 단기 전략 방향과 중장기 전략 방향을 구체적으로 시사
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시 (예: "첫 번째 줄\\n두 번째 줄")
- ai_text 내에서 여러 문단이나 항목을 나눌 때는 \\n\\n을 사용
- 불릿 포인트나 리스트 항목 사이에는 \\n을 사용
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱 (마크다운 코드 블록 제거)
        analysis_response = analysis_response.strip()
        if analysis_response.startswith('```json'):
            analysis_response = analysis_response[7:]
        if analysis_response.startswith('```'):
            analysis_response = analysis_response[3:]
        if analysis_response.endswith('```'):
            analysis_response = analysis_response[:-3]
        analysis_response = analysis_response.strip()
        
        try:
            analysis_data = json.loads(analysis_response)
        except json.JSONDecodeError as e:
            print(f"[WARNING] JSON 파싱 실패: {e}")
            print(f"[WARNING] 응답 내용: {analysis_response[:500]}")
            # 기본 구조로 대체
            analysis_data = {
                "title": "성별 구매 패턴 분석 (당해 전년 주요변화)",
                "sections": [
                    {"div": "기타", "sub_title": "분석 결과", "ai_text": analysis_response}
                ]
            }
        
        # JSON 데이터 생성
        json_data = {
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'analysis_data': analysis_data,
            'summary': {
                'total_sales': round(total_sales / 1000000, 2),
                'total_qty': round(total_qty, 0),
                'unique_genders': unique_genders,
                'unique_categories': unique_categories,
                'unique_items': unique_items,
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'gender_summary': gender_summary,
            'raw_data': {
                'sample_records': [
                    {
                        'YYYY_MM': r.get('YYYY_MM', ''),
                        'SEX_NM': r.get('SEX_NM', ''),
                        'PRDT_HRRC1_NM': r.get('PRDT_HRRC1_NM', ''),
                        'PRDT_HRRC2_NM': r.get('PRDT_HRRC2_NM', ''),
                        'PRDT_HRRC3_NM': r.get('PRDT_HRRC3_NM', ''),
                        'SALE_QTY': float(r.get('SALE_QTY', 0)),
                        'ACT_SALE_AMT': float(r.get('ACT_SALE_AMT', 0))
                    }
                    for r in records[:50]
                ],
                'total_records_count': len(records)
            },
            'trend_data': {
                'trend_months': sorted(list(set(r.get('YYYY_MM', '') for r in records))),
                'monthly_totals': monthly_totals_list,
                'monthly_details': [
                    {
                        'yyyymm': r.get('YYYY_MM', ''),
                        'sex_nm': r.get('SEX_NM', ''),
                        'prdt_hrrc1_nm': r.get('PRDT_HRRC1_NM', ''),
                        'prdt_hrrc2_nm': r.get('PRDT_HRRC2_NM', ''),
                        'prdt_hrrc3_nm': r.get('PRDT_HRRC3_NM', ''),
                        'sale_qty': float(r.get('SALE_QTY', 0)),
                        'sale_amt': round(float(r.get('ACT_SALE_AMT', 0)) / 1000000, 2)
                    }
                    for r in records
                ]
            }
        }
        
        # 파일 저장
        filename = f"4-1-3-1.{brd_cd}_성별_구매패턴_분석(당해_전년_주요변화)"
        save_json(json_data, filename)
        
        # Markdown도 저장 (analysis_data의 sections를 조합)
        markdown_content = f"# {analysis_data.get('title', '성별 구매 패턴 분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_gender_purchase_pattern_overall(yyyymm, brd_cd):
    """성별 구매 패턴 종합분석 (12개월 추이) - 4-1-3-2"""
    print(f"\n{'='*60}")
    print(f"성별 구매 패턴 종합분석 시작 (4-1-3-2): {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (12개월)
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        
        start_year = current_year
        start_month = current_month - 11
        
        while start_month <= 0:
            start_month += 12
            start_year -= 1
        
        yyyymm_start = f"{start_year:04d}{start_month:02d}"
        yyyymm_end = yyyymm
        
        print(f"분석 기간: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월")
        
        # SQL 쿼리 실행
        sql = get_gender_purchase_pattern_overall_query(yyyymm_start, yyyymm_end, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('ACT_SALE_AMT', 0)) for r in records)
        total_qty = sum(float(r.get('SALE_QTY', 0)) for r in records)
        unique_genders = len(set(r.get('SEX_NM', '') for r in records))
        unique_categories = len(set(r.get('PRDT_HRRC1_NM', '') for r in records))
        unique_items = len(set(r.get('PRDT_HRRC3_NM', '') for r in records))
        unique_months = len(set(r.get('YYYY_MM', '') for r in records))
        
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)")
        print(f"총 판매수량: {total_qty:,.0f}개")
        print(f"성별 수: {unique_genders}개")
        print(f"카테고리 수: {unique_categories}개")
        print(f"아이템 수: {unique_items}개")
        print(f"분석 월 수: {unique_months}개월")
        
        # 성별별 요약 데이터 생성
        gender_summary = {}
        for record in records:
            sex_nm = record.get('SEX_NM', '기타')
            month = record.get('YYYY_MM', '')
            sale_amt = float(record.get('ACT_SALE_AMT', 0))
            sale_qty = float(record.get('SALE_QTY', 0))
            
            if sex_nm not in gender_summary:
                gender_summary[sex_nm] = {
                    'total_sales': 0,
                    'total_qty': 0,
                    'months': {},
                    'top_items': []
                }
            
            gender_summary[sex_nm]['total_sales'] += sale_amt
            gender_summary[sex_nm]['total_qty'] += sale_qty
            
            if month not in gender_summary[sex_nm]['months']:
                gender_summary[sex_nm]['months'][month] = {'sales': 0, 'qty': 0}
            gender_summary[sex_nm]['months'][month]['sales'] += sale_amt
            gender_summary[sex_nm]['months'][month]['qty'] += sale_qty
        
        # 성별별 상위 아이템 추출
        item_sales_by_gender = {}
        for record in records:
            sex_nm = record.get('SEX_NM', '기타')
            class3 = record.get('PRDT_HRRC3_NM', '기타')
            sale_amt = float(record.get('ACT_SALE_AMT', 0))
            
            key = f"{sex_nm}|{class3}"
            if key not in item_sales_by_gender:
                item_sales_by_gender[key] = {
                    'sex_nm': sex_nm,
                    'class3': class3,
                    'total_sales': 0
                }
            item_sales_by_gender[key]['total_sales'] += sale_amt
        
        # 성별별로 상위 5개 아이템 추출
        for sex_nm in gender_summary.keys():
            gender_items = [
                item for key, item in item_sales_by_gender.items()
                if item['sex_nm'] == sex_nm
            ]
            gender_items.sort(key=lambda x: x['total_sales'], reverse=True)
            gender_summary[sex_nm]['top_items'] = [
                {
                    'class3': item['class3'],
                    'total_sales': round(item['total_sales'] / 1000000, 2)
                }
                for item in gender_items[:5]
            ]
            gender_summary[sex_nm]['total_sales'] = round(
                gender_summary[sex_nm]['total_sales'] / 1000000, 2
            )
            gender_summary[sex_nm]['total_qty'] = round(
                gender_summary[sex_nm]['total_qty'], 0
            )
        
        # 월별 합계 계산
        monthly_totals = {}
        for record in records:
            month = record.get('YYYY_MM', '')
            sale_amt = float(record.get('ACT_SALE_AMT', 0))
            if month not in monthly_totals:
                monthly_totals[month] = 0
            monthly_totals[month] += sale_amt
        
        monthly_totals_list = [
            {'yyyymm': month, 'total_amount': round(amount / 1000000, 2)}
            for month, amount in sorted(monthly_totals.items())
        ]
        
        # 섹션 정의 (변수 처리)
        section_definitions = [
            {
                'sub_title': '성별 구매 패턴 종합 평가',
                'ai_text': '12개월간의 성별 구매 패턴을 종합적으로 평가한 내용 (예: 남성 고객이 전체 매출의 55%를 차지하며 핵심 타겟으로 부상, 여성 고객은 아우터 카테고리에서 지속적 성장세 유지 등)'
            },
            {
                'sub_title': '성장 성별 및 기회',
                'ai_text': '성장세가 뚜렷한 성별과 기회를 불릿 포인트로 나열 (예: • 남성 고객: 12개월간 지속적 성장으로 전체 매출의 55% 기여, 아우터 카테고리에서 강세 등)'
            },
            {
                'sub_title': '주의 필요 성별',
                'ai_text': '주의가 필요한 성별들을 불릿 포인트로 나열 (예: • 여성 고객: 최근 3개월간 특정 카테고리 매출 감소 추세 등)'
            },
            {
                'sub_title': '이상징후 및 리스크 감지',
                'ai_text': '이상징후와 리스크를 구체적으로 설명 (예: • 특정 성별의 아이템 집중도 과다: 남성 고객의 상위 3개 아이템이 전체의 60% 차지 등)'
            },
            {
                'sub_title': '성별별 전략 최적화 방안',
                'ai_text': '단기 전략 방향과 중장기 전략 방향을 구체적으로 제시 (예: ### 즉시 실행 방안\\n1. 남성 고객 타겟 아이템 포트폴리오 다변화: ... 등)'
            }
        ]
        
        # 섹션 템플릿 동적 생성
        sections_template = ',\n    '.join([
            '{{\n      "div": "overall-{idx}",\n      "sub_title": "{sub_title}",\n      "ai_text": "{ai_text}"\n    }}'.format(
                idx=i+1,
                sub_title=section['sub_title'],
                ai_text=section['ai_text']
            )
            for i, section in enumerate(section_definitions)
        ])
        
        # LLM 프롬프트 생성 (JSON 형식 응답 요청)
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 고객 전략 전문가야. 12개월간의 성별 구매 패턴 추이를 분석하여 성별별 성과와 제품 포트폴리오 전략을 제시해야 해.

**분석 기간**
- 시작: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월
- 종료: {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월
- 기간: {unique_months}개월

**전체 요약**
- 총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)
- 총 판매수량: {total_qty:,.0f}개
- 분석 성별 수: {unique_genders}개
- 분석 카테고리 수: {unique_categories}개
- 분석 아이템 수: {unique_items}개

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 12개월간 성별 구매 패턴 추이를 분석하여:
1. 성별별 성과와 성장 패턴 파악
2. 성별별 핵심 제품(카테고리/아이템) 식별
3. 성별별 매출 기여도와 비중 분석
4. 성별별 전략적 인사이트 제시

<데이터 샘플>
{json_dumps_safe(records[:100], ensure_ascii=False, indent=2)}

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

{{
  "title": "성별 구매 패턴 분석 (12개월 추이)",
  "sections": [
    {sections_template}
  ]
}}

<작성 가이드라인>
- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 백만원 단위로 표시하고 절대 변형하지 말 것
- 성별별 구매 패턴과 성장 추세 분석
- 성별별 핵심 제품 카테고리와 아이템 식별
- 전년대비 변화에 대한 구체적 원인과 효과 분석
- 단기 전략 방향과 중장기 전략 방향을 구체적으로 제시
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시 (예: "첫 번째 줄\\n두 번째 줄")
- ai_text 내에서 여러 문단이나 항목을 나눌 때는 \\n\\n을 사용
- 불릿 포인트나 리스트 항목 사이에는 \\n을 사용
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱 (마크다운 코드 블록 제거)
        analysis_response = analysis_response.strip()
        if analysis_response.startswith('```json'):
            analysis_response = analysis_response[7:]
        if analysis_response.startswith('```'):
            analysis_response = analysis_response[3:]
        if analysis_response.endswith('```'):
            analysis_response = analysis_response[:-3]
        analysis_response = analysis_response.strip()
        
        try:
            analysis_data = json.loads(analysis_response)
        except json.JSONDecodeError as e:
            print(f"[WARNING] JSON 파싱 실패: {e}")
            print(f"[WARNING] 응답 내용: {analysis_response[:500]}")
            # 기본 구조로 대체
            analysis_data = {
                "title": "성별 구매 패턴 분석 (12개월 추이)",
                "sections": [
                    {"div": "overall-1", "sub_title": "분석 결과", "ai_text": analysis_response}
                ]
            }
        
        # JSON 데이터 생성
        # yyyymm_py 계산 (전년 동월)
        previous_year = int(yyyymm_end[:4]) - 1
        yyyymm_py = f"{previous_year}{yyyymm_end[4:6]}"
        
        json_data = {
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm_end,
            'yyyymm_py': yyyymm_py,
            'analysis_data': analysis_data,
            'summary': {
                'total_sales': round(total_sales / 1000000, 2),
                'total_qty': round(total_qty, 0),
                'unique_genders': unique_genders,
                'unique_categories': unique_categories,
                'unique_items': unique_items,
                'unique_months': unique_months,
                'analysis_period': f"{yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월"
            },
            'gender_summary': gender_summary,
            'raw_data': {
                'sample_records': [
                    {
                        'YYYY_MM': r.get('YYYY_MM', ''),
                        'SEX_NM': r.get('SEX_NM', ''),
                        'PRDT_HRRC1_NM': r.get('PRDT_HRRC1_NM', ''),
                        'PRDT_HRRC2_NM': r.get('PRDT_HRRC2_NM', ''),
                        'PRDT_HRRC3_NM': r.get('PRDT_HRRC3_NM', ''),
                        'SALE_QTY': float(r.get('SALE_QTY', 0)),
                        'ACT_SALE_AMT': float(r.get('ACT_SALE_AMT', 0))
                    }
                    for r in records[:50]
                ],
                'total_records_count': len(records)
            },
            'trend_data': {
                'trend_months': sorted(list(set(r.get('YYYY_MM', '') for r in records))),
                'monthly_totals': monthly_totals_list,
                'monthly_details': [
                    {
                        'yyyymm': r.get('YYYY_MM', ''),
                        'sex_nm': r.get('SEX_NM', ''),
                        'prdt_hrrc1_nm': r.get('PRDT_HRRC1_NM', ''),
                        'prdt_hrrc2_nm': r.get('PRDT_HRRC2_NM', ''),
                        'prdt_hrrc3_nm': r.get('PRDT_HRRC3_NM', ''),
                        'sale_qty': float(r.get('SALE_QTY', 0)),
                        'sale_amt': round(float(r.get('ACT_SALE_AMT', 0)) / 1000000, 2)
                    }
                    for r in records
                ]
            }
        }
        
        # 파일 저장 (4-1-3-2로 저장)
        filename = f"4-1-3-2.{brd_cd}_성별_구매패턴_종합분석(12개월추이)"
        save_json(json_data, filename)
        
        # Markdown도 저장 (analysis_data의 sections를 조합)
        markdown_content = f"# {analysis_data.get('title', '성별 구매 패턴 분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_category_profit(yyyymm, brd_cd):
    """카테고리별 수익성 분석 (당해/전년 동월 비교) - 5-2-1-1"""
    print(f"\n{'='*60}")
    print(f"카테고리별 수익성 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
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
        sql = get_category_profit_analysis_query(yyyymm, yyyymm_py, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('ACT_SALE_AMT', 0)) for r in records)
        total_qty = sum(float(r.get('SALE_QTY', 0)) for r in records)
        total_profit = sum(float(r.get('SALE_TTL_PRFT', 0)) for r in records)
        unique_categories = len(set(r.get('PRDT_HRRC1_NM', '') for r in records))
        unique_subcategories = len(set(r.get('PRDT_HRRC2_NM', '') for r in records))
        unique_items = len(set(r.get('PRDT_HRRC3_NM', '') for r in records))
        unique_products = len(set(r.get('PRDT_NM', '') for r in records))
        unique_months = len(set(r.get('YYYY_MM', '') for r in records))
        
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)")
        print(f"총 판매수량: {total_qty:,.0f}개")
        print(f"총 이익: {total_profit:,.0f}원 ({total_profit/1000000:.2f}백만원)")
        print(f"카테고리 수: {unique_categories}개")
        print(f"서브카테고리 수: {unique_subcategories}개")
        print(f"아이템 수: {unique_items}개")
        print(f"제품 수: {unique_products}개")
        print(f"분석 월 수: {unique_months}개월")
        
        # 카테고리별 요약 데이터 생성
        category_summary = {}
        for record in records:
            category1 = record.get('PRDT_HRRC1_NM', '기타')
            month = record.get('YYYY_MM', '')
            sale_amt = float(record.get('ACT_SALE_AMT', 0))
            sale_qty = float(record.get('SALE_QTY', 0))
            profit = float(record.get('SALE_TTL_PRFT', 0))
            
            if category1 not in category_summary:
                category_summary[category1] = {
                    'total_sales': 0,
                    'total_qty': 0,
                    'total_profit': 0,
                    'months': {},
                    'top_items': []
                }
            
            category_summary[category1]['total_sales'] += sale_amt
            category_summary[category1]['total_qty'] += sale_qty
            category_summary[category1]['total_profit'] += profit
            
            if month not in category_summary[category1]['months']:
                category_summary[category1]['months'][month] = {'sales': 0, 'qty': 0, 'profit': 0}
            category_summary[category1]['months'][month]['sales'] += sale_amt
            category_summary[category1]['months'][month]['qty'] += sale_qty
            category_summary[category1]['months'][month]['profit'] += profit
        
        # 카테고리별 상위 아이템 추출
        item_sales_by_category = {}
        for record in records:
            category1 = record.get('PRDT_HRRC1_NM', '기타')
            class3 = record.get('PRDT_HRRC3_NM', '기타')
            sale_amt = float(record.get('ACT_SALE_AMT', 0))
            profit = float(record.get('SALE_TTL_PRFT', 0))
            
            key = f"{category1}|{class3}"
            if key not in item_sales_by_category:
                item_sales_by_category[key] = {
                    'category1': category1,
                    'class3': class3,
                    'total_sales': 0,
                    'total_profit': 0
                }
            item_sales_by_category[key]['total_sales'] += sale_amt
            item_sales_by_category[key]['total_profit'] += profit
        
        # 카테고리별로 상위 5개 아이템 추출
        for category1 in category_summary.keys():
            category_items = [
                item for key, item in item_sales_by_category.items()
                if item['category1'] == category1
            ]
            category_items.sort(key=lambda x: x['total_sales'], reverse=True)
            category_summary[category1]['top_items'] = [
                {
                    'class3': item['class3'],
                    'total_sales': round(item['total_sales'] / 1000000, 2),
                    'total_profit': round(item['total_profit'] / 1000000, 2),
                    'profit_rate': round((item['total_profit'] / item['total_sales'] * 100) if item['total_sales'] != 0 else 0, 1)
                }
                for item in category_items[:5]
            ]
            category_summary[category1]['total_sales'] = round(
                category_summary[category1]['total_sales'] / 1000000, 2
            )
            category_summary[category1]['total_qty'] = round(
                category_summary[category1]['total_qty'], 0
            )
            category_summary[category1]['total_profit'] = round(
                category_summary[category1]['total_profit'] / 1000000, 2
            )
            if category_summary[category1]['total_sales'] > 0:
                category_summary[category1]['profit_rate'] = round(
                    (category_summary[category1]['total_profit'] / category_summary[category1]['total_sales'] * 100), 1
                )
            else:
                category_summary[category1]['profit_rate'] = 0
        
        # 월별 합계 계산
        monthly_totals = {}
        for record in records:
            month = record.get('YYYY_MM', '')
            sale_amt = float(record.get('ACT_SALE_AMT', 0))
            if month not in monthly_totals:
                monthly_totals[month] = 0
            monthly_totals[month] += sale_amt
        
        monthly_totals_list = [
            {'yyyymm': month, 'total_amount': round(amount / 1000000, 2)}
            for month, amount in sorted(monthly_totals.items())
        ]
        
        # 카테고리별로 당해/전년 데이터 존재 여부 확인
        category_data_check = {}
        for record in records:
            category1 = record.get('PRDT_HRRC1_NM', '기타')
            month = record.get('YYYY_MM', '').replace('-', '')
            
            if category1 not in category_data_check:
                category_data_check[category1] = {
                    'has_current': False,
                    'has_previous': False
                }
            
            if month == yyyymm:
                category_data_check[category1]['has_current'] = True
            elif month == yyyymm_py:
                category_data_check[category1]['has_previous'] = True
        
        # 당해/전년 데이터가 모두 있는 카테고리만 필터링
        valid_categories = [
            category for category, check in category_data_check.items()
            if check['has_current'] and check['has_previous']
        ]
        
        # 카테고리별 데이터 요약 (당해/전년 비교용)
        category_comparison = {}
        for category1 in valid_categories:
            current_data = [r for r in records if r.get('PRDT_HRRC1_NM') == category1 and r.get('YYYY_MM', '').replace('-', '') == yyyymm]
            previous_data = [r for r in records if r.get('PRDT_HRRC1_NM') == category1 and r.get('YYYY_MM', '').replace('-', '') == yyyymm_py]
            
            # 카테고리별 TOP 3 아이템 (당해 기준)
            current_items = sorted(current_data, key=lambda x: float(x.get('ACT_SALE_AMT', 0)), reverse=True)[:3]
            
            category_comparison[category1] = {
                'current_top3': [
                    {
                        'prdt_nm': item.get('PRDT_NM', ''),
                        'prdt_hrrc2_nm': item.get('PRDT_HRRC2_NM', ''),
                        'prdt_hrrc3_nm': item.get('PRDT_HRRC3_NM', ''),
                        'sale_amt': round(float(item.get('ACT_SALE_AMT', 0)) / 1000000, 2),
                        'sale_qty': float(item.get('SALE_QTY', 0)),
                        'profit': round(float(item.get('SALE_TTL_PRFT', 0)) / 1000000, 2),
                        'profit_rate': round((float(item.get('SALE_TTL_PRFT', 0)) / float(item.get('ACT_SALE_AMT', 0)) * 100) if float(item.get('ACT_SALE_AMT', 0)) != 0 else 0, 1)
                    }
                    for item in current_items
                ],
                'current_total': round(sum(float(r.get('ACT_SALE_AMT', 0)) for r in current_data) / 1000000, 2),
                'current_profit': round(sum(float(r.get('SALE_TTL_PRFT', 0)) for r in current_data) / 1000000, 2),
                'previous_total': round(sum(float(r.get('ACT_SALE_AMT', 0)) for r in previous_data) / 1000000, 2),
                'previous_profit': round(sum(float(r.get('SALE_TTL_PRFT', 0)) for r in previous_data) / 1000000, 2)
            }
            if category_comparison[category1]['current_total'] > 0:
                category_comparison[category1]['current_profit_rate'] = round(
                    (category_comparison[category1]['current_profit'] / category_comparison[category1]['current_total'] * 100), 1
                )
            else:
                category_comparison[category1]['current_profit_rate'] = 0
        
        # 카테고리별 섹션 템플릿 생성
        category_sections_template = ',\n    '.join([
            '{{\n      "div": "{category}",\n      "sub_title": "{category} 전년대비 주요 변화",\n      "ai_text": "각 {category} 당해 당월 매출과 수익성을 전년대비 주요변화로 분석해줘. 카테고리별 데이터 요약의 current_top3와 current_total, previous_total, current_profit, previous_profit을 참고하여 구체적인 변화율과 원인을 분석해줘. (예: • ACC: 당해 신규 운동모 제품 +156.3% 폭증, 수익률 45.2%로 전년(42.1%) 대비 +3.1%p 상승\\n • 의류: 다운점퍼 제품 폭발적 성장 +120.1%, 수익률 38.5%로 전년(35.8%) 대비 +2.7%p 증가 등)"\n    }}'.format(category=category)
            for category in valid_categories
        ])
        
        # LLM 프롬프트 생성 (JSON 형식 응답 요청)
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 제품 전략 전문가야. 각 카테고리별(악세서리, 의류 등) 당해 당월 매출과 수익성을 전년대비 주요변화로 분석해줘.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)
- 총 판매수량: {total_qty:,.0f}개
- 총 이익: {total_profit:,.0f}원 ({total_profit/1000000:.2f}백만원)
- 전체 수익률: {round((total_profit / total_sales * 100) if total_sales != 0 else 0, 1)}%
- 분석 가능한 카테고리 수: {len(valid_categories)}개
- 분석 카테고리 목록: {', '.join(valid_categories)}
- 분석 아이템 수: {unique_items}개

**카테고리별 데이터 요약**
{json_dumps_safe(category_comparison, ensure_ascii=False, indent=2)}

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 각 카테고리별(악세서리, 의류 등) 당해 당월 매출과 수익성을 전년대비 주요변화로 분석해줘.

**중요**: 위 "카테고리별 데이터 요약"에 있는 카테고리만 분석하면 됩니다. 데이터가 없는 카테고리는 분석하지 마세요.

<데이터 샘플>
{json_dumps_safe(records[:200], ensure_ascii=False, indent=2)}

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

각 카테고리별로 하나의 섹션을 만들어야 합니다. 카테고리 목록: {', '.join(valid_categories)}

{{
  "title": "카테고리별 수익성 분석 (당해 전년 주요변화)",
  "sections": [
    {category_sections_template}
  ]
}}

<작성 가이드라인>
- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 백만원 단위로 표시하고 절대 변형하지 말 것
- 당해 카테고리별 TOP 3 매출 아이템과 수익성 분석
- 전년대비 주요 변화 분석 (매출, 수익, 수익률)
- 단기 전략 방향과 중장기 전략 방향을 구체적으로 시사
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시 (예: "첫 번째 줄\\n두 번째 줄")
- ai_text 내에서 여러 문단이나 항목을 나눌 때는 \\n\\n을 사용
- 불릿 포인트나 리스트 항목 사이에는 \\n을 사용
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱 (마크다운 코드 블록 제거)
        analysis_response = analysis_response.strip()
        if analysis_response.startswith('```json'):
            analysis_response = analysis_response[7:]
        if analysis_response.startswith('```'):
            analysis_response = analysis_response[3:]
        if analysis_response.endswith('```'):
            analysis_response = analysis_response[:-3]
        analysis_response = analysis_response.strip()
        
        try:
            analysis_data = json.loads(analysis_response)
        except json.JSONDecodeError as e:
            print(f"[WARNING] JSON 파싱 실패: {e}")
            print(f"[WARNING] 응답 내용: {analysis_response[:500]}")
            # 기본 구조로 대체
            analysis_data = {
                "title": "카테고리별 수익성 분석 (당해 전년 주요변화)",
                "sections": [
                    {"div": "기타", "sub_title": "분석 결과", "ai_text": analysis_response}
                ]
            }
        
        # JSON 데이터 생성
        json_data = {
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'analysis_data': analysis_data,
            'summary': {
                'total_sales': round(total_sales / 1000000, 2),
                'total_qty': round(total_qty, 0),
                'total_profit': round(total_profit / 1000000, 2),
                'total_profit_rate': round((total_profit / total_sales * 100) if total_sales != 0 else 0, 1),
                'unique_categories': unique_categories,
                'unique_subcategories': unique_subcategories,
                'unique_items': unique_items,
                'unique_products': unique_products,
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'category_summary': category_summary,
            'raw_data': {
                'sample_records': [
                    {
                        'YYYY_MM': r.get('YYYY_MM', ''),
                        'PRDT_NM': r.get('PRDT_NM', ''),
                        'PRDT_HRRC1_NM': r.get('PRDT_HRRC1_NM', ''),
                        'PRDT_HRRC2_NM': r.get('PRDT_HRRC2_NM', ''),
                        'PRDT_HRRC3_NM': r.get('PRDT_HRRC3_NM', ''),
                        'SALE_QTY': float(r.get('SALE_QTY', 0)),
                        'ACT_SALE_AMT': float(r.get('ACT_SALE_AMT', 0)),
                        'SALE_TTL_PRFT': float(r.get('SALE_TTL_PRFT', 0))
                    }
                    for r in records[:50]
                ],
                'total_records_count': len(records)
            },
            'trend_data': {
                'trend_months': sorted(list(set(r.get('YYYY_MM', '') for r in records))),
                'monthly_totals': monthly_totals_list,
                'monthly_details': [
                    {
                        'yyyymm': r.get('YYYY_MM', ''),
                        'prdt_hrrc1_nm': r.get('PRDT_HRRC1_NM', ''),
                        'prdt_hrrc2_nm': r.get('PRDT_HRRC2_NM', ''),
                        'prdt_hrrc3_nm': r.get('PRDT_HRRC3_NM', ''),
                        'prdt_nm': r.get('PRDT_NM', ''),
                        'sale_qty': float(r.get('SALE_QTY', 0)),
                        'sale_amt': round(float(r.get('ACT_SALE_AMT', 0)) / 1000000, 2),
                        'profit': round(float(r.get('SALE_TTL_PRFT', 0)) / 1000000, 2)
                    }
                    for r in records
                ]
            }
        }
        
        # 파일 저장
        filename = f"5-2-1-1.{brd_cd}_카테고리별_수익성_분석(당해_전년_주요변화)"
        save_json(json_data, filename)
        
        # Markdown도 저장 (analysis_data의 sections를 조합)
        markdown_content = f"# {analysis_data.get('title', '카테고리별 수익성 분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_category_profit_overall(yyyymm, brd_cd):
    """카테고리별 수익성 종합분석 (12개월 추이) - 5-2-1-2"""
    print(f"\n{'='*60}")
    print(f"카테고리별 수익성 종합분석 시작 (5-2-1-2): {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (12개월)
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        
        start_year = current_year
        start_month = current_month - 11
        
        while start_month <= 0:
            start_month += 12
            start_year -= 1
        
        yyyymm_start = f"{start_year:04d}{start_month:02d}"
        yyyymm_end = yyyymm
        
        print(f"분석 기간: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월")
        
        # SQL 쿼리 실행
        sql = get_category_profit_overall_query(yyyymm_start, yyyymm_end, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('ACT_SALE_AMT', 0)) for r in records)
        total_qty = sum(float(r.get('SALE_QTY', 0)) for r in records)
        total_profit = sum(float(r.get('SALE_TTL_PRFT', 0)) for r in records)
        unique_categories = len(set(r.get('PRDT_HRRC1_NM', '') for r in records))
        unique_subcategories = len(set(r.get('PRDT_HRRC2_NM', '') for r in records))
        unique_items = len(set(r.get('PRDT_HRRC3_NM', '') for r in records))
        unique_products = len(set(r.get('PRDT_NM', '') for r in records))
        unique_months = len(set(r.get('YYYY_MM', '') for r in records))
        
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)")
        print(f"총 판매수량: {total_qty:,.0f}개")
        print(f"총 이익: {total_profit:,.0f}원 ({total_profit/1000000:.2f}백만원)")
        print(f"전체 수익률: {round((total_profit / total_sales * 100) if total_sales != 0 else 0, 1)}%")
        print(f"카테고리 수: {unique_categories}개")
        print(f"서브카테고리 수: {unique_subcategories}개")
        print(f"아이템 수: {unique_items}개")
        print(f"제품 수: {unique_products}개")
        print(f"분석 월 수: {unique_months}개월")
        
        # 카테고리별 요약 데이터 생성
        category_summary = {}
        for record in records:
            category1 = record.get('PRDT_HRRC1_NM', '기타')
            month = record.get('YYYY_MM', '')
            sale_amt = float(record.get('ACT_SALE_AMT', 0))
            sale_qty = float(record.get('SALE_QTY', 0))
            profit = float(record.get('SALE_TTL_PRFT', 0))
            
            if category1 not in category_summary:
                category_summary[category1] = {
                    'total_sales': 0,
                    'total_qty': 0,
                    'total_profit': 0,
                    'months': {},
                    'top_items': []
                }
            
            category_summary[category1]['total_sales'] += sale_amt
            category_summary[category1]['total_qty'] += sale_qty
            category_summary[category1]['total_profit'] += profit
            
            if month not in category_summary[category1]['months']:
                category_summary[category1]['months'][month] = {'sales': 0, 'qty': 0, 'profit': 0}
            category_summary[category1]['months'][month]['sales'] += sale_amt
            category_summary[category1]['months'][month]['qty'] += sale_qty
            category_summary[category1]['months'][month]['profit'] += profit
        
        # 카테고리별 상위 아이템 추출
        item_sales_by_category = {}
        for record in records:
            category1 = record.get('PRDT_HRRC1_NM', '기타')
            class3 = record.get('PRDT_HRRC3_NM', '기타')
            sale_amt = float(record.get('ACT_SALE_AMT', 0))
            profit = float(record.get('SALE_TTL_PRFT', 0))
            
            key = f"{category1}|{class3}"
            if key not in item_sales_by_category:
                item_sales_by_category[key] = {
                    'category1': category1,
                    'class3': class3,
                    'total_sales': 0,
                    'total_profit': 0
                }
            item_sales_by_category[key]['total_sales'] += sale_amt
            item_sales_by_category[key]['total_profit'] += profit
        
        # 카테고리별로 상위 5개 아이템 추출
        for category1 in category_summary.keys():
            category_items = [
                item for key, item in item_sales_by_category.items()
                if item['category1'] == category1
            ]
            category_items.sort(key=lambda x: x['total_sales'], reverse=True)
            category_summary[category1]['top_items'] = [
                {
                    'class3': item['class3'],
                    'total_sales': round(item['total_sales'] / 1000000, 2),
                    'total_profit': round(item['total_profit'] / 1000000, 2),
                    'profit_rate': round((item['total_profit'] / item['total_sales'] * 100) if item['total_sales'] != 0 else 0, 1)
                }
                for item in category_items[:5]
            ]
            category_summary[category1]['total_sales'] = round(
                category_summary[category1]['total_sales'] / 1000000, 2
            )
            category_summary[category1]['total_qty'] = round(
                category_summary[category1]['total_qty'], 0
            )
            category_summary[category1]['total_profit'] = round(
                category_summary[category1]['total_profit'] / 1000000, 2
            )
            if category_summary[category1]['total_sales'] > 0:
                category_summary[category1]['profit_rate'] = round(
                    (category_summary[category1]['total_profit'] / category_summary[category1]['total_sales'] * 100), 1
                )
            else:
                category_summary[category1]['profit_rate'] = 0
        
        # 월별 합계 계산
        monthly_totals = {}
        for record in records:
            month = record.get('YYYY_MM', '')
            sale_amt = float(record.get('ACT_SALE_AMT', 0))
            if month not in monthly_totals:
                monthly_totals[month] = 0
            monthly_totals[month] += sale_amt
        
        monthly_totals_list = [
            {'yyyymm': month, 'total_amount': round(amount / 1000000, 2)}
            for month, amount in sorted(monthly_totals.items())
        ]
        
        # 섹션 정의 (변수 처리)
        section_definitions = [
            {
                'sub_title': '카테고리별 수익성 종합 평가',
                'ai_text': '12개월간의 카테고리별 매출과 수익성을 종합적으로 평가한 내용 (예: ACC 카테고리가 전체 매출의 45%를 차지하며 핵심 카테고리로 부상, 수익률 42.5%로 전반적으로 높은 수익성을 보이고 있습니다. 의류 카테고리는 안정적 성장세를 유지하며 수익률 38.2%를 기록했습니다 등)'
            },
            {
                'sub_title': '성장 카테고리 및 기회',
                'ai_text': '성장세가 뚜렷한 카테고리와 기회를 불릿 포인트로 나열 (예: • ACC: 12개월간 지속적 성장으로 전체 매출의 45% 기여, 수익률 42.5%로 높은 수익성 유지 등)'
            },
            {
                'sub_title': '주의 필요 카테고리',
                'ai_text': '주의가 필요한 카테고리들을 불릿 포인트로 나열 (예: • 특정 카테고리: 최근 3개월간 수익률 하락 추세 등)'
            },
            {
                'sub_title': '이상징후 및 리스크 감지',
                'ai_text': '이상징후와 리스크를 구체적으로 설명 (예: • 특정 카테고리의 아이템 집중도 과다: ACC의 상위 3개 아이템이 전체의 60% 차지, 수익률 변동성 증가 등)'
            },
            {
                'sub_title': '카테고리별 전략 최적화 방안',
                'ai_text': '단기 전략 방향과 중장기 전략 방향을 구체적으로 제시 (예: ### 즉시 실행 방안\\n1. ACC 카테고리 아이템 포트폴리오 다변화: ... 등)'
            }
        ]
        
        # 섹션 템플릿 동적 생성
        sections_template = ',\n    '.join([
            '{{\n      "div": "overall-{idx}",\n      "sub_title": "{sub_title}",\n      "ai_text": "{ai_text}"\n    }}'.format(
                idx=i+1,
                sub_title=section['sub_title'],
                ai_text=section['ai_text']
            )
            for i, section in enumerate(section_definitions)
        ])
        
        # LLM 프롬프트 생성 (JSON 형식 응답 요청)
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 제품 전략 전문가야. 12개월간의 카테고리별 매출과 수익성 추이를 분석하여 카테고리별 성과와 제품 포트폴리오 전략을 제시해야 해.

**분석 기간**
- 시작: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월
- 종료: {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월
- 기간: {unique_months}개월

**전체 요약**
- 총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)
- 총 판매수량: {total_qty:,.0f}개
- 총 이익: {total_profit:,.0f}원 ({total_profit/1000000:.2f}백만원)
- 전체 수익률: {round((total_profit / total_sales * 100) if total_sales != 0 else 0, 1)}%
- 분석 카테고리 수: {unique_categories}개
- 분석 서브카테고리 수: {unique_subcategories}개
- 분석 아이템 수: {unique_items}개
- 분석 제품 수: {unique_products}개

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 12개월간 카테고리별 매출과 수익성 추이를 분석하여:
1. 카테고리별 성과와 성장 패턴 파악
2. 카테고리별 핵심 제품(카테고리/아이템) 식별
3. 카테고리별 매출 기여도와 수익성 분석
4. 카테고리별 전략적 인사이트 제시

<데이터 샘플>
{json_dumps_safe(records[:100], ensure_ascii=False, indent=2)}

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

{{
  "title": "카테고리별 수익성 분석 (12개월 추이)",
  "sections": [
    {sections_template}
  ]
}}

<작성 가이드라인>
- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 백만원 단위로 표시하고 절대 변형하지 말 것
- 카테고리별 구매 패턴과 성장 추세 분석
- 카테고리별 핵심 제품 카테고리와 아이템 식별
- 카테고리별 수익성(수익률) 분석
- 전년대비 변화에 대한 구체적 원인과 효과 분석
- 단기 전략 방향과 중장기 전략 방향을 구체적으로 제시
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시 (예: "첫 번째 줄\\n두 번째 줄")
- ai_text 내에서 여러 문단이나 항목을 나눌 때는 \\n\\n을 사용
- 불릿 포인트나 리스트 항목 사이에는 \\n을 사용
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱 (마크다운 코드 블록 제거)
        analysis_response = analysis_response.strip()
        if analysis_response.startswith('```json'):
            analysis_response = analysis_response[7:]
        if analysis_response.startswith('```'):
            analysis_response = analysis_response[3:]
        if analysis_response.endswith('```'):
            analysis_response = analysis_response[:-3]
        analysis_response = analysis_response.strip()
        
        try:
            analysis_data = json.loads(analysis_response)
        except json.JSONDecodeError as e:
            print(f"[WARNING] JSON 파싱 실패: {e}")
            print(f"[WARNING] 응답 내용: {analysis_response[:500]}")
            # 기본 구조로 대체
            analysis_data = {
                "title": "카테고리별 수익성 분석 (12개월 추이)",
                "sections": [
                    {"div": "overall-1", "sub_title": "분석 결과", "ai_text": analysis_response}
                ]
            }
        
        # JSON 데이터 생성
        # yyyymm_py 계산 (전년 동월)
        previous_year = int(yyyymm_end[:4]) - 1
        yyyymm_py = f"{previous_year}{yyyymm_end[4:6]}"
        
        json_data = {
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm_end,
            'yyyymm_py': yyyymm_py,
            'analysis_data': analysis_data,
            'summary': {
                'total_sales': round(total_sales / 1000000, 2),
                'total_qty': round(total_qty, 0),
                'total_profit': round(total_profit / 1000000, 2),
                'total_profit_rate': round((total_profit / total_sales * 100) if total_sales != 0 else 0, 1),
                'unique_categories': unique_categories,
                'unique_subcategories': unique_subcategories,
                'unique_items': unique_items,
                'unique_products': unique_products,
                'unique_months': unique_months,
                'analysis_period': f"{yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월"
            },
            'category_summary': category_summary,
            'raw_data': {
                'sample_records': [
                    {
                        'YYYY_MM': r.get('YYYY_MM', ''),
                        'PRDT_NM': r.get('PRDT_NM', ''),
                        'PRDT_HRRC1_NM': r.get('PRDT_HRRC1_NM', ''),
                        'PRDT_HRRC2_NM': r.get('PRDT_HRRC2_NM', ''),
                        'PRDT_HRRC3_NM': r.get('PRDT_HRRC3_NM', ''),
                        'SALE_QTY': float(r.get('SALE_QTY', 0)),
                        'ACT_SALE_AMT': float(r.get('ACT_SALE_AMT', 0)),
                        'SALE_TTL_PRFT': float(r.get('SALE_TTL_PRFT', 0))
                    }
                    for r in records[:50]
                ],
                'total_records_count': len(records)
            },
            'trend_data': {
                'trend_months': sorted(list(set(r.get('YYYY_MM', '') for r in records))),
                'monthly_totals': monthly_totals_list,
                'monthly_details': [
                    {
                        'yyyymm': r.get('YYYY_MM', ''),
                        'prdt_hrrc1_nm': r.get('PRDT_HRRC1_NM', ''),
                        'prdt_hrrc2_nm': r.get('PRDT_HRRC2_NM', ''),
                        'prdt_hrrc3_nm': r.get('PRDT_HRRC3_NM', ''),
                        'prdt_nm': r.get('PRDT_NM', ''),
                        'sale_qty': float(r.get('SALE_QTY', 0)),
                        'sale_amt': round(float(r.get('ACT_SALE_AMT', 0)) / 1000000, 2),
                        'profit': round(float(r.get('SALE_TTL_PRFT', 0)) / 1000000, 2)
                    }
                    for r in records
                ]
            }
        }
        
        # 파일 저장 (5-2-1-2로 저장)
        filename = f"5-2-1-2.{brd_cd}_카테고리별_수익성_종합분석(12개월추이)"
        save_json(json_data, filename)
        
        # Markdown도 저장 (analysis_data의 sections를 조합)
        markdown_content = f"# {analysis_data.get('title', '카테고리별 수익성 분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_channel_sales_overall_12m(yyyymm, brd_cd):
    """채널별 매출 종합분석 (당해 1월~현재월) - 14-1-1-1"""
    print(f"\n{'='*60}")
    print(f"채널별 매출 종합분석 시작 (14-1-1-1): {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (당해 1월부터 현재월까지)
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        
        yyyymm_start = f"{current_year}01"  # 당해 1월
        yyyymm_end = yyyymm  # 현재월
        
        print(f"분석 기간: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월")
        
        # SQL 쿼리 실행
        sql = get_channel_sales_query(yyyymm_start, yyyymm_end, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('SALE_AMT', 0)) for r in records)
        unique_channels = len(set(r.get('CHNL_NM', '') for r in records))
        unique_items = len(set(r.get('CLASS3', '') for r in records))
        unique_months = len(set(r.get('PST_YYYYMM', '') for r in records))
        
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)")
        print(f"채널 수: {unique_channels}개")
        print(f"아이템 수: {unique_items}개")
        print(f"분석 월 수: {unique_months}개월")
        
        # 채널별 요약 데이터 생성
        channel_summary = {}
        for record in records:
            chnl_nm = record.get('CHNL_NM', '기타')
            month = record.get('PST_YYYYMM', '')
            sale_amt = float(record.get('SALE_AMT', 0))
            
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
            chnl_nm = record.get('CHNL_NM', '기타')
            class3 = record.get('CLASS3', '기타')
            sale_amt = float(record.get('SALE_AMT', 0))
            
            key = f"{chnl_nm}|{class3}"
            if key not in item_sales_by_channel:
                item_sales_by_channel[key] = {
                    'chnl_nm': chnl_nm,
                    'class3': class3,
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
                    'class3': item['class3'],
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
            month = record.get('PST_YYYYMM', '')
            sale_amt = float(record.get('SALE_AMT', 0))
            if month not in monthly_totals:
                monthly_totals[month] = 0
            monthly_totals[month] += sale_amt
        
        monthly_totals_list = [
            {'yyyymm': month, 'total_amount': round(amount / 1000000, 2)}
            for month, amount in sorted(monthly_totals.items())
        ]
        
        # 월별 매출 분석 (최대/최소/턴어라운드)
        if monthly_totals_list:
            # 최대 매출월 찾기
            max_month_data = max(monthly_totals_list, key=lambda x: x['total_amount'])
            max_month = max_month_data['yyyymm']
            max_amount = max_month_data['total_amount']
            
            # 최소 매출월 찾기
            min_month_data = min(monthly_totals_list, key=lambda x: x['total_amount'])
            min_month = min_month_data['yyyymm']
            min_amount = min_month_data['total_amount']
            
            # 턴어라운드 시점 찾기 (하락 후 상승으로 전환되는 시점)
            turnaround_month = None
            turnaround_amount = None
            if len(monthly_totals_list) >= 3:
                for i in range(1, len(monthly_totals_list) - 1):
                    prev_amount = monthly_totals_list[i-1]['total_amount']
                    curr_amount = monthly_totals_list[i]['total_amount']
                    next_amount = monthly_totals_list[i+1]['total_amount']
                    
                    # 이전 월보다 감소했고, 다음 월보다 증가한 경우 (턴어라운드)
                    if prev_amount > curr_amount and next_amount > curr_amount:
                        turnaround_month = monthly_totals_list[i]['yyyymm']
                        turnaround_amount = curr_amount
                        break
                
                # 턴어라운드가 없으면 마지막으로 상승한 시점 찾기
                if turnaround_month is None:
                    for i in range(len(monthly_totals_list) - 1, 0, -1):
                        prev_amount = monthly_totals_list[i-1]['total_amount']
                        curr_amount = monthly_totals_list[i]['total_amount']
                        if curr_amount > prev_amount:
                            turnaround_month = monthly_totals_list[i]['yyyymm']
                            turnaround_amount = curr_amount
                            break
            
            # 월 표시 형식 변환 (YYYYMM -> M월)
            def format_month(yyyymm):
                if len(yyyymm) == 6:
                    return f"{int(yyyymm[4:6])}월"
                return yyyymm
            
            max_month_str = format_month(max_month)
            min_month_str = format_month(min_month)
            turnaround_month_str = format_month(turnaround_month) if turnaround_month else "없음"
            
            # 주요 인사이트 텍스트 생성
            insight_text = f"• {max_month_str} 최대 {max_amount:,.0f}\n• {min_month_str} 최저 {min_amount:,.0f}\n"
            if turnaround_month:
                insight_text += f"• {turnaround_month_str} 회복 {turnaround_amount:,.0f}"
            else:
                insight_text += f"• 턴어라운드 시점 없음"
        else:
            insight_text = "• 데이터 부족"
        
        # 채널별 트렌드 분석 (특정 이벤트가 있는 채널 찾기)
        channel_trends = []
        
        for chnl_nm, chnl_data in channel_summary.items():
            months_data = chnl_data.get('months', {})
            if len(months_data) < 3:
                continue
            
            # 월별 매출을 정렬된 리스트로 변환 (월 순서대로, YYYYMM 형식으로 정렬)
            sorted_months = sorted(months_data.items())
            month_values = [amount / 1000000 for month, amount in sorted_months]  # 백만원 단위로 변환
            
            # 트렌드 분석
            trend_type = None
            trend_month = None
            trend_description = None
            
            # 1. 회복 패턴 찾기 (하락 후 상승)
            # 가장 최근의 회복 시점을 찾기 위해 뒤에서부터 검색
            # 회복 = 연속으로 하락하다가 상승으로 전환되는 시점
            for i in range(len(month_values) - 2, 0, -1):
                # 기본 회복 패턴: 이전 월보다 감소했고, 다음 월보다 증가한 경우
                if month_values[i-1] > month_values[i] and month_values[i+1] > month_values[i]:
                    # 회복이 시작된 월 찾기 (상승이 시작된 시점 = i+1)
                    recovery_month = sorted_months[i+1][0]
                    month_num = int(recovery_month[4:6]) if len(recovery_month) == 6 else recovery_month
                    trend_type = "회복"
                    trend_description = f"{month_num}월 회복"
                    break
                # 연속 하락 후 상승 패턴 (2개월 이상 하락 후 상승)
                elif i >= 2:
                    if (month_values[i-2] > month_values[i-1] and 
                        month_values[i-1] > month_values[i] and 
                        month_values[i+1] > month_values[i]):
                        recovery_month = sorted_months[i+1][0]
                        month_num = int(recovery_month[4:6]) if len(recovery_month) == 6 else recovery_month
                        trend_type = "회복"
                        trend_description = f"{month_num}월 회복"
                        break
            
            # 2. 지속 성장 패턴 (전반적으로 상승 추세)
            if trend_type is None:
                growth_count = 0
                decline_count = 0
                for i in range(1, len(month_values)):
                    if month_values[i] > month_values[i-1]:
                        growth_count += 1
                    elif month_values[i] < month_values[i-1]:
                        decline_count += 1
                
                if growth_count > decline_count * 1.5:  # 성장이 하락보다 1.5배 이상
                    trend_type = "지속 성장"
                    trend_description = "지속 성장"
            
            # 3. 계절성 영향 (특정 월에 급증/급감)
            if trend_type is None:
                max_month_idx = month_values.index(max(month_values))
                min_month_idx = month_values.index(min(month_values))
                max_month = sorted_months[max_month_idx][0]
                min_month = sorted_months[min_month_idx][0]
                
                if abs(max_month_idx - min_month_idx) >= 2:  # 최대/최소가 충분히 떨어져 있음
                    max_month_num = int(max_month[4:6]) if len(max_month) == 6 else max_month
                    min_month_num = int(min_month[4:6]) if len(min_month) == 6 else min_month
                    if max_month_num in [3, 4, 5, 9, 10, 11, 12] or min_month_num in [1, 2, 6, 7, 8]:
                        trend_type = "계절성"
                        trend_description = "계절성 영향"
            
            # 4. 하락 추세
            if trend_type is None:
                if decline_count > growth_count * 1.5:
                    trend_type = "하락"
                    trend_description = "하락 추세"
            
            if trend_type:
                channel_trends.append({
                    'channel': chnl_nm,
                    'trend_type': trend_type,
                    'trend_description': trend_description,
                    'trend_month': trend_month,
                    'total_sales': chnl_data['total_sales']
                })
        
        # 총 매출 기준으로 상위 3개 채널 선택 (특정 이벤트가 있는 것 중에서)
        channel_trends.sort(key=lambda x: x['total_sales'], reverse=True)
        top_3_trends = channel_trends[:3] if len(channel_trends) >= 3 else channel_trends
        
        # 채널 트렌드 텍스트 생성
        if top_3_trends:
            trend_text = '\n'.join([
                f"• {item['channel']}: {item['trend_description']}"
                for item in top_3_trends
            ])
        else:
            trend_text = "• 분석 가능한 채널 트렌드 없음"
        
        # 전략 제안을 위한 데이터 분석
        # 1. 채널별 매출 기여도 분석
        channel_contributions = []
        for chnl_nm, chnl_data in channel_summary.items():
            contribution_pct = round((chnl_data['total_sales'] / (total_sales / 1000000) * 100) if total_sales > 0 else 0, 1)
            channel_contributions.append({
                'channel': chnl_nm,
                'sales': chnl_data['total_sales'],
                'contribution': contribution_pct,
                'top_items': chnl_data.get('top_items', [])[:3]
            })
        channel_contributions.sort(key=lambda x: x['sales'], reverse=True)
        
        # 2. 성장 채널과 하락 채널 식별
        growing_channels = []
        declining_channels = []
        for chnl_nm, chnl_data in channel_summary.items():
            months_data = chnl_data.get('months', {})
            if len(months_data) >= 2:
                sorted_months = sorted(months_data.items())
                first_half = sum([amount for month, amount in sorted_months[:len(sorted_months)//2]])
                second_half = sum([amount for month, amount in sorted_months[len(sorted_months)//2:]])
                
                if second_half > first_half * 1.1:  # 10% 이상 성장
                    growing_channels.append(chnl_nm)
                elif second_half < first_half * 0.9:  # 10% 이상 하락
                    declining_channels.append(chnl_nm)
        
        # 3. 아이템 집중도 분석 (상위 3개 아이템이 전체의 비중)
        item_concentration = {}
        for chnl_nm, chnl_data in channel_summary.items():
            top_items = chnl_data.get('top_items', [])
            if top_items:
                top3_sales = sum([item['total_sales'] for item in top_items[:3]])
                total_chnl_sales = chnl_data['total_sales']
                concentration = round((top3_sales / total_chnl_sales * 100) if total_chnl_sales > 0 else 0, 1)
                item_concentration[chnl_nm] = concentration
        
        # 전략 제안 데이터 정리
        strategy_data = {
            'top_channels': channel_contributions[:3],
            'growing_channels': growing_channels[:3],
            'declining_channels': declining_channels[:3],
            'high_concentration_channels': [
                {'channel': chnl, 'concentration': conc}
                for chnl, conc in sorted(item_concentration.items(), key=lambda x: x[1], reverse=True)
                if conc > 50
            ][:3]
        }
        
        # 전략 포인트 텍스트 생성 (데이터 요약)
        strategy_summary = f"""
**주요 채널 기여도 (상위 3개)**
{json_dumps_safe([{'channel': c['channel'], 'sales': c['sales'], 'contribution': c['contribution']} for c in strategy_data['top_channels']], ensure_ascii=False, indent=2)}

**성장 채널**: {', '.join(strategy_data['growing_channels']) if strategy_data['growing_channels'] else '없음'}
**하락 채널**: {', '.join(strategy_data['declining_channels']) if strategy_data['declining_channels'] else '없음'}
**아이템 집중도 높은 채널**: {', '.join([c['channel'] for c in strategy_data['high_concentration_channels']]) if strategy_data['high_concentration_channels'] else '없음'}
"""
        
        # 섹션 정의 (변수 처리)
        section_definitions = [
            {
                'sub_title': '주요 인사이트',
                'ai_text': '당해 1월~현재월까지의 매출 분석 결과를 구체적으로 3줄로 작성해줘. 각 줄은 다음 형식을 정확히 따르세요:\n• [최대 매출월] 최대 [금액]백만원 - [구체적 원인 또는 특징 설명]\n• [최소 매출월] 최저 [금액]백만원 - [구체적 원인 또는 특징 설명]\n• [턴어라운드 월] 회복 [금액]백만원 - [구체적 회복 요인 설명]\n위 "월별 매출 분석 결과"의 데이터를 바탕으로 각 월의 구체적인 특징과 원인을 포함하여 작성하세요.'
            },
            {
                'sub_title': '채널 트렌드',
                'ai_text': f'특정 이벤트가 있는 채널 3개를 구체적으로 3줄로 작성해줘. 각 줄은 다음 형식을 정확히 따르세요:\n• [채널명]: [구체적인 트렌드 설명] - [매출 변화율 또는 금액 변화, 주요 아이템 또는 특징]\n• [채널명]: [구체적인 트렌드 설명] - [매출 변화율 또는 금액 변화, 주요 아이템 또는 특징]\n• [채널명]: [구체적인 트렌드 설명] - [매출 변화율 또는 금액 변화, 주요 아이템 또는 특징]\n\n아래 채널별 트렌드 분석 결과를 참고하여 각 채널의 구체적인 변화 패턴, 성장률, 주요 아이템 등을 포함하여 작성하세요:\n{trend_text}'
            },
            {
                'sub_title': '전략 포인트',
                'ai_text': '위 데이터 분석 결과를 바탕으로 구체적이고 실행 가능한 전략을 3줄로 제시해줘. 각 전략은 불릿 포인트 형식으로 작성하세요.'
            },
 
        ]
        
        # 섹션 템플릿 동적 생성
        sections_template = ',\n    '.join([
            '{{\n      "div": "overall-{idx}",\n      "sub_title": "{sub_title}",\n      "ai_text": "{ai_text}"\n    }}'.format(
                idx=i+1,
                sub_title=section['sub_title'],
                ai_text=section['ai_text']
            )
            for i, section in enumerate(section_definitions)
        ])
        
        # LLM 프롬프트 생성 (JSON 형식 응답 요청)
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 채널 전략 전문가야. 당해 1월부터 현재월까지의 채널별 매출 추이를 분석하여 채널별 성과와 아이템 포트폴리오 전략을 제시해야 해.

**분석 기간**
- 시작: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월
- 종료: {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월
- 기간: {unique_months}개월

**전체 요약**
- 총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)
- 분석 채널 수: {unique_channels}개
- 분석 아이템 수: {unique_items}개

**월별 매출 분석 결과**
{insight_text}

**월별 매출 상세 데이터**
{json_dumps_safe(monthly_totals_list, ensure_ascii=False, indent=2)}

**채널별 트렌드 분석 결과 (특정 이벤트가 있는 채널 3개)**
{trend_text}

**전략 제안을 위한 데이터 분석**
{strategy_summary}

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 당해 1월부터 현재월까지 채널별 매출 추이를 분석하여:
1. 채널별 성과와 성장 패턴 파악
2. 채널별 핵심 아이템(클래스3) 식별
3. 채널별 매출 기여도와 비중 분석
4. 채널별 전략적 인사이트 제시

<데이터 샘플>
{json_dumps_safe(records[:100], ensure_ascii=False, indent=2)}

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

{{
  "title": "채널별 매출 종합분석 (당해 1월~현재월)",
  "sections": [
    {sections_template}
  ]
}}

<작성 가이드라인>
- **"주요 인사이트" 섹션은 반드시 아래 형식으로 3줄로 작성해야 합니다:**
  • [최대 매출월] 최대 [금액]백만원 - [구체적 원인 또는 특징 설명]
  • [최소 매출월] 최저 [금액]백만원 - [구체적 원인 또는 특징 설명]
  • [턴어라운드 월] 회복 [금액]백만원 - [구체적 회복 요인 설명]
  위 "월별 매출 분석 결과"의 데이터를 바탕으로 각 월의 구체적인 특징, 원인, 배경을 포함하여 작성하세요. 단순히 숫자만 나열하지 말고 분석적 인사이트를 포함하세요.

- **"채널 트렌드" 섹션은 반드시 아래 형식으로 3줄로 작성해야 합니다:**
  • [채널명]: [구체적인 트렌드 설명] - [매출 변화율 또는 금액 변화, 주요 아이템 또는 특징]
  • [채널명]: [구체적인 트렌드 설명] - [매출 변화율 또는 금액 변화, 주요 아이템 또는 특징]
  • [채널명]: [구체적인 트렌드 설명] - [매출 변화율 또는 금액 변화, 주요 아이템 또는 특징]
  위 "채널별 트렌드 분석 결과"의 데이터를 바탕으로 각 채널의 구체적인 변화 패턴, 성장률, 주요 아이템, 특징 등을 포함하여 작성하세요. 단순히 채널명만 나열하지 말고 구체적인 수치와 분석을 포함하세요.

- **"전략 포인트" 섹션은 반드시 아래 형식으로 3줄로 작성해야 합니다:**
  • [구체적인 전략 제안 1]
  • [구체적인 전략 제안 2]
  • [구체적인 전략 제안 3]
  위 "전략 제안을 위한 데이터 분석" 결과를 바탕으로 구체적이고 실행 가능한 전략을 제시하세요. 채널별 매출 기여도, 성장/하락 채널, 아이템 집중도 등을 고려하여 실용적인 전략을 제안하세요. 추가 설명 없이 3줄만 작성하세요.

- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 백만원 단위로 표시하고 절대 변형하지 말 것
- 채널별 구매 패턴과 성장 추세 분석
- 채널별 핵심 아이템 식별
- 전년대비 변화에 대한 구체적 원인과 효과 분석
- 단기 전략 방향과 중장기 전략 방향을 구체적으로 제시
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시 (예: "첫 번째 줄\\n두 번째 줄")
- ai_text 내에서 여러 문단이나 항목을 나눌 때는 \\n\\n을 사용
- 불릿 포인트나 리스트 항목 사이에는 \\n을 사용
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱 (마크다운 코드 블록 제거)
        analysis_response = analysis_response.strip()
        if analysis_response.startswith('```json'):
            analysis_response = analysis_response[7:]
        if analysis_response.startswith('```'):
            analysis_response = analysis_response[3:]
        if analysis_response.endswith('```'):
            analysis_response = analysis_response[:-3]
        analysis_response = analysis_response.strip()
        
        try:
            analysis_data = json.loads(analysis_response)
        except json.JSONDecodeError as e:
            print(f"[WARNING] JSON 파싱 실패: {e}")
            print(f"[WARNING] 응답 내용: {analysis_response[:500]}")
            # 기본 구조로 대체
            analysis_data = {
                "title": "채널별 매출 종합분석 (당해 1월~현재월)",
                "sections": [
                    {"div": "overall-1", "sub_title": "분석 결과", "ai_text": analysis_response}
                ]
            }
        
        # JSON 데이터 생성
        # yyyymm_py 계산 (전년 동월)
        previous_year = int(yyyymm_end[:4]) - 1
        yyyymm_py = f"{previous_year}{yyyymm_end[4:6]}"
        
        json_data = {
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm_end,
            'yyyymm_py': yyyymm_py,
            'analysis_data': analysis_data,
            'summary': {
                'total_sales': round(total_sales / 1000000, 2),
                'unique_channels': unique_channels,
                'unique_items': unique_items,
                'unique_months': unique_months,
                'analysis_period': f"{yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월"
            },
            'channel_summary': channel_summary,
            'raw_data': {
                'sample_records': [
                    {
                        'PST_YYYYMM': r.get('PST_YYYYMM', ''),
                        'CHNL_NM': r.get('CHNL_NM', ''),
                        'CLASS3': r.get('CLASS3', ''),
                        'SALE_AMT': float(r.get('SALE_AMT', 0)),
                        'SALE_RATIO': float(r.get('SALE_RATIO', 0))
                    }
                    for r in records[:50]
                ],
                'total_records_count': len(records)
            },
            'trend_data': {
                'trend_months': sorted(list(set(r.get('PST_YYYYMM', '') for r in records))),
                'monthly_totals': monthly_totals_list,
                'monthly_details': [
                    {
                        'yyyymm': r.get('PST_YYYYMM', ''),
                        'chnl_nm': r.get('CHNL_NM', ''),
                        'class3': r.get('CLASS3', ''),
                        'sale_amt': round(float(r.get('SALE_AMT', 0)) / 1000000, 2),
                        'sale_ratio': float(r.get('SALE_RATIO', 0))
                    }
                    for r in records
                ]
            }
        }
        
        # 파일 저장 (14-1-1-1로 저장)
        filename = f"14-1-1-1.{brd_cd}_채널별_매출_종합분석(당해1월~현재월)"
        save_json(json_data, filename)
        
        # Markdown도 저장 (analysis_data의 sections를 조합)
        markdown_content = f"# {analysis_data.get('title', '채널별 매출 분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def analyze_operating_expense(yyyymm, brd_cd):
    """영업비 추이분석 - CTGR1별 개별 분석"""
    print(f"\n{'='*60}")
    print(f"영업비 추이분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 전년 동월 계산
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        previous_year = current_year - 1
        yyyymm_py = f"{previous_year:04d}{current_month:02d}"
        
        print(f"분석 기간: {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월")
        
        # 1. 모든 CTGR1 조회
        all_detail_sql = get_ad_expense_detail_query(yyyymm, yyyymm_py, brd_cd)
        all_detail_df = run_query(all_detail_sql, engine)
        all_detail_records = all_detail_df.to_dicts()
        
        if not all_detail_records:
            print("데이터가 없습니다.")
            return None
        
        # 2. CTGR1별로 그룹화
        ctgr1_groups = {}
        for record in all_detail_records:
            ctgr1 = record.get('CTGR1', '')
            if ctgr1 and ctgr1 not in ctgr1_groups:
                ctgr1_groups[ctgr1] = []
            if ctgr1:
                ctgr1_groups[ctgr1].append(record)
        
        print(f"발견된 CTGR1 카테고리: {len(ctgr1_groups)}개")
        for ctgr1 in ctgr1_groups.keys():
            print(f"  - {ctgr1}")
        
        # 3. 각 CTGR1별로 분석 수행
        results = []
        for ctgr1, detail_records in ctgr1_groups.items():
            print(f"\n{'='*60}")
            print(f"분석 중: {ctgr1}")
            print(f"{'='*60}")
            
            try:
                result = analyze_operating_expense_by_ctgr1(yyyymm, brd_cd, ctgr1, detail_records, engine)
                if result:
                    results.append(result)
            except Exception as e:
                print(f"[ERROR] {ctgr1} 분석 중 오류 발생: {e}")
                continue
        
        print(f"\n[OK] 전체 영업비 분석 완료! ({len(results)}개 카테고리 분석)")
        return results
        
    finally:
        engine.dispose()

def analyze_operating_expense_by_ctgr1(yyyymm, brd_cd, ctgr1, detail_records, engine):
    """CTGR1별 영업비 추이분석"""
    # 전년 동월 계산
    current_year = int(yyyymm[:4])
    current_month = int(yyyymm[4:6])
    previous_year = current_year - 1
    yyyymm_py = f"{previous_year:04d}{current_month:02d}"
    
    # 1. 전체 합계 계산
    curr_total = sum(float(r.get('AD_TTL_AMT', 0)) for r in detail_records if r.get('PST_YYYYMM') == yyyymm)
    prev_total = sum(float(r.get('AD_TTL_AMT', 0)) for r in detail_records if r.get('PST_YYYYMM') == yyyymm_py)
    change_amount = curr_total - prev_total
    change_pct = (change_amount / prev_total * 100) if prev_total != 0 else 0
        
    print(f"전년 합계: {prev_total:,.0f}원 ({prev_total/1000000:.2f}백만원)")
    print(f"당해 합계: {curr_total:,.0f}원 ({curr_total/1000000:.2f}백만원)")
    print(f"변화액: {change_amount:,.0f}원 ({change_pct:.1f}%)")
    
    # 2. 12개월 추세 데이터 (현재 월부터 12개월 전까지)
    trend_months = []
    for i in range(12):
        year = current_year
        month = current_month - i
        while month <= 0:
            month += 12
            year -= 1
        trend_months.append(f"{year:04d}{month:02d}")
    trend_months.reverse()
    
    trend_sql = get_ad_expense_trend_query(trend_months, brd_cd, ctgr1)
    trend_df = run_query(trend_sql, engine)
    trend_records = trend_df.to_dicts()
    
    # 3. 월별 합계 계산
    monthly_totals = {}
    for record in trend_records:
        month = record.get('PST_YYYYMM', '')
        amount = float(record.get('TTL_USE_AMT', 0))
        if month not in monthly_totals:
            monthly_totals[month] = 0
        monthly_totals[month] += amount
    
    monthly_totals_list = [
        {'yyyymm': month, 'total_amount': round(amount / 1000000, 2)}
        for month, amount in sorted(monthly_totals.items())
    ]
    
    # 4. 카테고리별 데이터 정리
    categories = []
    prev_year_dict = {}
    curr_year_dict = {}
    
    for record in detail_records:
        pst_yyyymm = record.get('PST_YYYYMM', '')
        ctgr2 = record.get('CTGR2', '')
        ctgr3 = record.get('CTGR3', '')
        gl_nm = record.get('GL_NM', '')
        amount = float(record.get('AD_TTL_AMT', 0))
        
        key = f"{ctgr2}|{ctgr3}|{gl_nm}"
        
        if pst_yyyymm == yyyymm_py:
            prev_year_dict[key] = {
                'ctgr2': ctgr2,
                'ctgr3': ctgr3,
                'gl_nm': gl_nm,
                'amount': amount
            }
        elif pst_yyyymm == yyyymm:
            curr_year_dict[key] = {
                'ctgr2': ctgr2,
                'ctgr3': ctgr3,
                'gl_nm': gl_nm,
                'amount': amount
            }
        
    # 모든 키 통합
    all_keys = set(prev_year_dict.keys()) | set(curr_year_dict.keys())
    
    for key in all_keys:
        prev_data = prev_year_dict.get(key, {'amount': 0, 'ctgr2': '', 'ctgr3': '', 'gl_nm': ''})
        curr_data = curr_year_dict.get(key, {'amount': 0, 'ctgr2': '', 'ctgr3': '', 'gl_nm': ''})
        
        prev_amt = prev_data['amount'] / 1000000
        curr_amt = curr_data['amount'] / 1000000
        change_amt = curr_amt - prev_amt
        change_pct_val = (change_amt / prev_amt * 100) if prev_amt != 0 else 0
        
        categories.append({
            'ctgr2': curr_data.get('ctgr2') or prev_data.get('ctgr2', ''),
            'ctgr3': curr_data.get('ctgr3') or prev_data.get('ctgr3', ''),
            'gl_nm': curr_data.get('gl_nm') or prev_data.get('gl_nm', ''),
            'prev_year': round(prev_amt, 2),
            'curr_year': round(curr_amt, 2),
            'change': round(change_amt, 2),
            'change_pct': round(change_pct_val, 1),
            'is_new': prev_amt == 0 and curr_amt > 0,
            'is_discontinued': prev_amt > 0 and curr_amt == 0
        })
    
    # 5. 카테고리 요약
    increased = [c for c in categories if c['change'] > 0]
    decreased = [c for c in categories if c['change'] < 0]
    new_investments = [c for c in categories if c['is_new']]
    discontinued = [c for c in categories if c['is_discontinued']]
    
    # 6. LLM 프롬프트 생성 (JSON 형식 응답 요청)
    total_records_json = json_dumps_safe([
        {'PST_YYYYMM': yyyymm_py, 'TOTAL_AMT': prev_total},
        {'PST_YYYYMM': yyyymm, 'TOTAL_AMT': curr_total}
    ], ensure_ascii=False, indent=2)
    
    detail_records_json = json_dumps_safe([
            {
                'PST_YYYYMM': r.get('PST_YYYYMM', ''),
                'BRD_CD': brd_cd,
                'BRD_NM': BRAND_CODE_MAP.get(brd_cd, brd_cd),
                'CTGR1': r.get('CTGR1', ''),
                'CTGR2': r.get('CTGR2', ''),
                'CTGR3': r.get('CTGR3', ''),
                'GL_NM': r.get('GL_NM', ''),
                'TTL_USE_AMT': float(r.get('AD_TTL_AMT', 0))
            }
        for r in detail_records
    ], ensure_ascii=False, indent=2)
    
    prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 마케팅 전략 책임자야. {previous_year}년 {current_month}월과 {current_year}년 {current_month}월의 {ctgr1}를 비교 분석하여 투자 효율성과 최적화 방안을 제시해야 해.

**분석 기간**
- 당해: {current_year}년 {current_month}월
- 전년: {previous_year}년 {current_month}월

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월 {ctgr1} 투자 변화를 분석하여 전략의 효과성과 향후 예산 배분 전략을 경영관리팀에게 수립해줘.

<전체 합계 데이터>
{total_records_json}

<세부 계정별 데이터>
{detail_records_json}

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

{{
  "title": "{ctgr1} 분석",
  "sections": [
    {{
      "div": "overall-1",
      "sub_title": "투자 방향성 종합 평가",
      "ai_text": "전년대비 {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월 광고비 변화를 종합적으로 평가한 내용 (예: 선택적 축소 - 효율성 중심 예산 재배분 등)"
    }},
    {{
      "div": "overall-2",
      "sub_title": "효율적 투자 영역",
      "ai_text": "효과적인 투자 영역들을 불릿 포인트로 나열 (예: • 모델료 신규 투입: 122.9백만원으로 브랜드 이미지 제고 및 소비자 어필 강화 등)"
    }},
    {{
      "div": "overall-3",
      "sub_title": "주의 필요 영역",
      "ai_text": "주의가 필요한 영역들을 불릿 포인트로 나열 (예: • E-BIZ 매체광고 증가: 9.2→14.0백만원(+51.8%)로 급격한 증가 원인 등)"
    }},
    {{
      "div": "overall-4",
      "sub_title": "이상징후 및 리스크 감지",
      "ai_text": "이상징후와 리스크를 구체적으로 설명 (예: • 예산 배분의 극단적 변화: 일부 계정의 전액 삭감(기타 광고비)과 신규 대규모 투입(모델료)이 동시 발생하여 마케팅 전략의 급격한 방향 전환을 시사합니다 등)"
    }},
    {{
      "div": "overall-5",
      "sub_title": "마케팅 전략 최적화 방안",
      "ai_text": "단기 전략 방향과 중장기 전략 방향을 구체적으로 제시 (예: ### 즉시 실행 방안\\n1. 모델 마케팅 효과 측정: ... 등)"
    }}
  ]
}}

<작성 가이드라인>
- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 백만원 단위로 표시하고 절대 변형하지 말 것
- 모든 {ctgr1} 계정 (CTGR3) 누락 없이 분석
- 전년대비 변화에 대한 구체적 원인과 효과 분석
- 단기 전략 방향성 제시와 중장기 전략 방향을 구체적으로 제시
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시 (예: "첫 번째 줄\\n두 번째 줄")
- ai_text 내에서 여러 문단이나 항목을 나눌 때는 \\n\\n을 사용
- 불릿 포인트나 리스트 항목 사이에는 \\n을 사용
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
"""
        
    # 7. LLM 호출 (JSON 응답)
    analysis_response = call_llm(prompt, max_tokens=4000)
    
    # JSON 파싱 (마크다운 코드 블록 제거)
    analysis_response = analysis_response.strip()
    if analysis_response.startswith('```json'):
        analysis_response = analysis_response[7:]
    if analysis_response.startswith('```'):
        analysis_response = analysis_response[3:]
    if analysis_response.endswith('```'):
        analysis_response = analysis_response[:-3]
    analysis_response = analysis_response.strip()
    
    try:
        analysis_data = json.loads(analysis_response)
        # sections에 div 필드 추가 (overall-1, overall-2, ...)
        for idx, section in enumerate(analysis_data.get('sections', []), 1):
            if 'div' not in section:
                section['div'] = f'overall-{idx}'
    except json.JSONDecodeError as e:
        print(f"[WARNING] JSON 파싱 실패: {e}")
        print(f"[WARNING] 응답 내용: {analysis_response[:500]}")
        # 기본 구조로 대체
        analysis_data = {
            "title": f"{ctgr1} 분석",
            "sections": [
                {"div": "overall-1", "sub_title": "분석 결과", "ai_text": analysis_response}
            ]
        }
    
    # 8. JSON 데이터 생성
    json_data = {
        'brand_cd': brd_cd,
        'yyyymm': yyyymm,
        'ctgr1': ctgr1,
        'analysis_data': analysis_data,
            'summary': {
                'prev_year_total': round(prev_total / 1000000, 2),
                'curr_year_total': round(curr_total / 1000000, 2),
                'change_amount': round(change_amount / 1000000, 2),
                'change_pct': round(change_pct, 1),
                'investment_direction': '증가' if change_amount > 0 else '축소' if change_amount < 0 else '유지'
            },
            'categories': categories,
            'category_summary': {
                'increased': increased,
                'decreased': decreased,
                'new_investments': new_investments,
                'discontinued': discontinued
            },
            'raw_data': {
                'total_records': [
                    {'PST_YYYYMM': yyyymm_py, 'TOTAL_AMT': prev_total},
                    {'PST_YYYYMM': yyyymm, 'TOTAL_AMT': curr_total}
                ],
                'detail_records': [
                    {
                        'PST_YYYYMM': r.get('PST_YYYYMM', ''),
                        'BRD_CD': brd_cd,
                        'BRD_NM': BRAND_CODE_MAP.get(brd_cd, brd_cd),
                        'CTGR1': r.get('CTGR1', ''),
                        'CTGR2': r.get('CTGR2', ''),
                        'CTGR3': r.get('CTGR3', ''),
                        'GL_NM': r.get('GL_NM', ''),
                        'TTL_USE_AMT': float(r.get('AD_TTL_AMT', 0))
                    }
                    for r in detail_records
                ]
            },
            'trend_data': {
                'trend_months': trend_months,
                'monthly_totals': monthly_totals_list,
                'monthly_details': [
                    {
                        'yyyymm': r.get('PST_YYYYMM', ''),
                        'ctgr2': r.get('CTGR2', ''),
                        'ctgr3': r.get('CTGR3', ''),
                        'gl_nm': r.get('GL_NM', ''),
                        'amount': round(float(r.get('TTL_USE_AMT', 0)) / 1000000, 2)
                    }
                    for r in trend_records
                ]
            }
        }
        
    # 9. 파일 저장
    # 파일명에서 특수문자 제거 및 안전한 이름 생성
    safe_ctgr1 = ctgr1.replace('/', '_').replace('(', '_').replace(')', '_').replace(' ', '_')
    filename = f"6-1-1-1.{brd_cd}_{safe_ctgr1}_추이분석"
    save_json(json_data, filename)
    
    # Markdown도 저장 (analysis_data의 sections를 조합)
    markdown_content = f"# {analysis_data.get('title', f'{ctgr1} 분석')}\n\n"
    for section in analysis_data.get('sections', []):
        markdown_content += f"## {section.get('sub_title', '')}\n\n"
        markdown_content += f"{section.get('ai_text', '')}\n\n"
    save_markdown(markdown_content, filename)
    
    print(f"[OK] {ctgr1} 분석 완료!\n")
    return json_data

def get_discount_rate_overall_query(yyyymm, brd_cd):
    """할인율 종합분석 쿼리 (당해/전년 동월 비교) - 7-1-1-1용"""
    # 분석 기간 계산
    current_year = int(yyyymm[:4])
    current_month = int(yyyymm[4:6])
    previous_year = current_year - 1
    yyyymm_py = f"{previous_year:04d}{current_month:02d}"
    
    # -6개월 계산 (시즌 구분용)
    minus6_year = current_year
    minus6_month = current_month - 6
    while minus6_month <= 0:
        minus6_month += 12
        minus6_year -= 1
    yyyymm_minus6 = f"{minus6_year:04d}{minus6_month:02d}"
    
    minus6_year_py = previous_year
    minus6_month_py = current_month - 6
    while minus6_month_py <= 0:
        minus6_month_py += 12
        minus6_year_py -= 1
    yyyymm_py_minus6 = f"{minus6_year_py:04d}{minus6_month_py:02d}"
    
    # 현재 날짜 문자열 (시즌 계산용)
    current_date_str = f"{current_year}-{current_month:02d}-01"
    
    return f"""
    -- cy_item : 당해 아이템 구분 기준
    with cy_item as (
        select a.prdt_cd
            , a.sesn
            , a.prdt_hrrc1_nm
            , a.prdt_hrrc2_nm
            , a.prdt_hrrc3_nm
            , case when ('{yyyymm}' between b.start_yyyymm and b.end_yyyymm) and prdt_hrrc1_nm = '의류'
                        then decode(a.sesn, 'N', 'S', a.sesn) || ' ' || a.prdt_hrrc1_nm -- 당시즌 의류
                    when ('{yyyymm_minus6}' between b.start_yyyymm and b.end_yyyymm) and prdt_hrrc1_nm = '의류' -- -6개월
                        then decode(a.sesn, 'N', 'S', a.sesn) || ' ' || a.prdt_hrrc1_nm-- 전시즌 의류
                    when (b.start_yyyymm > '{yyyymm}') and prdt_hrrc1_nm = '의류'
                        then '차기시즌 의류'
                    when (b.start_yyyymm < '{yyyymm_minus6}') and prdt_hrrc1_nm = '의류' -- -6개월
                        then '과시즌 의류'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Headwear'
                        then '모자'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Shoes' 
                        then '신발'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Bag'   
                        then '가방'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Acc_etc'
                        then '기타ACC'
                    else '기타' end as item_std
        from sap_fnf.mst_prdt a
        left join comm.mst_sesn b
            on a.sesn = b.sesn
        where 1=1
            and brd_cd = '{brd_cd}'
    )
    -- py_item : 전년 아이템 구분 기준
    , py_item as (
        select a.prdt_cd
            , a.sesn
            , a.prdt_hrrc1_nm
            , a.prdt_hrrc2_nm
            , a.prdt_hrrc3_nm
            , case when ('{yyyymm_py}' between b.start_yyyymm and b.end_yyyymm) and prdt_hrrc1_nm = '의류'
                        then (left(a.sesn,2)+1)::int || decode(right(a.sesn,1), 'N', 'S', right(a.sesn,1)) || ' ' || a.prdt_hrrc1_nm -- 당시즌 의류
                    when ('{yyyymm_py_minus6}' between b.start_yyyymm and b.end_yyyymm) and prdt_hrrc1_nm = '의류'   -- -1.6개월
                        then (left(a.sesn,2)+1)::int || decode(right(a.sesn,1), 'N', 'S', right(a.sesn,1)) || ' ' || a.prdt_hrrc1_nm-- 전시즌 의류
                    when (b.start_yyyymm > '{yyyymm_py}') and prdt_hrrc1_nm = '의류'
                        then '차기시즌 의류'
                    when (b.start_yyyymm < '{yyyymm_py_minus6}') and prdt_hrrc1_nm = '의류' -- -1.6개월
                        then '과시즌 의류'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Headwear'
                        then '모자'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Shoes' 
                        then '신발'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Bag'   
                        then '가방'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Acc_etc'
                        then '기타ACC'
                    else '기타' end as item_std
        from sap_fnf.mst_prdt a
        left join comm.mst_sesn b
            on a.sesn = b.sesn
        where 1=1
            and brd_cd = '{brd_cd}'
    ), raw as (
        select  'cy' as div
            , case
                when b.mgmt_chnl_cd = '4' then '자사몰'
                when b.mgmt_chnl_cd = '5' then '제휴몰'
                when b.mgmt_chnl_cd in ('3', '11', 'C3') then '직영점' 
                when b.mgmt_chnl_cd in ('7', '12') then '아울렛'       
                else b.mgmt_chnl_nm
            end as chnl_nm
            , c.item_std
            , sum(a.tag_sale_amt) AS tag_sale_amt
            , sum(a.act_sale_amt) AS act_sale_amt
        from sap_fnf.dm_pl_shop_prdt_m a
        join sap_fnf.mst_shop b
        on a.brd_cd = b.brd_cd
        and a.shop_cd = b.sap_shop_cd
        join cy_item c
        on a.prdt_cd = c.prdt_cd
        where a.corp_cd = '1000'
        and a.chnl_cd not in ('0', '9', '8', '99')
        and a.brd_cd = '{brd_cd}'
        and a.pst_yyyymm = '{yyyymm}'
        group by 1,2,3
        union all
        select  'py' as div
            , case
                when b.mgmt_chnl_cd = '4' then '자사몰'
                when b.mgmt_chnl_cd = '5' then '제휴몰'
                when b.mgmt_chnl_cd in ('3', '11', 'C3') then '직영점' 
                when b.mgmt_chnl_cd in ('7', '12') then '아울렛'       
                else b.mgmt_chnl_nm
            end as chnl_nm
            , c.item_std
            , sum(a.tag_sale_amt) AS tag_sale_amt
            , sum(a.act_sale_amt) AS act_sale_amt
        from sap_fnf.dm_pl_shop_prdt_m a
        join sap_fnf.mst_shop b
        on a.brd_cd = b.brd_cd
        and a.shop_cd = b.sap_shop_cd
        join py_item c
        on a.prdt_cd = c.prdt_cd
        where a.corp_cd = '1000'
        and a.chnl_cd not in ('0', '9', '8', '99')
        and a.brd_cd = '{brd_cd}'
        and a.pst_yyyymm = '{yyyymm_py}'
        group by 1,2,3
    ), class_summary as (
        select item_std
            , sum(case when div = 'cy' then tag_sale_amt else  0 end) tag_sale_amt_cy
            , sum(case when div = 'py' then tag_sale_amt else  0 end) tag_sale_amt_py
            , sum(case when div = 'cy' then act_sale_amt else  0 end) act_sale_amt_cy
            , sum(case when div = 'py' then act_sale_amt else  0 end) act_sale_amt_py
        from raw
        where 1=1
        group by 1
    ), chnl_summary as (
        select chnl_nm
            , sum(case when div = 'cy' then tag_sale_amt else  0 end) tag_sale_amt_cy
            , sum(case when div = 'py' then tag_sale_amt else  0 end) tag_sale_amt_py
            , sum(case when div = 'cy' then act_sale_amt else  0 end) act_sale_amt_cy
            , sum(case when div = 'py' then act_sale_amt else  0 end) act_sale_amt_py
    from raw
    group by chnl_nm
    ), chnl_seq as (
        select '플래그쉽' as chnl_nm, 1 as chnl_seq
        union all select '백화점' as chnl_nm, 2 as chnl_seq
        union all select '대리점' as chnl_nm, 3 as chnl_seq
        union all select '직영점' as chnl_nm, 4 as chnl_seq
        union all select '자사몰' as chnl_nm, 5 as chnl_seq
        union all select '제휴몰' as chnl_nm, 6 as chnl_seq
        union all select '아울렛' as chnl_nm, 7 as chnl_seq
        union all select '면세점' as chnl_nm, 8 as chnl_seq
        union all select 'RF' as chnl_nm, 9 as chnl_seq
        union all select case when to_char('{current_date_str}'::date, 'MM') between '03' and '08' then to_char('{current_date_str}'::date, 'YY') || 'S'      
                            when to_char('{current_date_str}'::date, 'MM') between '09' and '12' then to_char('{current_date_str}'::date, 'YY') || 'F'        
                            else (to_char('{current_date_str}'::date, 'YY') -1)::float || 'F'  end || ' 의류' as chnl_nm, 101 as chnl_seq
        union all select case when to_char('{current_date_str}'::date, 'MM') between '03' and '08' then (to_char('{current_date_str}'::date, 'YY')-1)::float || 'F'
                            when to_char('{current_date_str}'::date, 'MM') between '09' and '12' then to_char('{current_date_str}'::date, 'YY') || 'S'        
                            else (to_char('{current_date_str}'::date, 'YY') -1)::float || 'S'  end || ' 의류' as chnl_nm, 102 as chnl_seq
        union all select '과시즌 의류' as chnl_nm, 103 as chnl_seq     
        union all select '모자' as chnl_nm, 201 as chnl_seq
        union all select '신발' as chnl_nm, 202 as chnl_seq
        union all select '가방' as chnl_nm, 203 as chnl_seq
        union all select '기타ACC' as chnl_nm, 204 as chnl_seq
    ), total_summary as (
        select sum(case when div = 'cy' then tag_sale_amt else  0 end) tag_sale_amt_cy
            , sum(case when div = 'py' then tag_sale_amt else  0 end) tag_sale_amt_py
            , sum(case when div = 'cy' then act_sale_amt else  0 end) act_sale_amt_cy
            , sum(case when div = 'py' then act_sale_amt else  0 end) act_sale_amt_py
        from raw
    ), main as (
        select '전체' chnl_nm
            , 1 as seq
            , case when tag_sale_amt_cy = 0 then 0 else round((1 - (act_sale_amt_cy / tag_sale_amt_cy)) * 100, 1) end as discount
            , case when tag_sale_amt_py = 0 then 0 else round((1 - (act_sale_amt_py / tag_sale_amt_py)) * 100, 1) end as discount_py
        from total_summary
        union all
        select chnl_nm
            , 2 as seq
            , case when tag_sale_amt_cy = 0 then 0 else round((1 - (act_sale_amt_cy / tag_sale_amt_cy)) * 100, 1) end as discount
            , case when tag_sale_amt_py = 0 then 0 else round((1 - (act_sale_amt_py / tag_sale_amt_py)) * 100, 1) end as discount_py
        from chnl_summary
        union all
        select item_std
            , 4 as seq
            , case when tag_sale_amt_cy = 0 then 0 else round((1 - (act_sale_amt_cy / tag_sale_amt_cy)) * 100, 1) end as discount
            , case when tag_sale_amt_py = 0 then 0 else round((1 - (act_sale_amt_py / tag_sale_amt_py)) * 100, 1) end as discount_py
        from class_summary
    )
    select a.chnl_nm
        , a.discount
        , discount - discount_py as yoy
        , a.seq
        , b.chnl_seq
    from main a
    left join chnl_seq b
      on a.chnl_nm = b.chnl_nm
    order by seq, b.chnl_seq
    """

def analyze_discount_rate_overall(yyyymm, brd_cd):
    """할인율 종합분석 (당해/전년 동월 비교) - 7-1-1-1"""
    print(f"\n{'='*60}")
    print(f"할인율 종합분석 시작 (7-1-1-1): {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
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
        sql = get_discount_rate_overall_query(yyyymm, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 전체 할인율 계산
        total_record = next((r for r in records if r.get('SEQ') == 1), None)
        total_discount_cy = float(total_record.get('DISCOUNT', 0)) if total_record else 0
        total_discount_py = total_discount_cy - float(total_record.get('YOY', 0)) if total_record else 0
        
        # 채널별 데이터 추출 (seq = 2)
        channel_data = [r for r in records if r.get('SEQ') == 2]
        
        # 채널별 할인율 데이터 정리
        channel_summary = {}
        for record in channel_data:
            chnl_nm = record.get('CHNL_NM', '기타')
            discount_cy = float(record.get('DISCOUNT', 0))
            yoy = float(record.get('YOY', 0))
            discount_py = discount_cy - yoy
            
            channel_summary[chnl_nm] = {
                'discount_cy': round(discount_cy, 1),
                'discount_py': round(discount_py, 1),
                'yoy': round(yoy, 1)
            }
        
        # 전략 우수 채널 (할인율이 낮고 전년대비 개선)
        excellent_channels = [
            {
                'chnl_nm': chnl,
                'discount_cy': data['discount_cy'],
                'yoy': data['yoy']
            }
            for chnl, data in channel_summary.items()
            if data['discount_cy'] < total_discount_cy and data['yoy'] < 0  # 할인율이 평균보다 낮고 개선됨
        ]
        excellent_channels.sort(key=lambda x: (x['discount_cy'], x['yoy']))
        
        # 주의 필요 채널 (할인율이 높거나 악화)
        warning_channels = [
            {
                'chnl_nm': chnl,
                'discount_cy': data['discount_cy'],
                'yoy': data['yoy']
            }
            for chnl, data in channel_summary.items()
            if data['discount_cy'] > total_discount_cy or data['yoy'] > 0  # 할인율이 평균보다 높거나 악화됨
        ]
        warning_channels.sort(key=lambda x: (x['discount_cy'], -x['yoy']), reverse=True)
        
        # 아이템별 데이터 추출 (seq = 4)
        item_data = [r for r in records if r.get('SEQ') == 4]
        
        unique_channels = len(channel_summary)
        unique_items = len(item_data)
        
        print(f"전년 할인율: {total_discount_py:.1f}%")
        print(f"당해 할인율: {total_discount_cy:.1f}%")
        print(f"전년대비 변화: {round(total_discount_cy - total_discount_py, 1)}%p")
        print(f"채널 수: {unique_channels}개")
        print(f"아이템 수: {unique_items}개")
        
        # LLM 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 가격 전략 전문가야. 당해와 전년 동월의 할인율을 비교 분석하여 채널별 할인 전략의 효율성을 평가하고 최적화 방안을 제시해야 해.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 전년 할인율: {total_discount_py:.1f}%
- 당해 할인율: {total_discount_cy:.1f}%
- 전년대비 변화: {round(total_discount_cy - total_discount_py, 1)}%p
- 분석 채널 수: {unique_channels}개
- 분석 아이템 수: {unique_items}개

**채널별 할인율 데이터**
{json_dumps_safe(channel_summary, ensure_ascii=False, indent=2)}

**전략 우수 채널 (할인율 낮고 개선)**
{json_dumps_safe(excellent_channels[:5], ensure_ascii=False, indent=2)}

**주의 필요 채널 (할인율 높거나 악화)**
{json_dumps_safe(warning_channels[:5], ensure_ascii=False, indent=2)}

**아이템별 할인율 데이터 (샘플)**
{json_dumps_safe(item_data[:20], ensure_ascii=False, indent=2)}

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 할인율 종합분석을 통해:
1. 전략 우수 채널: 할인율이 낮고 전년대비 개선된 채널 식별 및 성공 요인 분석
2. 주의 필요 채널: 할인율이 높거나 악화된 채널 식별 및 개선 방안 제시
3. AI 권장 사항: 할인율 최적화를 위한 구체적이고 실행 가능한 전략 제시

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

{{
  "title": "할인율 종합분석",
  "sections": [
    {{
      "div": "overall-1",
      "sub_title": "전략 우수 채널",
      "ai_text": "할인율이 낮고 전년대비 개선된 채널들을 4줄 이하로 간략하게 요약해줘. 각 채널의 할인율 수치와 개선 정도를 포함하여 구체적으로 작성하세요."
    }},
    {{
      "div": "overall-2",
      "sub_title": "주의 필요 채널",
      "ai_text": "할인율이 높거나 악화된 채널들을 4줄 이하로 간략하게 요약해줘. 각 채널의 할인율 수치와 악화 정도를 포함하여 구체적으로 작성하세요."
    }},
    {{
      "div": "overall-3",
      "sub_title": "AI 권장 사항",
      "ai_text": "할인율 최적화를 위한 구체적이고 실행 가능한 전략을 4줄 이하로 간략하게 제시해줘. 채널별 특성을 고려한 실용적인 권장사항을 작성하세요."
    }}
  ]
}}

<작성 가이드라인>
- 각 섹션의 ai_text는 반드시 4줄을 넘지 않도록 간략하게 작성
- 숫자는 할인율(%)로 표시하고 절대 변형하지 말 것
- 채널별 할인율 수치와 전년대비 변화를 구체적으로 언급
- 전략 우수 채널의 성공 요인 분석
- 주의 필요 채널의 개선 방안 제시
- AI 권장 사항은 실행 가능한 구체적 전략으로 작성
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱 (마크다운 코드 블록 제거)
        analysis_response = analysis_response.strip()
        if analysis_response.startswith('```json'):
            analysis_response = analysis_response[7:]
        if analysis_response.startswith('```'):
            analysis_response = analysis_response[3:]
        if analysis_response.endswith('```'):
            analysis_response = analysis_response[:-3]
        analysis_response = analysis_response.strip()
        
        try:
            analysis_data = json.loads(analysis_response)
            # sections에 div 필드 추가 (overall-1, overall-2, overall-3)
            for idx, section in enumerate(analysis_data.get('sections', []), 1):
                if 'div' not in section:
                    section['div'] = f'overall-{idx}'
        except json.JSONDecodeError as e:
            print(f"[WARNING] JSON 파싱 실패: {e}")
            print(f"[WARNING] 응답 내용: {analysis_response[:500]}")
            # 기본 구조로 대체
            analysis_data = {
                "title": "할인율 종합분석",
                "sections": [
                    {"div": "overall-1", "sub_title": "전략 우수 채널", "ai_text": analysis_response},
                    {"div": "overall-2", "sub_title": "주의 필요 채널", "ai_text": ""},
                    {"div": "overall-3", "sub_title": "AI 권장 사항", "ai_text": ""}
                ]
            }
        
        # JSON 데이터 생성
        # yyyymm_py 계산 (전년 동월)
        json_data = {
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'analysis_data': analysis_data,
            'summary': {
                'total_discount_cy': round(total_discount_cy, 1),
                'total_discount_py': round(total_discount_py, 1),
                'change_pct': round(total_discount_cy - total_discount_py, 1),
                'unique_channels': unique_channels,
                'unique_items': unique_items,
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'channel_summary': channel_summary,
            'excellent_channels': excellent_channels[:5],
            'warning_channels': warning_channels[:5],
            'raw_data': {
                'sample_records': [
                    {
                        'CHNL_NM': r.get('CHNL_NM', ''),
                        'DISCOUNT': float(r.get('DISCOUNT', 0)),
                        'YOY': float(r.get('YOY', 0)),
                        'SEQ': int(r.get('SEQ', 0))
                    }
                    for r in records[:100]
                ],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        filename = f"7-1-1-1.{brd_cd}_할인율_종합분석"
        save_json(json_data, filename)
        
        # Markdown도 저장 (analysis_data의 sections를 조합)
        markdown_content = f"# {analysis_data.get('title', '할인율 종합분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def get_store_efficiency_overall_query(yyyymm, brd_cd):
    """매장효율성 종합분석 쿼리 (당해/전년 동월 비교) - 8-1-1-1용"""
    # 분석 기간 계산
    current_year = int(yyyymm[:4])
    current_month = int(yyyymm[4:6])
    previous_year = current_year - 1
    yyyymm_py = f"{previous_year:04d}{current_month:02d}"
    
    # 날짜 범위 계산
    current_date_start = f"{current_year}-{current_month:02d}-01"
    # 해당 월의 마지막 날짜 계산
    if current_month == 12:
        current_date_end = f"{current_year}-12-31"
    else:
        next_month = current_month + 1
        next_month_date = datetime(current_year, next_month, 1)
        last_day = (next_month_date - timedelta(days=1)).day
        current_date_end = f"{current_year}-{current_month:02d}-{last_day:02d}"
    
    previous_date_start = f"{previous_year}-{current_month:02d}-01"
    if current_month == 12:
        previous_date_end = f"{previous_year}-12-31"
    else:
        next_month = current_month + 1
        next_month_date = datetime(previous_year, next_month, 1)
        last_day = (next_month_date - timedelta(days=1)).day
        previous_date_end = f"{previous_year}-{current_month:02d}-{last_day:02d}"
    
    return f"""
    with raw as (
        select yymm 
             , chnl_nm
             , count(distinct case when  brd_cd = 'I' and shop_shrt_nm not like '%M%' then shop_cd when brd_cd <> 'I' then shop_cd else null end) shop_cnt
             , sum(act_sale_amt) act_sale_amt
        from (
            select to_char(a.pst_dt, 'YYYYMM') yymm
                , case 
                    when b.mgmt_chnl_cd = '4' then '자사몰'
                    when b.mgmt_chnl_cd = '5' then '제휴몰'
                    when b.mgmt_chnl_cd in ('3', '11', 'C3') then '직영점'
                    when b.mgmt_chnl_cd in ('7', '12') then '아울렛'
                    else b.mgmt_chnl_nm
                end as chnl_nm
                , a.cust_cd as shop_cd
                , a.brd_cd
                , max(b.shop_shrt_nm) as shop_shrt_nm
                , sum(act_sale_amt) act_sale_amt
            from sap_fnf.dw_copa_d a
            join sap_fnf.mst_shop b
            on a.brd_cd = b.brd_cd
            and a.cust_cd = b.sap_shop_cd
            where a.corp_cd = '1000'
            and b.shop_shrt_nm not like '%상-위%'
            and a.chnl_cd not in ('0', '9', '8', '99')
            and b.mgmt_chnl_cd not in ('4', '5')  -- 온라인 제외
            and a.brd_cd = '{brd_cd}'
            and (a.pst_dt between '{current_date_start}' and '{current_date_end}'
                or a.pst_dt between '{previous_date_start}' and '{previous_date_end}'
            )
            group by 1,2,3,4
            having sum(act_sale_amt) > 0
        )
        group by 1,2
    ), chnl_summary as (
        select chnl_nm
            , sum(case when yymm = '{yyyymm}' then act_sale_amt else 0 end) as sale_amt_cy
            , sum(case when yymm = '{yyyymm}' then shop_cnt else 0 end) as shop_cnt_cy
            , sum(case when yymm = '{yyyymm_py}' then act_sale_amt else 0 end) as sale_amt_py
            , sum(case when yymm = '{yyyymm_py}' then shop_cnt else 0 end) as shop_cnt_py
        from raw
        group by 1
    ), total_summary as (
        select sum(case when yymm = '{yyyymm}' then act_sale_amt else 0 end) as sale_amt_cy
            , sum(case when yymm = '{yyyymm}' then shop_cnt else 0 end) as shop_cnt_cy
            , sum(case when yymm = '{yyyymm_py}' then act_sale_amt else 0 end) as sale_amt_py
            , sum(case when yymm = '{yyyymm_py}' then shop_cnt else 0 end) as shop_cnt_py
        from raw
    ), exp_notax as (
        select sum(case when yymm = '{yyyymm}' then act_sale_amt else 0 end) as sale_amt_cy
            , sum(case when yymm = '{yyyymm}' then shop_cnt else 0 end) as shop_cnt_cy
            , sum(case when yymm = '{yyyymm_py}' then act_sale_amt else 0 end) as sale_amt_py
            , sum(case when yymm = '{yyyymm_py}' then shop_cnt else 0 end) as shop_cnt_py
        from raw
        where 1=1
          and chnl_nm <> '면세점'
    ), main as (
        select '전체' as chnl_nm 
            , 1 as seq 
            , shop_cnt_cy
            , shop_cnt_py
            , round(case when shop_cnt_cy = 0 then 0 else sale_amt_cy / shop_cnt_cy end) shop_amt_cy
            , round(case when shop_cnt_py = 0 then 0 else sale_amt_py / shop_cnt_py end) shop_amt_py
            , case when shop_amt_py = 0 then 0 else round(shop_amt_cy / shop_amt_py * 100, 1) end as yoy
        from total_summary
        union all
        select chnl_nm
            , 2 as seq
            , shop_cnt_cy
            , shop_cnt_py
            , round(case when shop_cnt_cy = 0 then 0 else sale_amt_cy / shop_cnt_cy end) shop_amt_cy
            , round(case when shop_cnt_py = 0 then 0 else sale_amt_py / shop_cnt_py end) shop_amt_py
            , case when shop_amt_py = 0 then 0 else round(shop_amt_cy / shop_amt_py * 100, 1) end as yoy
        from chnl_summary
        union all
        select '면세 제외' as chnl_nm
            , 3 as seq
            , shop_cnt_cy
            , shop_cnt_py
            , round(case when shop_cnt_cy = 0 then 0 else sale_amt_cy / shop_cnt_cy end) shop_amt_cy
            , round(case when shop_cnt_py = 0 then 0 else sale_amt_py / shop_cnt_py end) shop_amt_py
            , case when shop_amt_py = 0 then 0 else round(shop_amt_cy / shop_amt_py * 100, 1) end as yoy
        from exp_notax
    ), chnl_seq as (
        select '플래그쉽' as chnl_nm, 1 as chnl_seq
        union all select '백화점' as chnl_nm, 2 as chnl_seq
        union all select '대리점' as chnl_nm, 3 as chnl_seq
        union all select '직영점' as chnl_nm, 4 as chnl_seq
        union all select '면세점' as chnl_nm, 5 as chnl_seq
    )
    select a.chnl_nm
        , a.seq
        , a.shop_cnt_cy
        , a.shop_cnt_py
        , round(a.shop_amt_cy / 1000000) as shop_amt_cy
        , round(a.shop_amt_py / 1000000) as shop_amt_py
        , a.yoy
        , b.chnl_seq
    from main as a
    left join chnl_seq  as b
      on a.chnl_nm = b.chnl_nm
    order by b.chnl_seq
    """

def analyze_store_efficiency_overall(yyyymm, brd_cd):
    """매장효율성 종합분석 (당해/전년 동월 비교) - 8-1-1-1"""
    print(f"\n{'='*60}")
    print(f"매장효율성 종합분석 시작 (8-1-1-1): {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
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
        sql = get_store_efficiency_overall_query(yyyymm, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 전체 데이터 추출 (seq = 1)
        total_record = next((r for r in records if r.get('SEQ') == 1), None)
        total_shop_amt_cy = float(total_record.get('SHOP_AMT_CY', 0)) if total_record else 0
        total_shop_amt_py = float(total_record.get('SHOP_AMT_PY', 0)) if total_record else 0
        total_shop_cnt_cy = int(total_record.get('SHOP_CNT_CY', 0)) if total_record else 0
        total_shop_cnt_py = int(total_record.get('SHOP_CNT_PY', 0)) if total_record else 0
        
        # 채널별 데이터 추출 (seq = 2)
        channel_data = [r for r in records if r.get('SEQ') == 2]
        
        # 채널별 매장효율성 데이터 정리
        channel_summary = {}
        for record in channel_data:
            chnl_nm = record.get('CHNL_NM', '기타')
            shop_amt_cy = float(record.get('SHOP_AMT_CY', 0))
            shop_amt_py = float(record.get('SHOP_AMT_PY', 0))
            shop_cnt_cy = int(record.get('SHOP_CNT_CY', 0))
            shop_cnt_py = int(record.get('SHOP_CNT_PY', 0))
            yoy = float(record.get('YOY', 0))
            
            channel_summary[chnl_nm] = {
                'shop_amt_cy': round(shop_amt_cy, 2),
                'shop_amt_py': round(shop_amt_py, 2),
                'shop_cnt_cy': shop_cnt_cy,
                'shop_cnt_py': shop_cnt_py,
                'yoy': round(yoy, 1)
            }
        
        # 우수 점포 생산성 (점포당 매출이 평균보다 높고 전년대비 개선)
        excellent_channels = [
            {
                'chnl_nm': chnl,
                'shop_amt_cy': data['shop_amt_cy'],
                'shop_amt_py': data['shop_amt_py'],
                'yoy': data['yoy'],
                'shop_cnt_cy': data['shop_cnt_cy']
            }
            for chnl, data in channel_summary.items()
            if data['shop_amt_cy'] > total_shop_amt_cy and data['yoy'] > 100  # 평균보다 높고 개선됨
        ]
        excellent_channels.sort(key=lambda x: (x['shop_amt_cy'], x['yoy']), reverse=True)
        
        # 대응 필요 매장 (점포당 매출이 평균보다 낮거나 악화)
        warning_channels = [
            {
                'chnl_nm': chnl,
                'shop_amt_cy': data['shop_amt_cy'],
                'shop_amt_py': data['shop_amt_py'],
                'yoy': data['yoy'],
                'shop_cnt_cy': data['shop_cnt_cy']
            }
            for chnl, data in channel_summary.items()
            if data['shop_amt_cy'] < total_shop_amt_cy or data['yoy'] < 100  # 평균보다 낮거나 악화됨
        ]
        warning_channels.sort(key=lambda x: (x['shop_amt_cy'], x['yoy']))
        
        unique_channels = len(channel_summary)
        
        print(f"전년 점포당 매출: {total_shop_amt_py:.2f}백만원 ({total_shop_cnt_py}개 점포)")
        print(f"당해 점포당 매출: {total_shop_amt_cy:.2f}백만원 ({total_shop_cnt_cy}개 점포)")
        print(f"전년대비 변화: {round(total_shop_amt_cy / total_shop_amt_py * 100 if total_shop_amt_py > 0 else 0, 1)}%")
        print(f"채널 수: {unique_channels}개")
        
        # LLM 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 매장 운영 전문가야. 당해와 전년 동월의 매장 효율성(점포당 매출)을 비교 분석하여 우수 점포와 개선이 필요한 매장을 식별하고 최적화 방안을 제시해야 해.

**분석 기간**
- 당해: {current_year}년 {current_month}월 ({yyyymm})
- 전년: {previous_year}년 {current_month}월 ({yyyymm_py})

**전체 요약**
- 전년 점포당 매출: {total_shop_amt_py:.2f}백만원 ({total_shop_cnt_py}개 점포)
- 당해 점포당 매출: {total_shop_amt_cy:.2f}백만원 ({total_shop_cnt_cy}개 점포)
- 전년대비 변화: {round(total_shop_amt_cy / total_shop_amt_py * 100 if total_shop_amt_py > 0 else 0, 1)}%
- 분석 채널 수: {unique_channels}개

**채널별 매장효율성 데이터**
{json_dumps_safe(channel_summary, ensure_ascii=False, indent=2)}

**우수 점포 생산성 (점포당 매출 높고 개선)**
{json_dumps_safe(excellent_channels[:5], ensure_ascii=False, indent=2)}

**대응 필요 매장 (점포당 매출 낮거나 악화)**
{json_dumps_safe(warning_channels[:5], ensure_ascii=False, indent=2)}

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 매장 효율성 종합분석을 통해:
1. 우수 점포 생산성: 점포당 매출이 높고 전년대비 개선된 채널 식별 및 성공 요인 분석
2. 대응 필요 매장: 점포당 매출이 낮거나 악화된 채널 식별 및 개선 방안 제시
3. AI 권장사항: 매장 효율성 향상을 위한 구체적이고 실행 가능한 전략 제시
4. 최적의 시나리오: 이상적인 매장 운영 시나리오와 목표 설정

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

{{
  "title": "매장효율성 종합분석",
  "sections": [
    {{
      "div": "overall-1",
      "sub_title": "우수 점포 생산성",
      "ai_text": "점포당 매출이 높고 전년대비 개선된 채널들을 4줄 이하로 간략하게 요약해줘. 각 채널의 점포당 매출 수치와 개선 정도를 포함하여 구체적으로 작성하세요."
    }},
    {{
      "div": "overall-2",
      "sub_title": "대응 필요 매장",
      "ai_text": "점포당 매출이 낮거나 악화된 채널들을 4줄 이하로 간략하게 요약해줘. 각 채널의 점포당 매출 수치와 악화 정도를 포함하여 구체적으로 작성하세요."
    }},
    {{
      "div": "overall-3",
      "sub_title": "AI 권장사항",
      "ai_text": "매장 효율성 향상을 위한 구체적이고 실행 가능한 전략을 4줄 이하로 간략하게 제시해줘. 채널별 특성을 고려한 실용적인 권장사항을 작성하세요."
    }},
    {{
      "div": "overall-4",
      "sub_title": "최적의 시나리오",
      "ai_text": "이상적인 매장 운영 시나리오와 목표 설정을 4줄 이하로 간략하게 제시해줘. 구체적인 수치와 실행 방안을 포함하여 작성하세요."
    }}
  ]
}}

<작성 가이드라인>
- 각 섹션의 ai_text는 반드시 4줄을 넘지 않도록 간략하게 작성
- 숫자는 점포당 매출(백만원)로 표시하고 절대 변형하지 말 것
- 채널별 점포당 매출 수치와 전년대비 변화를 구체적으로 언급
- 우수 점포의 성공 요인 분석
- 대응 필요 매장의 개선 방안 제시
- AI 권장사항은 실행 가능한 구체적 전략으로 작성
- 최적의 시나리오는 현실적이고 달성 가능한 목표로 제시
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱 (마크다운 코드 블록 제거)
        analysis_response = analysis_response.strip()
        if analysis_response.startswith('```json'):
            analysis_response = analysis_response[7:]
        if analysis_response.startswith('```'):
            analysis_response = analysis_response[3:]
        if analysis_response.endswith('```'):
            analysis_response = analysis_response[:-3]
        analysis_response = analysis_response.strip()
        
        try:
            analysis_data = json.loads(analysis_response)
            # sections에 div 필드 추가 (overall-1, overall-2, overall-3, overall-4)
            for idx, section in enumerate(analysis_data.get('sections', []), 1):
                if 'div' not in section:
                    section['div'] = f'overall-{idx}'
        except json.JSONDecodeError as e:
            print(f"[WARNING] JSON 파싱 실패: {e}")
            print(f"[WARNING] 응답 내용: {analysis_response[:500]}")
            # 기본 구조로 대체
            analysis_data = {
                "title": "매장효율성 종합분석",
                "sections": [
                    {"div": "overall-1", "sub_title": "우수 점포 생산성", "ai_text": analysis_response},
                    {"div": "overall-2", "sub_title": "대응 필요 매장", "ai_text": ""},
                    {"div": "overall-3", "sub_title": "AI 권장사항", "ai_text": ""},
                    {"div": "overall-4", "sub_title": "최적의 시나리오", "ai_text": ""}
                ]
            }
        
        # JSON 데이터 생성
        json_data = {
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm,
            'yyyymm_py': yyyymm_py,
            'analysis_data': analysis_data,
            'summary': {
                'total_shop_amt_cy': round(total_shop_amt_cy, 2),
                'total_shop_amt_py': round(total_shop_amt_py, 2),
                'total_shop_cnt_cy': total_shop_cnt_cy,
                'total_shop_cnt_py': total_shop_cnt_py,
                'yoy': round(total_shop_amt_cy / total_shop_amt_py * 100 if total_shop_amt_py > 0 else 0, 1),
                'unique_channels': unique_channels,
                'analysis_period': f"{previous_year}년 {current_month}월 vs {current_year}년 {current_month}월"
            },
            'channel_summary': channel_summary,
            'excellent_channels': excellent_channels[:5],
            'warning_channels': warning_channels[:5],
            'raw_data': {
                'sample_records': [
                    {
                        'CHNL_NM': r.get('CHNL_NM', ''),
                        'SHOP_CNT_CY': int(r.get('SHOP_CNT_CY', 0)),
                        'SHOP_CNT_PY': int(r.get('SHOP_CNT_PY', 0)),
                        'SHOP_AMT_CY': float(r.get('SHOP_AMT_CY', 0)),
                        'SHOP_AMT_PY': float(r.get('SHOP_AMT_PY', 0)),
                        'YOY': float(r.get('YOY', 0)),
                        'SEQ': int(r.get('SEQ', 0))
                    }
                    for r in records[:100]
                ],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        filename = f"8-1-1-1.{brd_cd}_매장효율성_종합분석"
        save_json(json_data, filename)
        
        # Markdown도 저장 (analysis_data의 sections를 조합)
        markdown_content = f"# {analysis_data.get('title', '매장효율성 종합분석')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def get_item_sales_overall_query(yyyymm, brd_cd):
    """아이템별 매출 종합분석 쿼리 (당해 1월~현재월)"""
    # 분석 기간 계산
    current_year = int(yyyymm[:4])
    current_month = int(yyyymm[4:6])
    previous_year = current_year - 1
    
    year_start = f"{current_year}01"  # 당해 1월
    year_start_py = f"{previous_year}01"  # 전년 1월
    yyyymm_py = f"{previous_year}{current_month:02d}"  # 전년 동월
    
    # 현재 날짜 문자열 (시즌 계산용)
    current_date_str = f"{current_year}-{current_month:02d}-01"
    
    # -6개월 계산
    current_date = datetime(current_year, current_month, 1)
    minus6_date = current_date - timedelta(days=180)  # 약 6개월
    minus6_yyyymm = minus6_date.strftime('%Y%m')
    minus18_date = current_date - timedelta(days=540)  # 약 18개월
    minus18_yyyymm = minus18_date.strftime('%Y%m')
    
    return f"""
    WITH cy_item as (
        select a.prdt_cd
            , a.sesn
            , a.prdt_hrrc1_nm
            , a.prdt_hrrc2_nm
            , a.prdt_hrrc3_nm
            , case when ('{yyyymm}' between b.start_yyyymm and b.end_yyyymm) and prdt_hrrc1_nm = '의류'
                        then decode(a.sesn, 'N', 'S', a.sesn) || ' ' || a.prdt_hrrc1_nm -- 당시즌 의류
                    when ('{minus6_yyyymm}' between b.start_yyyymm and b.end_yyyymm) and prdt_hrrc1_nm = '의류' -- -6개월
                        then decode(a.sesn, 'N', 'S', a.sesn) || ' ' || a.prdt_hrrc1_nm-- 전시즌 의류
                    when (b.start_yyyymm > '{yyyymm}') and prdt_hrrc1_nm = '의류'
                        then '차기시즌 의류'
                    when (b.start_yyyymm < '{minus6_yyyymm}') and prdt_hrrc1_nm = '의류' -- -6개월
                        then '과시즌 의류'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Headwear'
                        then '모자'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Shoes'
                        then '신발'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Bag'
                        then '가방'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Acc_etc'
                        then '기타ACC'
                    else '기타' end as item_std
        from sap_fnf.mst_prdt a
        left join comm.mst_sesn b
            on a.sesn = b.sesn
        where 1=1
            and brd_cd = '{brd_cd}'
    )
    -- py_item : 전년 아이템 구분 기준
    , py_item as (
        select a.prdt_cd
            , a.sesn
            , a.prdt_hrrc1_nm
            , a.prdt_hrrc2_nm
            , a.prdt_hrrc3_nm
            , case when ('{yyyymm_py}' between b.start_yyyymm and b.end_yyyymm) and prdt_hrrc1_nm = '의류'
                        then (left(a.sesn,2)+1)::int || decode(right(a.sesn,1), 'N', 'S', right(a.sesn,1)) || ' ' || a.prdt_hrrc1_nm -- 당시즌 의류
                    when ('{minus18_yyyymm}' between b.start_yyyymm and b.end_yyyymm) and prdt_hrrc1_nm = '의류'   -- -18개월
                        then (left(a.sesn,2)+1)::int || decode(right(a.sesn,1), 'N', 'S', right(a.sesn,1)) || ' ' || a.prdt_hrrc1_nm-- 전시즌 의류
                    when (b.start_yyyymm > '{yyyymm_py}') and prdt_hrrc1_nm = '의류'
                        then '차기시즌 의류'
                    when (b.start_yyyymm < '{minus18_yyyymm}') and prdt_hrrc1_nm = '의류' -- -18개월
                        then '과시즌 의류'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Headwear'
                        then '모자'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Shoes'
                        then '신발'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Bag'
                        then '가방'
                    when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Acc_etc'
                        then '기타ACC'
                    else '기타' end as item_std
        from sap_fnf.mst_prdt a
        left join comm.mst_sesn b
            on a.sesn = b.sesn
        where 1=1
            and brd_cd = '{brd_cd}'
    ), cy_py_item_raw as (
        select 'cy' as div
            ,right(pst_yyyymm, 2) as month
            , case
                    when b.mgmt_chnl_cd = '4' then '자사몰'
                    when b.mgmt_chnl_cd = '5' then '제휴몰'
                    when b.mgmt_chnl_cd in ('3', '11', 'C3') then '직영기타'
                    when b.mgmt_chnl_cd in ('7', '12') then '아울렛'
                    else b.mgmt_chnl_nm
                end as chnl_nm
            , c.item_std
            , sum(a.act_sale_amt) act_sale_amt
        from sap_fnf.dm_pl_shop_prdt_m a
        join sap_fnf.mst_shop b
        on a.brd_cd = b.brd_cd
        and a.shop_cd = b.sap_shop_cd
        join cy_item  c
        on a.prdt_cd = c.prdt_cd
        where 1=1
        and a.brd_cd = '{brd_cd}'
        and a.corp_cd = '1000'
        and b.chnl_cd not in ('0','8', '9', '99')
        and a.pst_yyyymm between '{year_start}' and '{yyyymm}'
        group by 1, 2, 3, 4
        union all
        select 'py' as div
            ,right(pst_yyyymm, 2) as month
            , case
                    when b.mgmt_chnl_cd = '4' then '자사몰'
                    when b.mgmt_chnl_cd = '5' then '제휴몰'
                    when b.mgmt_chnl_cd in ('3', '11', 'C3') then '직영기타'
                    when b.mgmt_chnl_cd in ('7', '12') then '아울렛'
                    else b.mgmt_chnl_nm
                end as chnl_nm
            , c.item_std
            , sum(a.act_sale_amt) act_sale_amt
        from sap_fnf.dm_pl_shop_prdt_m a
        join sap_fnf.mst_shop b
        on a.brd_cd = b.brd_cd
        and a.shop_cd = b.sap_shop_cd
        join py_item  c
        on a.prdt_cd = c.prdt_cd
        where 1=1
        and a.brd_cd = '{brd_cd}'
        and a.corp_cd = '1000'
        and b.chnl_cd not in ('0','8', '9', '99')
        and a.pst_yyyymm between '{year_start_py}' and '{yyyymm_py}'
        group by 1, 2, 3, 4
    ), raw as (
        select month
             , chnl_nm
             , item_std
             , sum(case when div = 'cy' then act_sale_amt else 0 end) act_sale_amt_cy
             , sum(case when div = 'py' then act_sale_amt else 0 end) act_sale_amt_py
        from cy_py_item_raw
        where item_std not in ('기타')
        group by 1, 2, 3
    ) , class_summary as (
        select month
            , item_std
            , sum(act_sale_amt_cy) over(partition by month) as sale_ttl
            , act_sale_amt_cy
            , case when sale_ttl = 0 then 0 else round(act_sale_amt_cy / sale_ttl * 100) end as ratio
            , case when act_sale_amt_py = 0 then 0 else round(act_sale_amt_cy / act_sale_amt_py * 100) end yoy
        from (
            select month
                , item_std
                , sum(act_sale_amt_cy) act_sale_amt_cy
                , sum(act_sale_amt_py) act_sale_amt_py
            from raw
            group by 1,2
        )
    ), chnl_summary as (
        select month
            , chnl_nm
            , sum(act_sale_amt_cy) over(partition by month) as sale_ttl
            , act_sale_amt_cy
            , case when sale_ttl = 0 then 0 else round(act_sale_amt_cy / sale_ttl * 100) end as ratio
            , case when act_sale_amt_py = 0 then 0 else round(act_sale_amt_cy / act_sale_amt_py * 100) end yoy
        from (
            select month
                    , chnl_nm
                    , sum(act_sale_amt_cy) act_sale_amt_cy
                    , sum(act_sale_amt_py) act_sale_amt_py
            from raw
            group by 1,2
        )
    ), total_summary as (
        select month
            , act_sale_amt_cy
            , case when act_sale_amt_py = 0 then 0 else round(act_sale_amt_cy / act_sale_amt_py * 100) end as yoy
        from (
            select month
                , sum(act_sale_amt_cy) act_sale_amt_cy
                , sum(act_sale_amt_py) act_sale_amt_py
            from raw
            group by 1
        )
    ), main as (
        select '전체' chnl_nm
            , month
            , round(act_sale_amt_cy / 1000000) as sale_amt
            , 100 ratio
            , yoy
            , 1 seq
        from total_summary
        union all
        select chnl_nm
            , month
            , round(act_sale_amt_cy / 1000000) as sale_amt
            , ratio
            , yoy
            , 2 seq
        from chnl_summary
        union all
        select item_std
            , month
            , round(act_sale_amt_cy / 1000000) as sale_amt
            , ratio
            , yoy
            , 3 seq
        from class_summary
    ), chnl_seq as (
        select '플래그쉽' as chnl_nm, 1 as chnl_seq
        union all select '백화점' as chnl_nm, 2 as chnl_seq
        union all select '대리점' as chnl_nm, 3 as chnl_seq
        union all select '직영기타' as chnl_nm, 4 as chnl_seq
        union all select '제휴몰' as chnl_nm, 5 as chnl_seq
        union all select '자사몰' as chnl_nm, 6 as chnl_seq
        union all select '면세점' as chnl_nm, 7 as chnl_seq
        union all select 'RF' as chnl_nm, 8 as chnl_seq
        union all select '아울렛' as chnl_nm, 9 as chnl_seq
        union all select case when to_char('{current_date_str}'::date, 'MM') between '03' and '08' then to_char('{current_date_str}'::date, 'YY') || 'S'
                            when to_char('{current_date_str}'::date, 'MM') between '09' and '12' then to_char('{current_date_str}'::date, 'YY') || 'F'
                            else (to_char('{current_date_str}'::date, 'YY') -1)::float || 'F'  end || ' 의류' as chnl_nm, 101 as chnl_seq
        union all select case when to_char('{current_date_str}'::date, 'MM') between '03' and '08' then (to_char('{current_date_str}'::date, 'YY')-1)::float || 'F'
                            when to_char('{current_date_str}'::date, 'MM') between '09' and '12' then to_char('{current_date_str}'::date, 'YY') || 'S'
                            else (to_char('{current_date_str}'::date, 'YY') -1)::float || 'S'  end || ' 의류' as chnl_nm, 102 as chnl_seq
        union all select '과시즌 의류' as chnl_nm, 103 as chnl_seq
        union all select '모자' as chnl_nm, 201 as chnl_seq
        union all select '신발' as chnl_nm, 202 as chnl_seq
        union all select '가방' as chnl_nm, 203 as chnl_seq
        union all select '기타ACC' as chnl_nm, 204 as chnl_seq
    )
    select month
        , a.chnl_nm
        , a.sale_amt
        , a.ratio
        , a.yoy
        , a.seq
        , b.chnl_seq
    from main a
    left join chnl_seq b
    on a.chnl_nm = b.chnl_nm
    order by month, a.seq, b.chnl_seq
    """

def analyze_item_sales_overall(yyyymm, brd_cd):
    """아이템별 매출 종합분석 (당해 1월~현재월) - 15-1-1-1"""
    print(f"\n{'='*60}")
    print(f"아이템별 매출 종합분석 시작 (15-1-1-1): {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (당해 1월부터 현재월까지)
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        
        yyyymm_start = f"{current_year}01"  # 당해 1월
        yyyymm_end = yyyymm  # 현재월
        
        print(f"분석 기간: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월")
        
        # SQL 쿼리 실행
        sql = get_item_sales_overall_query(yyyymm, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_sales = sum(float(r.get('SALE_AMT', 0)) * 1000000 for r in records if r.get('SEQ') == 1)  # 전체만 합산
        unique_months = len(set(r.get('MONTH', '') for r in records))
        
        print(f"총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)")
        print(f"분석 월 수: {unique_months}개월")
        
        # 아이템별 데이터 정리 (seq=3인 것만)
        item_data = {}
        season_items = []  # F시즌, S시즌, 과시즌 의류
        category_items = []  # 모자, 신발, 가방, 기타ACC
        
        for record in records:
            if record.get('SEQ') != 3:  # 아이템 데이터만
                continue
            
            item_std = record.get('CHNL_NM', '')  # 실제로는 item_std
            month = record.get('MONTH', '')
            sale_amt = float(record.get('SALE_AMT', 0)) * 1000000  # 백만원 -> 원
            ratio = float(record.get('RATIO', 0))
            yoy = float(record.get('YOY', 0))
            
            if item_std not in item_data:
                item_data[item_std] = {
                    'total_sales': 0,
                    'months': {},
                    'total_ratio': 0,
                    'avg_yoy': 0
                }
            
            item_data[item_std]['total_sales'] += sale_amt
            if month not in item_data[item_std]['months']:
                item_data[item_std]['months'][month] = {
                    'sale_amt': 0,
                    'ratio': 0,
                    'yoy': 0
                }
            item_data[item_std]['months'][month]['sale_amt'] += sale_amt
            item_data[item_std]['months'][month]['ratio'] = ratio
            item_data[item_std]['months'][month]['yoy'] = yoy
        
        # 시즌별 아이템 분류
        for item_std, data in item_data.items():
            if '의류' in item_std:
                if 'F' in item_std or 'F시즌' in item_std:
                    season_items.append({
                        'name': item_std,
                        'total_sales': round(data['total_sales'] / 1000000, 2),
                        'months': {k: round(v['sale_amt'] / 1000000, 2) for k, v in data['months'].items()},
                        'avg_ratio': sum(v['ratio'] for v in data['months'].values()) / len(data['months']) if data['months'] else 0,
                        'avg_yoy': sum(v['yoy'] for v in data['months'].values()) / len(data['months']) if data['months'] else 0
                    })
                elif 'S' in item_std or 'S시즌' in item_std:
                    season_items.append({
                        'name': item_std,
                        'total_sales': round(data['total_sales'] / 1000000, 2),
                        'months': {k: round(v['sale_amt'] / 1000000, 2) for k, v in data['months'].items()},
                        'avg_ratio': sum(v['ratio'] for v in data['months'].values()) / len(data['months']) if data['months'] else 0,
                        'avg_yoy': sum(v['yoy'] for v in data['months'].values()) / len(data['months']) if data['months'] else 0
                    })
                elif '과시즌' in item_std:
                    season_items.append({
                        'name': item_std,
                        'total_sales': round(data['total_sales'] / 1000000, 2),
                        'months': {k: round(v['sale_amt'] / 1000000, 2) for k, v in data['months'].items()},
                        'avg_ratio': sum(v['ratio'] for v in data['months'].values()) / len(data['months']) if data['months'] else 0,
                        'avg_yoy': sum(v['yoy'] for v in data['months'].values()) / len(data['months']) if data['months'] else 0
                    })
            elif item_std in ['모자', '신발', '가방', '기타ACC']:
                category_items.append({
                    'name': item_std,
                    'total_sales': round(data['total_sales'] / 1000000, 2),
                    'months': {k: round(v['sale_amt'] / 1000000, 2) for k, v in data['months'].items()},
                    'avg_ratio': sum(v['ratio'] for v in data['months'].values()) / len(data['months']) if data['months'] else 0,
                    'avg_yoy': sum(v['yoy'] for v in data['months'].values()) / len(data['months']) if data['months'] else 0
                })
        
        # 시즌별 정렬 (총 매출 기준)
        season_items.sort(key=lambda x: x['total_sales'], reverse=True)
        category_items.sort(key=lambda x: x['total_sales'], reverse=True)
        
        print(f"시즌 아이템 수: {len(season_items)}개")
        print(f"카테고리 아이템 수: {len(category_items)}개")
        
        # LLM 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 아이템 전략 전문가야. 당해 1월부터 현재월까지의 아이템별 매출 추이를 분석하여 시즌 트렌드와 카테고리별 성과를 분석하고 판매율 향상을 위한 전략을 제시해야 해.

**분석 기간**
- 시작: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월
- 종료: {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월
- 기간: {unique_months}개월

**전체 요약**
- 총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)

**시즌별 트렌드 데이터 (F시즌, S시즌, 과시즌 의류)**
{json_dumps_safe(season_items, ensure_ascii=False, indent=2)}

**카테고리별 데이터 (모자, 신발, 가방, 기타ACC)**
{json_dumps_safe(category_items, ensure_ascii=False, indent=2)}

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 당해 1월부터 현재월까지 아이템별 매출 추이를 분석하여:
1. 시즌별(F시즌, S시즌, 과시즌 의류) 트렌드와 성과 분석
2. 카테고리별(모자, 신발, 가방, 기타ACC) 성과와 성장 패턴 파악
3. 판매율을 높이기 위한 구체적이고 실행 가능한 전략 3가지 제시

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

{{
  "title": "아이템별 매출 종합분석 (당해 1월~현재월)",
  "sections": [
    {{
      "div": "overall-1",
      "sub_title": "시즌 트렌드",
      "ai_text": "[F시즌, S시즌, 과시즌 의류의 매출 추이와 성과를 분석하여 시즌별 트렌드를 3줄로 요약]"
    }},
    {{
      "div": "overall-2",
      "sub_title": "카테고리",
      "ai_text": "[모자, 신발, 가방, 기타ACC의 매출 추이와 성과를 분석하여 카테고리별 트렌드를 3줄로 요약]"
    }},
    {{
      "div": "overall-3",
      "sub_title": "핵심액션",
      "ai_text": "[판매율을 높이기 위해 어떤 전략을 해야하는지 구체적이고 실행 가능한 전략을 3줄로 제시]"
    }}
  ]
}}

<작성 가이드라인>
- **"시즌 트렌드" 섹션은 반드시 아래 형식으로 작성해야 합니다:**
  • [F시즌 의류 분석 결과]
  • [S시즌 의류 분석 결과]
  • [과시즌 의류 분석 결과]
  위 "시즌별 트렌드 데이터"를 바탕으로 각 시즌의 매출 추이, 비중, 전년 대비 변화 등을 분석하세요. 각 시즌별로 1줄씩 총 3줄로 작성하세요.

- **"카테고리" 섹션은 반드시 아래 형식으로 작성해야 합니다:**
  • [모자 분석 결과]
  • [신발 분석 결과]
  • [가방 또는 기타ACC 분석 결과]
  위 "카테고리별 데이터"를 바탕으로 각 카테고리의 매출 추이, 비중, 전년 대비 변화 등을 분석하세요. 주요 카테고리 3개를 선택하여 각각 1줄씩 총 3줄로 작성하세요.

- **"핵심액션" 섹션은 반드시 아래 형식으로 3줄로 작성해야 합니다:**
  • [구체적인 전략 제안 1]
  • [구체적인 전략 제안 2]
  • [구체적인 전략 제안 3]
  위 데이터 분석 결과를 바탕으로 판매율을 높이기 위한 구체적이고 실행 가능한 전략을 제시하세요. 시즌별 트렌드와 카테고리별 성과를 고려하여 실용적인 전략을 제안하세요. 추가 설명 없이 3줄만 작성하세요.

- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 백만원 단위로 표시하고 절대 변형하지 말 것
- 시즌별 구매 패턴과 성장 추세 분석
- 카테고리별 핵심 아이템 식별
- 전년대비 변화에 대한 구체적 원인과 효과 분석
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시 (예: "첫 번째 줄\\n두 번째 줄")
- 불릿 포인트나 리스트 항목 사이에는 \\n을 사용
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱 (마크다운 코드 블록 제거)
        analysis_response = analysis_response.strip()
        if analysis_response.startswith('```json'):
            analysis_response = analysis_response[7:]
        if analysis_response.startswith('```'):
            analysis_response = analysis_response[3:]
        if analysis_response.endswith('```'):
            analysis_response = analysis_response[:-3]
        analysis_response = analysis_response.strip()
        
        try:
            analysis_data = json.loads(analysis_response)
        except json.JSONDecodeError as e:
            print(f"[WARNING] JSON 파싱 실패: {e}")
            print(f"[WARNING] 응답 내용: {analysis_response[:500]}")
            # 기본 구조로 대체
            analysis_data = {
                "title": "아이템별 매출 종합분석 (당해 1월~현재월)",
                "sections": [
                    {"div": "overall-1", "sub_title": "시즌 트렌드", "ai_text": analysis_response},
                    {"div": "overall-2", "sub_title": "카테고리", "ai_text": "데이터 분석 중"},
                    {"div": "overall-3", "sub_title": "핵심액션", "ai_text": "데이터 분석 중"}
                ]
            }
        
        # JSON 데이터 생성
        # yyyymm_py 계산 (전년 동월)
        previous_year = int(yyyymm_end[:4]) - 1
        yyyymm_py = f"{previous_year}{yyyymm_end[4:6]}"
        
        json_data = {
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm_end,
            'yyyymm_py': yyyymm_py,
            'analysis_data': analysis_data,
            'summary': {
                'total_sales': round(total_sales / 1000000, 2),
                'unique_months': unique_months,
                'analysis_period': f"{yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월"
            },
            'season_items': season_items,
            'category_items': category_items,
            'raw_data': {
                'sample_records': [
                    {
                        'MONTH': r.get('MONTH', ''),
                        'CHNL_NM': r.get('CHNL_NM', ''),
                        'SALE_AMT': float(r.get('SALE_AMT', 0)),
                        'RATIO': float(r.get('RATIO', 0)),
                        'YOY': float(r.get('YOY', 0)),
                        'SEQ': int(r.get('SEQ', 0))
                    }
                    for r in records[:100]
                ],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        filename = f"15-1-1-1.{brd_cd}_아이템_매출_종합분석(당해1월~현재월)"
        save_json(json_data, filename)
        
        # Markdown 파일 생성
        markdown_content = f"# {analysis_data.get('title', '아이템별 매출 종합분석 (당해 1월~현재월)')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

def get_item_stock_overall_query(yyyymm, brd_cd):
    """아이템별 재고 종합분석 쿼리 (당해 1월~현재월)"""
    # 분석 기간 계산
    current_year = int(yyyymm[:4])
    current_month = int(yyyymm[4:6])
    previous_year = current_year - 1
    
    year_start = f"{current_year}01"  # 당해 1월
    year_start_py = f"{previous_year}01"  # 전년 1월
    yyyymm_py = f"{previous_year}{current_month:02d}"  # 전년 동월
    
    # 현재 날짜 문자열 (시즌 계산용)
    current_date_str = f"{current_year}-{current_month:02d}-01"
    
    # -6개월 계산
    current_date = datetime(current_year, current_month, 1)
    minus6_date = current_date - timedelta(days=180)  # 약 6개월
    minus6_yyyymm = minus6_date.strftime('%Y%m')
    minus18_date = current_date - timedelta(days=540)  # 약 18개월
    minus18_yyyymm = minus18_date.strftime('%Y%m')
    
    return f"""
    WITH cy_item as (
        select a.prdt_cd  
                , a.sesn
                , a.prdt_hrrc1_nm
                , a.prdt_hrrc2_nm
                , a.prdt_hrrc3_nm
                , case when ('{yyyymm}' between b.start_yyyymm and b.end_yyyymm) and prdt_hrrc1_nm = '의류' 
                            then decode(a.sesn, 'N', 'S', a.sesn) || ' ' || a.prdt_hrrc1_nm -- 당시즌 의류
                        when ('{minus6_yyyymm}' between b.start_yyyymm and b.end_yyyymm) and prdt_hrrc1_nm = '의류' -- -6개월
                            then decode(a.sesn, 'N', 'S', a.sesn) || ' ' || a.prdt_hrrc1_nm-- 전시즌 의류
                        when (b.start_yyyymm > '{yyyymm}') and prdt_hrrc1_nm = '의류' 
                            then '차기시즌 의류'
                        when (b.start_yyyymm < '{minus6_yyyymm}') and prdt_hrrc1_nm = '의류' -- -6개월
                            then '과시즌 의류'
                        when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Headwear' 
                            then '모자'
                        when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Shoes' 
                            then '신발'
                        when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Bag' 
                            then '가방'
                        when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Acc_etc' 
                            then '기타ACC'
                        else '기타' end as item_std
        from sap_fnf.mst_prdt a
        left join comm.mst_sesn b
            on a.sesn = b.sesn
        where 1=1
            and brd_cd = '{brd_cd}'
    )
    -- py_item : 전년 아이템 구분 기준
    , py_item as (
        select a.prdt_cd  
                , a.sesn
                , a.prdt_hrrc1_nm
                , a.prdt_hrrc2_nm
                , a.prdt_hrrc3_nm
                , case when ('{yyyymm_py}' between b.start_yyyymm and b.end_yyyymm) and prdt_hrrc1_nm = '의류' 
                            then (left(a.sesn,2)+1)::int || decode(right(a.sesn,1), 'N', 'S', right(a.sesn,1)) || ' ' || a.prdt_hrrc1_nm -- 당시즌 의류
                        when ('{minus18_yyyymm}' between b.start_yyyymm and b.end_yyyymm) and prdt_hrrc1_nm = '의류'   -- -18개월
                            then (left(a.sesn,2)+1)::int || decode(right(a.sesn,1), 'N', 'S', right(a.sesn,1)) || ' ' || a.prdt_hrrc1_nm-- 전시즌 의류
                        when (b.start_yyyymm > '{yyyymm_py}') and prdt_hrrc1_nm = '의류' 
                            then '차기시즌 의류'
                        when (b.start_yyyymm < '{minus18_yyyymm}') and prdt_hrrc1_nm = '의류' -- -18개월
                            then '과시즌 의류'
                        when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Headwear' 
                            then '모자'
                        when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Shoes' 
                            then '신발'
                        when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Bag' 
                            then '가방'
                        when prdt_hrrc1_nm='ACC' and prdt_hrrc2_nm='Acc_etc' 
                            then '기타ACC'
                        else '기타' end as item_std
        from sap_fnf.mst_prdt a
        left join comm.mst_sesn b
            on a.sesn = b.sesn
        where 1=1
            and brd_cd = '{brd_cd}'
    )
    -- base: 필요한 데이터
    , base as (
        -- 당해
        select 'cy' as div
            , a.yyyymm as yyyymm
            , b.item_std as item_std
            , sum(end_stock_tag_amt) as end_stock_tag_amt
        from sap_fnf.dw_ivtr_shop_prdt_m a
        left join cy_item b
        on a.prdt_cd = b.prdt_cd
        where 1=1 
        and a.brd_cd = '{brd_cd}'
        and a.yyyymm between '{year_start}' and '{yyyymm}'
        group by a.yyyymm, b.item_std
        -- 전년
        union all
        select 'py' as div
            , a.yyyymm as yyyymm
            , b.item_std as item_std
            , sum(end_stock_tag_amt) as end_stock_tag_amt
        from sap_fnf.dw_ivtr_shop_prdt_m a
        left join py_item b
        on a.prdt_cd = b.prdt_cd
        where 1=1 
        and a.brd_cd = '{brd_cd}'
        and a.yyyymm between '{year_start_py}' and '{yyyymm_py}'
        group by a.yyyymm, b.item_std
    )
    select yyyymm
            , item_std
            , sum(case when div='cy' then end_stock_tag_amt else 0 end) as cy_end_stock_tag_amt
            , sum(case when div='py' then end_stock_tag_amt else 0 end) as py_end_stock_tag_amt
            , round( sum(case when div='cy' then end_stock_tag_amt else 0 end)
                / nullif(sum(case when div='py' then end_stock_tag_amt else 0 end), 0)*100
                , 1) as yoy
    from base
    where item_std is not null
    and item_std != '기타'
    group by yyyymm, item_std
    order by yyyymm, item_std
    """

def analyze_item_stock_overall(yyyymm, brd_cd):
    """아이템별 재고 종합분석 (당해 1월~현재월) - 16-1-1-1"""
    print(f"\n{'='*60}")
    print(f"아이템별 재고 종합분석 시작 (16-1-1-1): {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # DB 연결
    engine = get_db_engine()
    
    try:
        # 분석 기간 계산 (당해 1월부터 현재월까지)
        current_year = int(yyyymm[:4])
        current_month = int(yyyymm[4:6])
        
        yyyymm_start = f"{current_year}01"  # 당해 1월
        yyyymm_end = yyyymm  # 현재월
        
        print(f"분석 기간: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월")
        
        # SQL 쿼리 실행
        sql = get_item_stock_overall_query(yyyymm, brd_cd)
        df = run_query(sql, engine)
        records = df.to_dicts()
        
        if not records:
            print("데이터가 없습니다.")
            return None
        
        # 데이터 요약
        total_stock = sum(float(r.get('CY_END_STOCK_TAG_AMT', 0)) for r in records)
        unique_months = len(set(r.get('YYYYMM', '') for r in records))
        unique_items = len(set(r.get('ITEM_STD', '') for r in records))
        
        print(f"총 재고액: {total_stock:,.0f}원 ({total_stock/1000000:.2f}백만원)")
        print(f"분석 월 수: {unique_months}개월")
        print(f"아이템 수: {unique_items}개")
        
        # 월별/아이템별 데이터 정리
        monthly_data = {}  # 월별 총 재고
        item_data = {}  # 아이템별 재고 추이
        
        for record in records:
            yyyymm_val = record.get('YYYYMM', '')
            item_std = record.get('ITEM_STD', '')
            cy_stock = float(record.get('CY_END_STOCK_TAG_AMT') or 0)
            py_stock = float(record.get('PY_END_STOCK_TAG_AMT') or 0)
            yoy = float(record.get('YOY') or 0)
            
            # 월별 총 재고
            if yyyymm_val not in monthly_data:
                monthly_data[yyyymm_val] = {
                    'total_stock': 0,
                    'items': {}
                }
            monthly_data[yyyymm_val]['total_stock'] += cy_stock
            
            # 아이템별 데이터
            if item_std not in item_data:
                item_data[item_std] = {
                    'total_stock': 0,
                    'months': {},
                    'max_stock': 0,
                    'max_month': None,
                    'min_stock': float('inf'),
                    'min_month': None,
                    'first_stock': None,
                    'last_stock': None
                }
            
            item_data[item_std]['total_stock'] += cy_stock
            item_data[item_std]['months'][yyyymm_val] = {
                'stock': cy_stock,
                'py_stock': py_stock,
                'yoy': yoy
            }
            
            # 최대/최소 재고 추적
            if cy_stock > item_data[item_std]['max_stock']:
                item_data[item_std]['max_stock'] = cy_stock
                item_data[item_std]['max_month'] = yyyymm_val
            if cy_stock < item_data[item_std]['min_stock']:
                item_data[item_std]['min_stock'] = cy_stock
                item_data[item_std]['min_month'] = yyyymm_val
            
            # 첫 월/마지막 월 재고
            if item_data[item_std]['first_stock'] is None:
                item_data[item_std]['first_stock'] = cy_stock
            item_data[item_std]['last_stock'] = cy_stock
        
        # 월별 총 재고 리스트 (정렬)
        monthly_totals_list = []
        for month in sorted(monthly_data.keys()):
            monthly_totals_list.append({
                'yyyymm': month,
                'total_stock': round(monthly_data[month]['total_stock'] / 1000000, 2)
            })
        
        # 조기경보 분석 (재고 증가, 최대 재고액, 수치 악화)
        early_warning_items = []
        for item_std, data in item_data.items():
            if len(data['months']) < 2:
                continue
            
            # 1월 대비 증가율 계산
            first_month = min(data['months'].keys())
            last_month = max(data['months'].keys())
            first_stock = data['months'][first_month]['stock']
            last_stock = data['months'][last_month]['stock']
            
            if first_stock > 0:
                change_pct = ((last_stock - first_stock) / first_stock) * 100
            else:
                change_pct = 0
            
            # 재고 증가하고 최대 재고액이 큰 아이템
            if change_pct > 0 and data['max_stock'] > 0:
                early_warning_items.append({
                    'item_std': item_std,
                    'max_stock': round(data['max_stock'] / 1000000, 2),
                    'max_month': data['max_month'],
                    'change_pct': round(change_pct, 1),
                    'first_stock': round(first_stock / 1000000, 2),
                    'last_stock': round(last_stock / 1000000, 2)
                })
        
        # 조기경보 정렬 (최대 재고액 기준)
        early_warning_items.sort(key=lambda x: x['max_stock'], reverse=True)
        
        # 긍정신호 분석 (재고 감소)
        positive_signal_items = []
        for item_std, data in item_data.items():
            if len(data['months']) < 2:
                continue
            
            first_month = min(data['months'].keys())
            last_month = max(data['months'].keys())
            first_stock = data['months'][first_month]['stock']
            last_stock = data['months'][last_month]['stock']
            
            if first_stock > 0:
                change_pct = ((last_stock - first_stock) / first_stock) * 100
            else:
                change_pct = 0
            
            # 재고 감소한 아이템
            if change_pct < 0:
                positive_signal_items.append({
                    'item_std': item_std,
                    'first_stock': round(first_stock / 1000000, 2),
                    'last_stock': round(last_stock / 1000000, 2),
                    'change_pct': round(change_pct, 1),
                    'reduction': round((first_stock - last_stock) / 1000000, 2)
                })
        
        # 긍정신호 정렬 (감소율 기준)
        positive_signal_items.sort(key=lambda x: x['change_pct'])
        
        # 인사이트 분석 (총 재고액, 재고 감소/증가 월)
        # 최종 월 총 재고액
        final_month_total = monthly_totals_list[-1]['total_stock'] if monthly_totals_list else 0
        
        # 재고가 감소한 월 찾기
        decreasing_months = []
        increasing_months = []
        for i in range(1, len(monthly_totals_list)):
            prev_stock = monthly_totals_list[i-1]['total_stock']
            curr_stock = monthly_totals_list[i]['total_stock']
            month = monthly_totals_list[i]['yyyymm']
            
            if curr_stock < prev_stock:
                decreasing_months.append({
                    'month': month,
                    'stock': curr_stock,
                    'change': round(curr_stock - prev_stock, 2)
                })
            elif curr_stock > prev_stock:
                increasing_months.append({
                    'month': month,
                    'stock': curr_stock,
                    'change': round(curr_stock - prev_stock, 2)
                })
        
        # 최저점 찾기
        min_total_month = min(monthly_totals_list, key=lambda x: x['total_stock']) if monthly_totals_list else None
        
        print(f"조기경보 아이템 수: {len(early_warning_items)}개")
        print(f"긍정신호 아이템 수: {len(positive_signal_items)}개")
        
        # LLM 프롬프트 생성
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 재고 관리 전문가야. 당해 1월부터 현재월까지의 아이템별 재고 추이를 분석하여 조기경보, 긍정신호, 인사이트를 제시해야 해.

**분석 기간**
- 시작: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월
- 종료: {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월
- 기간: {unique_months}개월

**전체 요약**
- 총 재고액: {total_stock:,.0f}원 ({total_stock/1000000:.2f}백만원)
- 분석 아이템 수: {unique_items}개

**월별 총 재고 데이터**
{json_dumps_safe(monthly_totals_list, ensure_ascii=False, indent=2)}

**조기경보 데이터 (재고 증가, 최대 재고액, 수치 악화)**
{json_dumps_safe(early_warning_items[:10], ensure_ascii=False, indent=2)}

**긍정신호 데이터 (재고 감소)**
{json_dumps_safe(positive_signal_items[:10], ensure_ascii=False, indent=2)}

**인사이트 데이터**
- 최종 월({yyyymm_end[:4]}년 {yyyymm_end[4:6]}월) 총 재고액: {final_month_total:.2f}백만원
- 재고 감소 월: {len(decreasing_months)}개월
- 재고 증가 월: {len(increasing_months)}개월
- 최저점 월: {min_total_month['yyyymm'] if min_total_month else 'N/A'} ({min_total_month['total_stock'] if min_total_month else 0}백만원)

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 당해 1월부터 현재월까지 아이템별 재고 추이를 분석하여:
1. 조기경보: 재고 증가, 최대 재고액, 수치 악화 분석
2. 긍정신호: 재고 감소, 재고금액 감소 분석
3. 인사이트: 총 재고액, 재고가 감소한 월, 증가한 월 분석

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

{{
  "title": "아이템별 재고 종합분석 (당해 1월~현재월)",
  "sections": [
    {{
      "div": "overall-1",
      "sub_title": "조기경보",
      "ai_text": "[재고증가, 최대 재고액, 수치 악화를 분석하여 3줄로 요약]"
    }},
    {{
      "div": "overall-2",
      "sub_title": "긍정신호",
      "ai_text": "[재고감소, 재고금액 감소를 분석하여 3줄로 요약]"
    }},
    {{
      "div": "overall-3",
      "sub_title": "인사이트",
      "ai_text": "[총재고액, 재고가 감소한 월, 증가한 월을 분석하여 3줄로 요약]"
    }}
  ]
}}

<작성 가이드라인>
- **"조기경보" 섹션은 반드시 아래 형식으로 작성해야 합니다:**
  • [아이템명] 재고 지속 증가
  • [월] [최대 재고액]백만원
  • [1월比] [증가율]% 악화
  위 "조기경보 데이터"를 바탕으로 재고가 지속적으로 증가하고 있는 아이템을 분석하세요. 최대 재고액이 큰 아이템을 우선적으로 선택하여 각각 1줄씩 총 3줄로 작성하세요.

- **"긍정신호" 섹션은 반드시 아래 형식으로 작성해야 합니다:**
  • [아이템명] 대폭 감소
  • [초기 재고액] → [최종 재고액]백만원
  • 효과적 재고 관리
  위 "긍정신호 데이터"를 바탕으로 재고가 효과적으로 감소한 아이템을 분석하세요. 감소율이 큰 아이템을 우선적으로 선택하여 각각 1줄씩 총 3줄로 작성하세요.

- **"인사이트" 섹션은 반드시 아래 형식으로 3줄로 작성해야 합니다:**
  • 총재고 [최종월] [총재고액]백만원
  • [월] 저점 후 반등 (또는 [월] 고점 후 하락)
  • [시즌] 재고 증가 (또는 [시즌] 재고 감소)
  위 "인사이트 데이터"와 "월별 총 재고 데이터"를 바탕으로 전체 재고 추이를 분석하세요. 최종 월 총 재고액, 최저점/최고점, 시즌별 패턴을 분석하여 3줄로 작성하세요.

- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 백만원 단위로 표시하고 절대 변형하지 말 것
- 재고 증가/감소 패턴 분석
- 시즌별 재고 트렌드 파악
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시 (예: "첫 번째 줄\\n두 번째 줄")
- 불릿 포인트나 리스트 항목 사이에는 \\n을 사용
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
"""
        
        # LLM 호출 (JSON 응답)
        analysis_response = call_llm(prompt, max_tokens=4000)
        
        # JSON 파싱 (마크다운 코드 블록 제거)
        analysis_response = analysis_response.strip()
        if analysis_response.startswith('```json'):
            analysis_response = analysis_response[7:]
        if analysis_response.startswith('```'):
            analysis_response = analysis_response[3:]
        if analysis_response.endswith('```'):
            analysis_response = analysis_response[:-3]
        analysis_response = analysis_response.strip()
        
        try:
            analysis_data = json.loads(analysis_response)
        except json.JSONDecodeError as e:
            print(f"[WARNING] JSON 파싱 실패: {e}")
            print(f"[WARNING] 응답 내용: {analysis_response[:500]}")
            # 기본 구조로 대체
            analysis_data = {
                "title": "아이템별 재고 종합분석 (당해 1월~현재월)",
                "sections": [
                    {"div": "overall-1", "sub_title": "조기경보", "ai_text": analysis_response},
                    {"div": "overall-2", "sub_title": "긍정신호", "ai_text": "데이터 분석 중"},
                    {"div": "overall-3", "sub_title": "인사이트", "ai_text": "데이터 분석 중"}
                ]
            }
        
        # JSON 데이터 생성
        # yyyymm_py 계산 (전년 동월)
        previous_year = int(yyyymm_end[:4]) - 1
        yyyymm_py = f"{previous_year}{yyyymm_end[4:6]}"
        
        json_data = {
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm': yyyymm_end,
            'yyyymm_py': yyyymm_py,
            'analysis_data': analysis_data,
            'summary': {
                'total_stock': round(total_stock / 1000000, 2),
                'unique_months': unique_months,
                'unique_items': unique_items,
                'analysis_period': f"{yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월"
            },
            'monthly_totals': monthly_totals_list,
            'early_warning_items': early_warning_items[:10],
            'positive_signal_items': positive_signal_items[:10],
            'insights': {
                'final_month_total': final_month_total,
                'decreasing_months_count': len(decreasing_months),
                'increasing_months_count': len(increasing_months),
                'min_month': min_total_month['yyyymm'] if min_total_month else None,
                'min_stock': min_total_month['total_stock'] if min_total_month else 0
            },
            'raw_data': {
                'sample_records': [
                    {
                        'YYYYMM': r.get('YYYYMM', ''),
                        'ITEM_STD': r.get('ITEM_STD', ''),
                        'CY_END_STOCK_TAG_AMT': float(r.get('CY_END_STOCK_TAG_AMT') or 0),
                        'PY_END_STOCK_TAG_AMT': float(r.get('PY_END_STOCK_TAG_AMT') or 0),
                        'YOY': float(r.get('YOY') or 0)
                    }
                    for r in records[:100]
                ],
                'total_records_count': len(records)
            }
        }
        
        # 파일 저장
        filename = f"16-1-1-1.{brd_cd}_아이템_재고_종합분석(당해1월~현재월)"
        save_json(json_data, filename)
        
        # Markdown 파일 생성
        markdown_content = f"# {analysis_data.get('title', '아이템별 재고 종합분석 (당해 1월~현재월)')}\n\n"
        for section in analysis_data.get('sections', []):
            markdown_content += f"## {section.get('sub_title', '')}\n\n"
            markdown_content += f"{section.get('ai_text', '')}\n\n"
        save_markdown(markdown_content, filename)
        
        print(f"[OK] 분석 완료!\n")
        return json_data
        
    finally:
        engine.dispose()

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
    
    # 분석 기간 설정 (202401부터 202509까지)
    yyyymm_list = []
    for year in range(2024, 2026):  # 2024년, 2025년
        for month in range(1, 13):  # 1월부터 12월까지
            if year == 2024 or (year == 2025 and month <= 9):  # 2024년 전체, 2025년 9월까지
                yyyymm = f"{year}{month:02d}"
                yyyymm_list.append(yyyymm)
    
    print(f"분석할 기간: {len(yyyymm_list)}개월 ({yyyymm_list[0]} ~ {yyyymm_list[-1]})")
    
    # 브랜드 선택 (원하는 브랜드만 주석 해제)
    brands_to_analyze = [
        'M',   # MLB
        'I',   # MLB KIDS
        'X',   # DISCOVERY
        'V',   # DUVETICA
        'ST',  # SERGIO TACCHINI
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
                analyze_channel_sales(yyyymm, brd_cd)  # 채널별 TOP3 매출 분석 (4-1-1-1)
                analyze_channel_sales_overall(yyyymm, brd_cd)  # 브랜드별 채널 매출 종합분석 (4-1-1-2)
                analyze_gender_purchase_pattern(yyyymm, brd_cd)  # 성별 구매 패턴 분석 (4-1-3-1)
                analyze_gender_purchase_pattern_overall(yyyymm, brd_cd)  # 성별 구매 패턴 종합분석 (4-1-3-2)
                analyze_category_profit(yyyymm, brd_cd)  # 카테고리별 수익성 분석 (5-2-1-1)
                analyze_category_profit_overall(yyyymm, brd_cd)  # 카테고리별 수익성 종합분석 (5-2-1-2)
                analyze_operating_expense(yyyymm, brd_cd)  # 영업비 추이분석(6-1-1-1)
                analyze_discount_rate_overall(yyyymm, brd_cd)  # 할인율 종합분석 (7-1-1-1)
                analyze_store_efficiency_overall(yyyymm, brd_cd)  # 매장효율성 종합분석 (8-1-1-1)
                analyze_channel_sales_overall_12m(yyyymm, brd_cd)  # 채널별 매출 종합분석 (당해 1월~현재월) (14-1-1-1)
                analyze_item_sales_overall(yyyymm, brd_cd)  # 아이템별 매출 종합분석 (당해 1월~현재월) (15-1-1-1)
                analyze_item_stock_overall(yyyymm, brd_cd)  # 아이템별 재고 종합분석 (당해 1월~현재월) (16-1-1-1)
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
    print(f"{'='*60}")
    print(f"\n{'='*60}")
    print(f"실행 시간 정보")
    print(f"{'='*60}")
    print(f"시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"종료 시간: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"총 실행 시간: {elapsed_time}")
    print(f"  - {elapsed_time.total_seconds():.2f}초")
    print(f"  - {elapsed_time.total_seconds() / 60:.2f}분")
    print(f"\n{'='*60}")
    print(f"토큰 사용량 정보")
    print(f"{'='*60}")
    print(f"입력 토큰: {total_tokens['input']:,} 토큰")
    print(f"출력 토큰: {total_tokens['output']:,} 토큰")
    print(f"총 토큰: {total_token_count:,} 토큰")
    print(f"{'='*60}\n")
