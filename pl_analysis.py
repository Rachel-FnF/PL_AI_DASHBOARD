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
from datetime import datetime
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
    print(f"[OK] LLM 응답 완료")
    return message.content[0].text

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

def save_json(data, filename):
    """JSON 파일 저장"""
    file_path = os.path.join(OUTPUT_JSON_PATH, f"{filename}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
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

def get_ad_expense_detail_query(yyyymm, yyyymm_py, brd_cd):
    """광고선전비 당해/전년 세부 내역 쿼리"""
    return f"""
    SELECT PST_YYYYMM, CTGR1, CTGR2, CTGR3, GL_NM, SUM(TTL_USE_AMT) AS AD_TTL_AMT
    FROM SAP_FNF.DM_IDCST_CCTR_M
    WHERE BRD_CD = '{brd_cd}'
      AND PST_YYYYMM = '{yyyymm}'
      AND CTGR1 = '광고선전비'
    GROUP BY PST_YYYYMM, BRD_NM, CTGR1, CTGR2, CTGR3, GL_NM
    
    UNION ALL
    
    SELECT PST_YYYYMM, CTGR1, CTGR2, CTGR3, GL_NM, SUM(TTL_USE_AMT) AS AD_TTL_AMT
    FROM SAP_FNF.DM_IDCST_CCTR_M
    WHERE BRD_CD = '{brd_cd}'
      AND PST_YYYYMM = '{yyyymm_py}'
      AND CTGR1 = '광고선전비'
    GROUP BY PST_YYYYMM, BRD_NM, CTGR1, CTGR2, CTGR3, GL_NM
    ORDER BY AD_TTL_AMT DESC
    """

def get_ad_expense_trend_query(trend_months, brd_cd):
    """광고선전비 12개월 추세 세부 내역 쿼리"""
    trend_months_str = "', '".join(trend_months)
    return f"""
    SELECT PST_YYYYMM,
           CTGR2,
           CTGR3,
           GL_NM,
           SUM(TTL_USE_AMT) AS TTL_USE_AMT
    FROM SAP_FNF.DM_IDCST_CCTR_M
    WHERE PST_YYYYMM IN ('{trend_months_str}')
      AND CTGR1 = '광고선전비'
      AND BRD_CD = '{brd_cd}'
    GROUP BY PST_YYYYMM, CTGR2, CTGR3, GL_NM
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
{json.dumps(channel_comparison, ensure_ascii=False, indent=2)}

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 각 채널별 당해 당월 매출 베스트 아이템 3개를 전년대비 주요변화로 분석해줘.

**중요**: 위 "채널별 데이터 요약"에 있는 채널만 분석하면 됩니다. 데이터가 없는 채널은 분석하지 마세요.

<데이터 샘플>
{json.dumps(records[:200], ensure_ascii=False, indent=2)}

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

각 채널별로 하나의 섹션을 만들어야 합니다. 채널 목록: {', '.join(valid_channels)}

{{
  "title": "채널별 매출 top3 분석 (당해 전년 주요변화)",
  "sections": [
    {{
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
    """채널별 매출 종합분석 (12개월 추이) - 4-1-1-2"""
    print(f"\n{'='*60}")
    print(f"채널별 매출 종합분석 시작 (4-1-1-2): {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
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
        
        # LLM 프롬프트 생성 (JSON 형식 응답 요청)
        prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 채널 전략 전문가야. 12개월간의 채널별 매출 추이를 분석하여 채널별 성과와 아이템 포트폴리오 전략을 제시해야 해.

**분석 기간**
- 시작: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월
- 종료: {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월
- 기간: {unique_months}개월

**전체 요약**
- 총 매출액: {total_sales:,.0f}원 ({total_sales/1000000:.2f}백만원)
- 분석 채널 수: {unique_channels}개
- 분석 아이템 수: {unique_items}개

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 12개월간 채널별 매출 추이를 분석하여:
1. 채널별 성과와 성장 패턴 파악
2. 채널별 핵심 아이템(클래스3) 식별
3. 채널별 매출 기여도와 비중 분석
4. 채널별 전략적 인사이트 제시

<데이터 샘플>
{json.dumps(records[:100], ensure_ascii=False, indent=2)}

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

{{
  "title": "채널별 매출 분석 (12개월 추이)",
  "sections": [
    {{
      "sub_title": "채널별 성과 종합 평가",
      "ai_text": "12개월간의 채널별 매출 성과를 종합적으로 평가한 내용 (예: 자사몰이 전체 매출의 45%를 차지하며 핵심 채널로 부상, 직영점은 안정적 성장세 유지 등)"
    }},
    {{
      "sub_title": "성장 채널 및 기회",
      "ai_text": "성장세가 뚜렷한 채널과 기회를 불릿 포인트로 나열 (예: • 자사몰: 12개월간 지속적 성장으로 전체 매출의 45% 기여 등)"
    }},
    {{
      "sub_title": "주의 필요 채널",
      "ai_text": "주의가 필요한 채널들을 불릿 포인트로 나열 (예: • 제휴몰: 최근 3개월간 매출 감소 추세 등)"
    }},
    {{
      "sub_title": "이상징후 및 리스크 감지",
      "ai_text": "이상징후와 리스크를 구체적으로 설명 (예: • 특정 채널의 아이템 집중도 과다: 자사몰의 상위 3개 아이템이 전체의 60% 차지 등)"
    }},
    {{
      "sub_title": "채널별 전략 최적화 방안",
      "ai_text": "단기 전략 방향과 중장기 전략 방향을 구체적으로 제시 (예: ### 즉시 실행 방안\\n1. 자사몰 아이템 포트폴리오 다변화: ... 등)"
    }}
  ]
}}

<작성 가이드라인>
- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 백만원 단위로 표시하고 절대 변형하지 말 것
- 모든 광고선전비 계정 (CTGR3) 누락 없이 분석
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
                "title": "채널별 매출 분석 (12개월 추이)",
                "sections": [
                    {"sub_title": "분석 결과", "ai_text": analysis_response}
                ]
            }
        
        # JSON 데이터 생성
        json_data = {
            'brand_cd': brd_cd,
            'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
            'yyyymm_start': yyyymm_start,
            'yyyymm_end': yyyymm_end,
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
        
        # 파일 저장 (4-1-1-2로 저장)
        filename = f"4-1-1-2.{brd_cd}_채널별_매출_종합분석(12개월추이)"
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

def analyze_ad_expense(yyyymm, brd_cd):
    """광고선전비 추이 분석"""
    print(f"\n{'='*60}")
    print(f"광고선전비 추이 분석 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
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
        
        # 1. 당해/전년 세부 내역 쿼리 실행
        detail_sql = get_ad_expense_detail_query(yyyymm, yyyymm_py, brd_cd)
        detail_df = run_query(detail_sql, engine)
        detail_records = detail_df.to_dicts()
        
        if not detail_records:
            print("데이터가 없습니다.")
            return None
        
        # 2. 전체 합계 계산
        curr_total = sum(float(r.get('AD_TTL_AMT', 0)) for r in detail_records if r.get('PST_YYYYMM') == yyyymm)
        prev_total = sum(float(r.get('AD_TTL_AMT', 0)) for r in detail_records if r.get('PST_YYYYMM') == yyyymm_py)
        change_amount = curr_total - prev_total
        change_pct = (change_amount / prev_total * 100) if prev_total != 0 else 0
        
        print(f"전년 합계: {prev_total:,.0f}원 ({prev_total/1000000:.2f}백만원)")
        print(f"당해 합계: {curr_total:,.0f}원 ({curr_total/1000000:.2f}백만원)")
        print(f"변화액: {change_amount:,.0f}원 ({change_pct:.1f}%)")
        
        # 3. 12개월 추세 데이터 (현재 월부터 12개월 전까지)
        trend_months = []
        for i in range(12):
            year = current_year
            month = current_month - i
            while month <= 0:
                month += 12
                year -= 1
            trend_months.append(f"{year:04d}{month:02d}")
        trend_months.reverse()
        
        trend_sql = get_ad_expense_trend_query(trend_months, brd_cd)
        trend_df = run_query(trend_sql, engine)
        trend_records = trend_df.to_dicts()
        
        # 4. 월별 합계 계산
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
        
        # 5. 카테고리별 데이터 정리
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
        
        # 6. 카테고리 요약
        increased = [c for c in categories if c['change'] > 0]
        decreased = [c for c in categories if c['change'] < 0]
        new_investments = [c for c in categories if c['is_new']]
        discontinued = [c for c in categories if c['is_discontinued']]
        
        # 7. LLM 프롬프트 생성 (JSON 형식 응답 요청)
        total_records_json = json.dumps([
            {'PST_YYYYMM': yyyymm_py, 'TOTAL_AMT': prev_total},
            {'PST_YYYYMM': yyyymm, 'TOTAL_AMT': curr_total}
        ], ensure_ascii=False, indent=2)
        
        detail_records_json = json.dumps([
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
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 마케팅 전략 책임자야. {previous_year}년 {current_month}월과 {current_year}년 {current_month}월의 광고선전비를 비교 분석하여 마케팅 투자 효율성과 최적화 방안을 제시해야 해.

**분석 기간**
- 당해: {current_year}년 {current_month}월
- 전년: {previous_year}년 {current_month}월

<분석 목표>
{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월 광고선전비 투자 변화를 분석하여 마케팅 전략의 효과성과 향후 예산 배분 전략을 경영관리팀에게 수립해줘.

<전체 합계 데이터>
{total_records_json}

<세부 계정별 데이터>
{detail_records_json}

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

{{
  "title": "광고비 분석",
  "sections": [
    {{
      "sub_title": "투자 방향성 종합 평가",
      "ai_text": "전년대비 {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월 광고비 변화를 종합적으로 평가한 내용 (예: 선택적 축소 - 효율성 중심 예산 재배분 등)"
    }},
    {{
      "sub_title": "효율적 투자 영역",
      "ai_text": "효과적인 투자 영역들을 불릿 포인트로 나열 (예: • 모델료 신규 투입: 122.9백만원으로 브랜드 이미지 제고 및 소비자 어필 강화 등)"
    }},
    {{
      "sub_title": "주의 필요 영역",
      "ai_text": "주의가 필요한 영역들을 불릿 포인트로 나열 (예: • E-BIZ 매체광고 증가: 9.2→14.0백만원(+51.8%)로 급격한 증가 원인 등)"
    }},
    {{
      "sub_title": "이상징후 및 리스크 감지",
      "ai_text": "이상징후와 리스크를 구체적으로 설명 (예: • 예산 배분의 극단적 변화: 일부 계정의 전액 삭감(기타 광고비)과 신규 대규모 투입(모델료)이 동시 발생하여 마케팅 전략의 급격한 방향 전환을 시사합니다 등)"
    }},
    {{
      "sub_title": "마케팅 전략 최적화 방안",
      "ai_text": "단기 전략 방향과 중장기 전략 방향을 구체적으로 제시 (예: ### 즉시 실행 방안\\n1. 모델 마케팅 효과 측정: ... 등)"
    }}
  ]
}}

<작성 가이드라인>
- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 백만원 단위로 표시하고 절대 변형하지 말 것
- 모든 광고선전비 계정 (CTGR3) 누락 없이 분석
- 전년대비 변화에 대한 구체적 원인과 효과 분석
- 단기 전략 방향성 제시와 중장기 전략 방향을 구체적으로 제시
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시 (예: "첫 번째 줄\\n두 번째 줄")
- ai_text 내에서 여러 문단이나 항목을 나눌 때는 \\n\\n을 사용
- 불릿 포인트나 리스트 항목 사이에는 \\n을 사용
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
"""
        
        # 8. LLM 호출 (JSON 응답)
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
                "title": "광고비 분석",
                "sections": [
                    {"sub_title": "분석 결과", "ai_text": analysis_response}
                ]
            }
        
        # 9. JSON 데이터 생성
        json_data = {
            'brand_cd': brd_cd,
            'yyyymm': yyyymm,
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
        
        # 10. 파일 저장
        filename = f"6-1-1-1.{brd_cd}_광고선전비_추이분석"
        save_json(json_data, filename)
        
        # Markdown도 저장 (analysis_data의 sections를 조합)
        markdown_content = f"# {analysis_data.get('title', '광고비 분석')}\n\n"
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
    # 분석 설정
    yyyymm = '202509'  # 분석할 년월
    brd_cd = 'M'       # 브랜드 코드
    
    # 분석 실행 (원하는 분석만 주석 해제)
    analyze_channel_sales(yyyymm, brd_cd)  # 채널별 매출 분석 (4-1-1-1)
    analyze_channel_sales_overall(yyyymm, brd_cd)  # 채널별 매출 종합분석 (4-1-1-2)
    analyze_ad_expense(yyyymm, brd_cd)  # 광고선전비 추이 분석(6-1-1-1)
