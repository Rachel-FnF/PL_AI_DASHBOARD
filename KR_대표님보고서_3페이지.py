"""
KR 대표님 보고용 3페이지 요약 보고서 생성 도구
- 기존 분석 JSON 파일들을 읽어서 대표님 보고용 3페이지 요약 보고서 생성
- 숫자 최소화, AI 인사이트 중심의 엑기스 요약
- HTML 형식으로 출력 (PDF 변환 가능)
"""

import os
import json
import glob
import anthropic
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv

# PDF 생성 라이브러리 (선택적)
try:
    from weasyprint import HTML
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("[INFO] weasyprint이 설치되지 않았습니다. PDF 생성 기능을 사용하려면 'pip install weasyprint'을 실행하세요.")

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

OUTPUT_JSON_PATH = './kr_output/json'
OUTPUT_MD_PATH = './kr_output/md'
OUTPUT_REPORT_PATH = './kr_output/reports'

# 출력 폴더 생성
os.makedirs(OUTPUT_JSON_PATH, exist_ok=True)
os.makedirs(OUTPUT_MD_PATH, exist_ok=True)
os.makedirs(OUTPUT_REPORT_PATH, exist_ok=True)

# ============================================================================
# LLM 호출
# ============================================================================
_total_tokens_used = {'input': 0, 'output': 0}

def call_llm(prompt, max_tokens=4000, temperature=0.7):
    """Claude API 호출"""
    api_key = os.getenv('CLAUDE_API_KEY')
    if not api_key:
        raise ValueError("CLAUDE_API_KEY가 설정되지 않았습니다. .env 파일을 확인하세요.")
    
    client = anthropic.Anthropic(api_key=api_key, timeout=120.0)
    
    system_prompt = """
당신은 F&F 그룹의 최고 전략 분석가이자 대표님 보고서 작성 전문가입니다. 다음 원칙을 반드시 준수하세요:

📊 **보고서 작성 원칙**
- 대표님은 숫자가 많이 나열된 보고서를 좋아하지 않으므로, 핵심 숫자만 선별적으로 제시
- 모든 금액은 백만원 단위로 표시하고 정수만 사용 (소수점 없음)
- AI 분석 결과를 엑기스처럼 요약하여 핵심만 전달
- 근거 자료는 간결하게 제시
- 즉시 실행 가능한 액션플랜 중심으로 작성
- 리스크와 기회를 명확히 구분하여 제시

📝 **작성 스타일**
- 간결하고 명확한 문장 사용
- 불릿 포인트 중심의 구조화된 내용
- 숫자는 최소한만 사용하고, 대신 트렌드와 인사이트 중심
- 각 섹션은 독립적으로 읽을 수 있도록 구성
- 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)
"""
    
    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=max_tokens,
            temperature=temperature,
            system=system_prompt,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        response_text = message.content[0].text
        
        # 토큰 사용량 추적
        if hasattr(message, 'usage'):
            _total_tokens_used['input'] += message.usage.input_tokens
            _total_tokens_used['output'] += message.usage.output_tokens
        
        return response_text
    except Exception as e:
        print(f"[ERROR] LLM 호출 실패: {e}")
        raise

def get_total_tokens():
    """전체 토큰 사용량 반환"""
    return _total_tokens_used.copy()

def reset_token_counter():
    """토큰 카운터 초기화"""
    global _total_tokens_used
    _total_tokens_used = {'input': 0, 'output': 0}

# ============================================================================
# JSON 파싱
# ============================================================================
def parse_llm_json_response(response_text, default_title="분석 결과"):
    """LLM 응답에서 JSON을 파싱"""
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
        analysis_data = {
            "title": default_title,
            "content": response_text
        }
    
    return analysis_data

# ============================================================================
# 데이터 로드 및 집계
# ============================================================================
def load_all_analysis_files(yyyymm, brd_cd):
    """특정 월, 브랜드의 모든 분석 파일 로드"""
    yyyymm_short = yyyymm[2:]  # 202512 -> 2512
    pattern = os.path.join(OUTPUT_JSON_PATH, f"KR_{yyyymm_short}_{brd_cd}_*.json")
    
    json_files = glob.glob(pattern)
    analysis_data = {}
    
    # 통합 요약 파일은 제외
    json_files = [f for f in json_files if '월별브랜드통합요약' not in f and '월별전체브랜드통합요약' not in f]
    
    print(f"[INFO] {len(json_files)}개 분석 파일 발견")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            filename = os.path.basename(json_file)
            key = data.get('key', '')
            sub_key = data.get('sub_key', '')
            
            # 파일명에서 분석 유형 추출
            if '실판매출_채널별매출분석' in filename:
                analysis_data['channel_sales'] = data
            elif '영업이익_아이템별직접이익' in filename:
                analysis_data['category_profit'] = data
            elif '영업이익_매장별직접이익' in filename:
                analysis_data['store_profit'] = data
            elif '할인율_종합분석' in filename:
                analysis_data['discount_rate'] = data
            elif '매장효율성_종합분석' in filename:
                analysis_data['store_efficiency'] = data
            elif '실판매출_성별구매패턴' in filename:
                analysis_data['gender_pattern'] = data
            elif '영업비_' in filename:
                if 'operating_expense' not in analysis_data:
                    analysis_data['operating_expense'] = []
                analysis_data['operating_expense'].append(data)
            elif '월별채널별매출추세' in filename:
                analysis_data['channel_trend'] = data
            elif '월별아이템별매출추세' in filename:
                analysis_data['item_sales_trend'] = data
            elif '월별아이템별재고추세' in filename:
                analysis_data['item_stock_trend'] = data
                
        except Exception as e:
            print(f"[WARNING] {json_file} 파일 읽기 실패: {e}")
            continue
    
    return analysis_data

def extract_key_metrics(analysis_data):
    """핵심 지표 추출"""
    metrics = {
        'total_sales': 0,
        'total_sales_yoy': 0,
        'operating_profit': 0,
        'operating_profit_rate': 0,
        'operating_profit_rate_change': 0,
        'discount_rate': 0,
        'discount_rate_change': 0,
        'direct_profit_rate': 0,
        'direct_profit_rate_change': 0,
        'cogs_rate': 0,
        'cogs_rate_change': 0,
        'store_count': 0,
        'store_count_change': 0,
        'sales_per_store': 0,
        'sales_per_store_yoy': 0,
        'sales_per_person': 0,
        'sales_per_person_yoy': 0,
    }
    
    # 채널별 매출 분석에서 총 매출 추출
    if 'channel_sales' in analysis_data:
        cs = analysis_data['channel_sales']
        if 'summary' in cs:
            metrics['total_sales'] = cs['summary'].get('total_sales_cy', 0)
            metrics['total_sales_yoy'] = cs['summary'].get('change_pct', 0)
    
    # 카테고리별 수익성 분석에서 수익률 추출
    if 'category_profit' in analysis_data:
        cp = analysis_data['category_profit']
        if 'summary' in cp:
            # 직접이익률은 카테고리별 분석에서 추출 가능
            pass
    
    # 매장별 수익 분석에서 매장 수 및 점당매출 추출
    if 'store_profit' in analysis_data:
        sp = analysis_data['store_profit']
        if 'summary' in sp:
            metrics['store_count'] = sp['summary'].get('store_count', 0) or 0
            metrics['store_count_change'] = sp['summary'].get('store_count_change', 0) or 0
            metrics['sales_per_store'] = sp['summary'].get('sales_per_store', 0) or 0
            metrics['sales_per_store_yoy'] = sp['summary'].get('sales_per_store_yoy', 0) or 0
    
    # 할인율 분석에서 할인율 추출
    if 'discount_rate' in analysis_data:
        dr = analysis_data['discount_rate']
        if 'summary' in dr:
            metrics['discount_rate'] = dr['summary'].get('overall_discount_rate', 0)
            metrics['discount_rate_change'] = dr['summary'].get('discount_rate_change', 0)
    
    return metrics

# ============================================================================
# 보고서 생성
# ============================================================================
def generate_executive_report(yyyymm, brd_cd):
    """대표님 보고용 3페이지 요약 보고서 생성"""
    print(f"\n{'='*60}")
    print(f"대표님 보고서 생성 시작: {brd_cd} ({BRAND_CODE_MAP.get(brd_cd, brd_cd)}) - {yyyymm}")
    print(f"{'='*60}\n")
    
    try:
        # 모든 분석 파일 로드
        analysis_data = load_all_analysis_files(yyyymm, brd_cd)
        
        if not analysis_data:
            print(f"[ERROR] 분석 데이터를 찾을 수 없습니다.")
            print(f"[INFO] 다음 경로에서 파일을 찾고 있습니다: {os.path.join(OUTPUT_JSON_PATH, f'KR_{yyyymm[2:]}_{brd_cd}_*.json')}")
            return None
    except Exception as e:
        print(f"[ERROR] 분석 파일 로드 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # 핵심 지표 추출
    metrics = extract_key_metrics(analysis_data)
    
    # 각 페이지별로 AI를 통해 요약 생성
    print("[INFO] Page 1: Executive Summary 생성 중...")
    page1 = generate_page1_executive_summary(yyyymm, brd_cd, analysis_data, metrics)
    
    print("[INFO] Page 2: Detailed Analysis 생성 중...")
    page2 = generate_page2_detailed_analysis(yyyymm, brd_cd, analysis_data, metrics)
    
    print("[INFO] Page 3: Strategy & Action Plan 생성 중...")
    page3 = generate_page3_strategy_plan(yyyymm, brd_cd, analysis_data, metrics)
    
    # 전체 보고서 통합
    report_data = {
        'brand_cd': brd_cd,
        'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
        'yyyymm': yyyymm,
        'report_date': datetime.now().strftime('%Y-%m-%d'),
        'page1': page1,
        'page2': page2,
        'page3': page3,
        'metrics': metrics
    }
    
    # JSON 저장
    yyyymm_short = yyyymm[2:]
    json_filename = os.path.join(OUTPUT_REPORT_PATH, f"KR_{yyyymm_short}_{brd_cd}_대표님보고서_3페이지.json")
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)
    print(f"[OK] JSON 저장 완료: {json_filename}")
    
    # HTML 저장
    html_content = generate_html_report(report_data)
    html_filename = os.path.join(OUTPUT_REPORT_PATH, f"KR_{yyyymm_short}_{brd_cd}_대표님보고서_3페이지.html")
    with open(html_filename, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"[OK] HTML 저장 완료: {html_filename}")
    
    # PDF 저장 (weasyprint 사용 가능한 경우)
    if PDF_AVAILABLE:
        try:
            pdf_filename = os.path.join(OUTPUT_REPORT_PATH, f"KR_{yyyymm_short}_{brd_cd}_대표님보고서_3페이지.pdf")
            HTML(string=html_content).write_pdf(pdf_filename)
            print(f"[OK] PDF 저장 완료: {pdf_filename}")
        except Exception as e:
            print(f"[WARNING] PDF 생성 실패: {e}")
            print(f"[INFO] HTML 파일을 브라우저에서 열어 '인쇄 > PDF로 저장' 기능을 사용할 수 있습니다.")
    else:
        print(f"[INFO] PDF 생성 기능을 사용하려면 'pip install weasyprint'을 실행하세요.")
        print(f"[INFO] 또는 HTML 파일을 브라우저에서 열어 '인쇄 > PDF로 저장' 기능을 사용할 수 있습니다.")
    
    return report_data

def generate_page1_executive_summary(yyyymm, brd_cd, analysis_data, metrics):
    """Page 1: Executive Summary 생성"""
    brand_name = BRAND_CODE_MAP.get(brd_cd, brd_cd)
    year = yyyymm[:4]
    month = yyyymm[4:6]
    
    # 분석 데이터 요약 텍스트 생성
    data_summary = []
    
    if 'channel_sales' in analysis_data:
        cs = analysis_data['channel_sales']
        if 'summary' in cs:
            total_sales_cy = cs['summary'].get('total_sales_cy', 0) or 0
            change_pct = cs['summary'].get('change_pct', 0) or 0
            try:
                data_summary.append(f"총 실판매출: {int(total_sales_cy):,}백만원 (YoY {float(change_pct):.1f}%)")
            except (ValueError, TypeError):
                data_summary.append(f"총 실판매출: {total_sales_cy}백만원 (YoY {change_pct}%)")
        if 'analysis_data' in cs and 'sections' in cs['analysis_data']:
            channel_insights = []
            for section in cs['analysis_data']['sections']:
                if section.get('div') not in ['종합분석-1', '종합분석-2', '종합분석-3']:
                    channel_insights.append(f"{section.get('div', '')}: {section.get('ai_text', '')[:200]}")
            data_summary.append("채널별 분석: " + " | ".join(channel_insights[:3]))
    
    if 'category_profit' in analysis_data:
        cp = analysis_data['category_profit']
        if 'summary' in cp:
            total_profit = cp['summary'].get('total_profit', 0) or 0
            profit_rate = cp['summary'].get('total_profit_rate', 0) or 0
            try:
                data_summary.append(f"총 수익: {int(total_profit):,}백만원, 수익률: {float(profit_rate):.1f}%")
            except (ValueError, TypeError):
                data_summary.append(f"총 수익: {total_profit}백만원, 수익률: {profit_rate}%")
    
    if 'store_profit' in analysis_data:
        sp = analysis_data['store_profit']
        if 'summary' in sp:
            store_count = sp['summary'].get('store_count', 0) or 0
            sales_per_store = sp['summary'].get('sales_per_store', 0) or 0
            try:
                data_summary.append(f"매장 수: {int(store_count)}개, 점당매출: {int(sales_per_store):,}백만원")
            except (ValueError, TypeError):
                data_summary.append(f"매장 수: {store_count}개, 점당매출: {sales_per_store}백만원")
    
    if 'discount_rate' in analysis_data:
        dr = analysis_data['discount_rate']
        if 'summary' in dr:
            discount_rate = dr['summary'].get('overall_discount_rate', 0) or 0
            discount_change = dr['summary'].get('discount_rate_change', 0) or 0
            try:
                data_summary.append(f"할인율: {float(discount_rate):.1f}% (전년대비 {float(discount_change):+.1f}%p)")
            except (ValueError, TypeError):
                data_summary.append(f"할인율: {discount_rate}% (전년대비 {discount_change}%p)")
    
    # 분석 데이터를 안전하게 요약 (너무 큰 데이터 방지)
    try:
        analysis_summary = json.dumps(analysis_data, ensure_ascii=False, indent=2, default=str)[:5000]
    except Exception as e:
        print(f"[WARNING] 분석 데이터 직렬화 실패: {e}")
        analysis_summary = str(analysis_data)[:5000]
    
    prompt = f"""
다음은 {brand_name} 브랜드 {year}년 {month}월 실적 분석 데이터입니다.

**핵심 지표:**
{chr(10).join(data_summary) if data_summary else '데이터 없음'}

**분석 데이터 요약:**
{analysis_summary}

위 데이터를 바탕으로 대표님 보고용 Executive Summary를 작성해주세요. 다음 구조로 JSON 형식으로 응답해주세요:

{{
  "executive_summary": {{
    "title": "브랜드명 연월 실적 보고서",
    "subtitle": "경영관리팀 FP&A | 보고일: YYYY년 MM월",
    "key_metrics": {{
      "total_sales": "총 실판매출 (백만원)",
      "sales_yoy": "YoY 성장률",
      "operating_profit": "영업이익 (백만원)",
      "operating_profit_rate": "영업이익률 (%)",
      "operating_profit_rate_change": "영업이익률 변화 (%p)",
      "discount_rate": "할인율 (%)",
      "discount_rate_change": "할인율 변화 (%p)",
      "direct_profit_rate": "직접이익률 (%)",
      "direct_profit_rate_change": "직접이익률 변화 (%p)",
      "cogs_rate": "매출원가율 (%)",
      "cogs_rate_change": "매출원가율 변화 (%p)",
      "sales_per_store": "점당매출 (백만원)",
      "sales_per_store_yoy": "점당매출 YoY (%)",
      "sales_per_person": "인당매출 (백만원)",
      "sales_per_person_yoy": "인당매출 YoY (%)"
    }},
    "channel_summary": [
      {{
        "channel": "채널명",
        "sales": "매출 (백만원)",
        "yoy": "YoY (%)",
        "profit_rate": "이익률 (%)",
        "weight": "비중 (%)"
      }}
    ],
    "category_summary": [
      {{
        "category": "카테고리명",
        "sales": "매출 (백만원)",
        "yoy": "YoY (%)",
        "weight": "비중 (%)"
      }}
    ],
    "critical_risks": [
      "리스크 1: 구체적인 설명",
      "리스크 2: 구체적인 설명"
    ],
    "positive_signals": [
      "긍정 신호 1: 구체적인 설명",
      "긍정 신호 2: 구체적인 설명"
    ],
    "key_points": "해당 월의 핵심 포인트를 3-5줄로 요약"
  }}
}}

**중요 지침:**
- 숫자는 최소한만 사용하고, 핵심 지표만 선별적으로 제시
- Critical Risks와 Positive Signals는 각각 3-5개 정도로 구체적으로 작성
- Key Points는 해당 월의 가장 중요한 인사이트를 간결하게 요약
- 모든 금액은 백만원 단위 정수로 표기
"""
    
    response = call_llm(prompt, max_tokens=4000)
    page1_data = parse_llm_json_response(response, "Executive Summary")
    
    return page1_data

def generate_page2_detailed_analysis(yyyymm, brd_cd, analysis_data, metrics):
    """Page 2: Detailed Analysis 생성"""
    brand_name = BRAND_CODE_MAP.get(brd_cd, brd_cd)
    year = yyyymm[:4]
    month = yyyymm[4:6]
    
    # 분석 데이터를 안전하게 요약
    try:
        analysis_summary = json.dumps(analysis_data, ensure_ascii=False, indent=2, default=str)[:8000]
    except Exception as e:
        print(f"[WARNING] 분석 데이터 직렬화 실패: {e}")
        analysis_summary = str(analysis_data)[:8000]
    
    prompt = f"""
다음은 {brand_name} 브랜드 {year}년 {month}월 상세 분석 데이터입니다.

**분석 데이터:**
{analysis_summary}

위 데이터를 바탕으로 대표님 보고용 상세 분석 페이지를 작성해주세요. 다음 구조로 JSON 형식으로 응답해주세요:

{{
  "detailed_analysis": {{
    "channel_analysis": {{
      "high_growth_channels": [
        {{
          "channel": "채널명",
          "sales": "매출 (백만원)",
          "yoy": "YoY (%)",
          "profit_rate": "직접이익률 (%)",
          "success_factors": "성공 요인 설명"
        }}
      ],
      "underperforming_channels": [
        {{
          "channel": "채널명",
          "sales": "매출 (백만원)",
          "yoy": "YoY (%)",
          "issues": "이슈 설명"
        }}
      ],
      "online_channel_comparison": {{
        "own_mall": {{
          "sales": "매출",
          "growth_rate": "성장률",
          "direct_profit": "직접이익",
          "direct_profit_rate": "직접이익률",
          "commission_rate": "유통수수료율",
          "discount_rate": "할인율"
        }},
        "partner_mall": {{
          "sales": "매출",
          "growth_rate": "성장률",
          "direct_profit": "직접이익",
          "direct_profit_rate": "직접이익률",
          "commission_rate": "유통수수료율",
          "discount_rate": "할인율"
        }}
      }},
      "discount_rate_changes": [
        {{
          "channel": "채널명",
          "current": "당월 할인율 (%)",
          "previous": "전년 할인율 (%)",
          "change": "증감 (%p)"
        }}
      ]
    }},
    "product_inventory_analysis": {{
      "season_sales_rate": {{
        "current": "당시즌 판매율 (%)",
        "previous": "전년 판매율 (%)",
        "change": "증감 (%p)"
      }},
      "risk_items": [
        {{
          "item_name": "상품명",
          "purchase_amount": "발주 매출 (백만원)",
          "sales_amount": "판매 매출 (백만원)",
          "sales_rate": "판매율 (%)",
          "stock": "재고 (백만원)"
        }}
      ],
      "high_growth_items": [
        {{
          "item_name": "상품명",
          "sales": "매출 (백만원)",
          "yoy": "YoY (%)",
          "stock_weeks": "재고주수"
        }}
      ]
    }},
    "store_efficiency_analysis": {{
      "channel_store_status": [
        {{
          "channel": "채널명",
          "store_count": "매장수",
          "change": "증감",
          "sales_per_store": "점당매출",
          "yoy": "YoY (%)"
        }}
      ],
      "top_stores": [
        {{
          "channel": "채널명",
          "store_name": "매장명",
          "direct_profit": "직접이익 (백만원)",
          "yoy": "YoY (%)"
        }}
      ],
      "closing_review_stores": [
        {{
          "store_name": "매장명",
          "direct_profit": "직접이익 (백만원)",
          "sales_yoy": "매출 YoY (%)",
          "discount_rate_yoy": "할인율 YoY (%p)"
        }}
      ]
    }}
  }}
}}

**중요 지침:**
- 숫자는 핵심만 선별적으로 제시
- 각 섹션은 3-5개 항목으로 제한하여 간결하게 작성
- 인사이트 중심으로 작성
"""
    
    response = call_llm(prompt, max_tokens=4000)
    page2_data = parse_llm_json_response(response, "Detailed Analysis")
    
    return page2_data

def generate_page3_strategy_plan(yyyymm, brd_cd, analysis_data, metrics):
    """Page 3: Strategy & Action Plan 생성"""
    brand_name = BRAND_CODE_MAP.get(brd_cd, brd_cd)
    year = yyyymm[:4]
    month = yyyymm[4:6]
    
    # 분석 데이터를 안전하게 요약
    try:
        analysis_summary = json.dumps(analysis_data, ensure_ascii=False, indent=2, default=str)[:5000]
    except Exception as e:
        print(f"[WARNING] 분석 데이터 직렬화 실패: {e}")
        analysis_summary = str(analysis_data)[:5000]
    
    prompt = f"""
다음은 {brand_name} 브랜드 {year}년 {month}월 분석 데이터와 앞서 생성된 Executive Summary 및 Detailed Analysis를 바탕으로 전략 및 액션플랜을 작성해주세요.

**분석 데이터 요약:**
{analysis_summary}

다음 구조로 JSON 형식으로 응답해주세요:

{{
  "strategy_plan": {{
    "immediate_actions": [
      {{
        "category": "카테고리 (예: 재고 긴급 소진)",
        "actions": [
          "액션 1: 구체적인 실행 방안",
          "액션 2: 구체적인 실행 방안"
        ],
        "target": "목표 (예: 4주 내 5,000백만원)",
        "effect": "예상 효과"
      }}
    ],
    "short_term_actions": [
      {{
        "category": "카테고리 (예: 고성장 채널 투자 확대)",
        "actions": [
          "액션 1: 구체적인 실행 방안",
          "액션 2: 구체적인 실행 방안"
        ],
        "target": "목표",
        "effect": "예상 효과"
      }}
    ],
    "mid_long_term_strategy": [
      {{
        "category": "카테고리 (예: 채널 포트폴리오 재편)",
        "strategies": [
          "전략 1: 구체적인 방향",
          "전략 2: 구체적인 방향"
        ]
      }}
    ],
    "kpi_targets": {{
      "operating_profit_rate": {{
        "current": "현재 (%)",
        "target": "목표 (%)",
        "gap": "Gap (%p)"
      }},
      "discount_rate": {{
        "current": "현재 (%)",
        "target": "목표 (%)",
        "gap": "Gap (%p)"
      }},
      "season_sales_rate": {{
        "current": "현재 (%)",
        "target": "목표 (%)",
        "gap": "Gap (%p)"
      }},
      "own_mall_ratio": {{
        "current": "현재 (%)",
        "target": "목표 (%)",
        "gap": "Gap (%p)"
      }}
    }},
    "key_message": "경영진에게 전달할 핵심 메시지를 3-5줄로 요약"
  }}
}}

**중요 지침:**
- 즉시 실행 항목은 1개월 내 실행 가능한 구체적인 액션으로 작성
- 단기 실행 항목은 1분기 내 실행 가능한 액션으로 작성
- 중장기 전략은 상반기 목표로 작성
- KPI 목표는 현실적이고 달성 가능한 수치로 설정
- Key Message는 대표님이 가장 중요하게 봐야 할 내용을 간결하게 요약
"""
    
    response = call_llm(prompt, max_tokens=4000)
    page3_data = parse_llm_json_response(response, "Strategy & Action Plan")
    
    return page3_data

# ============================================================================
# HTML 생성
# ============================================================================
def generate_html_report(report_data):
    """HTML 형식의 보고서 생성"""
    brand_name = report_data['brand_name']
    yyyymm = report_data['yyyymm']
    year = yyyymm[:4]
    month = yyyymm[4:6]
    report_date = datetime.now().strftime('%Y년 %m월')
    
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{brand_name} 브랜드 {year}년 {month}월 실적 보고서</title>
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700;900&display=swap" rel="stylesheet">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700;900&display=swap');
        
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Noto Sans KR', 'Malgun Gothic', '맑은 고딕', sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            line-height: 1.6;
            -webkit-font-smoothing: antialiased;
        }}
        
        .page {{
            background: linear-gradient(to bottom, #ffffff 0%, #f8f9fa 100%);
            width: 210mm;
            min-height: 297mm;
            margin: 0 auto 20px;
            padding: 25mm;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            page-break-after: always;
            position: relative;
            border-radius: 8px;
            overflow: hidden;
        }}
        
        .page::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 6px;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
        }}
        
        .page:last-child {{
            page-break-after: auto;
        }}
        
        .header {{
            border-bottom: 4px solid;
            border-image: linear-gradient(90deg, #667eea, #764ba2) 1;
            padding-bottom: 20px;
            margin-bottom: 35px;
            display: flex;
            justify-content: space-between;
            align-items: flex-end;
            position: relative;
        }}
        
        .header-left h1 {{
            font-size: 32px;
            font-weight: 900;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 8px;
            letter-spacing: -0.5px;
        }}
        
        .header-left .subtitle {{
            font-size: 13px;
            color: #6c757d;
            font-weight: 400;
            letter-spacing: 0.5px;
        }}
        
        .header-right {{
            text-align: right;
            font-size: 11px;
            color: #adb5bd;
            font-weight: 300;
        }}
        
        .section {{
            margin-bottom: 35px;
        }}
        
        .section-title {{
            font-size: 22px;
            font-weight: 700;
            color: #2d3748;
            margin-bottom: 20px;
            padding-left: 15px;
            position: relative;
            letter-spacing: -0.3px;
        }}
        
        .section-title::before {{
            content: '';
            position: absolute;
            left: 0;
            top: 50%;
            transform: translateY(-50%);
            width: 5px;
            height: 28px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 3px;
        }}
        
        .section-subtitle {{
            font-size: 17px;
            font-weight: 600;
            color: #4a5568;
            margin: 25px 0 15px 0;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        
        .section-subtitle::before {{
            content: '▶';
            color: #667eea;
            font-size: 14px;
        }}
        
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 18px;
            margin-bottom: 25px;
        }}
        
        .metric-card {{
            background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
            padding: 20px;
            border-radius: 12px;
            border: 1px solid #e2e8f0;
            box-shadow: 0 4px 6px rgba(0,0,0,0.07);
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }}
        
        .metric-card::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 4px;
            height: 100%;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }}
        
        .metric-card.primary {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
        }}
        
        .metric-card.primary .metric-label,
        .metric-card.primary .metric-value {{
            color: white;
        }}
        
        .metric-label {{
            font-size: 11px;
            color: #718096;
            margin-bottom: 8px;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        .metric-value {{
            font-size: 28px;
            font-weight: 700;
            color: #2d3748;
            margin-bottom: 5px;
            letter-spacing: -0.5px;
        }}
        
        .metric-change {{
            font-size: 13px;
            font-weight: 600;
            margin-top: 8px;
            padding: 4px 8px;
            border-radius: 6px;
            display: inline-block;
        }}
        
        .metric-change.positive {{
            background: #c6f6d5;
            color: #22543d;
        }}
        
        .metric-change.negative {{
            background: #fed7d7;
            color: #742a2a;
        }}
        
        .risk-list, .signal-list {{
            list-style: none;
            padding: 0;
        }}
        
        .risk-list li {{
            background: linear-gradient(135deg, #fff5e6 0%, #ffe8cc 100%);
            padding: 14px 16px;
            margin-bottom: 10px;
            border-left: 4px solid #f59e0b;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(245, 158, 11, 0.1);
            font-size: 13px;
            color: #78350f;
            font-weight: 500;
            position: relative;
            padding-left: 40px;
        }}
        
        .risk-list li::before {{
            content: '⚠';
            position: absolute;
            left: 14px;
            font-size: 18px;
        }}
        
        .signal-list li {{
            background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%);
            padding: 14px 16px;
            margin-bottom: 10px;
            border-left: 4px solid #10b981;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(16, 185, 129, 0.1);
            font-size: 13px;
            color: #065f46;
            font-weight: 500;
            position: relative;
            padding-left: 40px;
        }}
        
        .signal-list li::before {{
            content: '✓';
            position: absolute;
            left: 14px;
            font-size: 18px;
            font-weight: bold;
        }}
        
        .table {{
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            margin-bottom: 25px;
            font-size: 13px;
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }}
        
        .table th {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 14px 12px;
            text-align: center;
            font-weight: 600;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            border: none;
        }}
        
        .table th:first-child {{
            text-align: left;
            padding-left: 18px;
        }}
        
        .table td {{
            padding: 12px;
            text-align: right;
            border-bottom: 1px solid #e2e8f0;
            color: #4a5568;
            font-weight: 500;
        }}
        
        .table td:first-child {{
            text-align: left;
            padding-left: 18px;
            font-weight: 600;
            color: #2d3748;
        }}
        
        .table tr:last-child td {{
            border-bottom: none;
        }}
        
        .table tr:hover {{
            background: #f7fafc;
        }}
        
        .table .positive {{
            color: #059669;
            font-weight: 700;
        }}
        
        .table .negative {{
            color: #dc2626;
            font-weight: 700;
        }}
        
        .action-item {{
            background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
            padding: 20px;
            margin-bottom: 18px;
            border-left: 5px solid;
            border-image: linear-gradient(135deg, #667eea, #764ba2) 1;
            border-radius: 10px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        }}
        
        .action-title {{
            font-weight: 700;
            margin-bottom: 12px;
            color: #2d3748;
            font-size: 16px;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        
        .action-title::before {{
            content: '⚡';
            font-size: 20px;
        }}
        
        .action-list {{
            list-style: none;
            padding-left: 0;
            margin: 12px 0;
        }}
        
        .action-list li {{
            padding: 8px 0;
            padding-left: 28px;
            position: relative;
            color: #4a5568;
            font-size: 13px;
            line-height: 1.6;
        }}
        
        .action-list li::before {{
            content: '▸';
            position: absolute;
            left: 8px;
            color: #667eea;
            font-weight: bold;
            font-size: 16px;
        }}
        
        .key-message {{
            background: linear-gradient(135deg, #e0e7ff 0%, #c7d2fe 100%);
            padding: 25px;
            border-left: 5px solid #667eea;
            border-radius: 10px;
            margin-top: 25px;
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.15);
        }}
        
        .key-message h3 {{
            color: #4338ca;
            font-weight: 700;
            margin-bottom: 12px;
            font-size: 16px;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        
        .key-message h3::before {{
            content: '💡';
            font-size: 20px;
        }}
        
        .key-message p {{
            color: #3730a3;
            font-size: 13px;
            line-height: 1.8;
            font-weight: 500;
        }}
        
        .kpi-table {{
            width: 100%;
            margin-top: 20px;
            border-collapse: separate;
            border-spacing: 0;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }}
        
        .kpi-table th {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 14px;
            text-align: center;
            font-weight: 600;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        .kpi-table td {{
            padding: 12px;
            text-align: center;
            border-bottom: 1px solid #e2e8f0;
            color: #4a5568;
            font-weight: 500;
        }}
        
        .kpi-table tr:last-child td {{
            border-bottom: none;
        }}
        
        .kpi-table tr:nth-child(even) {{
            background: #f8f9fa;
        }}
        
        .page-footer {{
            position: absolute;
            bottom: 20mm;
            right: 25mm;
            font-size: 10px;
            color: #adb5bd;
            font-weight: 300;
        }}
        
        .highlight-box {{
            background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
            padding: 15px 18px;
            border-radius: 10px;
            border-left: 4px solid #f59e0b;
            margin: 15px 0;
            box-shadow: 0 2px 8px rgba(245, 158, 11, 0.15);
        }}
        
        .highlight-box p {{
            color: #78350f;
            font-size: 12px;
            font-weight: 600;
            margin: 0;
            line-height: 1.6;
        }}
        
        @media print {{
            body {{
                background: white;
                padding: 0;
            }}
            .page {{
                margin: 0;
                box-shadow: none;
                border-radius: 0;
            }}
            .page::before {{
                display: none;
            }}
        }}
    </style>
</head>
<body>
"""
    
    # Page 1: Executive Summary
    page1 = report_data.get('page1', {}).get('executive_summary', {})
    html += f"""
    <div class="page">
        <div class="header">
            <div class="header-left">
                <h1>{page1.get('title', f'{brand_name} 브랜드 {year}년 {month}월 실적 보고서')}</h1>
                <div class="subtitle">{page1.get('subtitle', '경영관리팀 FP&A | 보고일: ' + report_date)}</div>
            </div>
            <div class="header-right">
                {brand_name} 브랜드 {year}년 {month}월 실적 보고서 | Page 1/3
            </div>
        </div>
        
        <div class="section">
            <div class="section-title">📊 핵심 경영 지표</div>
            <div class="metrics-grid">
"""
    
    key_metrics = page1.get('key_metrics', {})
    metric_keys = list(key_metrics.keys())
    for idx, key in enumerate(metric_keys):
        value = key_metrics.get(key, '')
        if value:
            # 주요 지표는 primary 스타일 적용
            is_primary = idx < 2  # 처음 2개는 주요 지표
            card_class = 'metric-card primary' if is_primary else 'metric-card'
            
            # 값에서 변화 추출 (예: +1.6%p, -3.1%p)
            change_class = 'positive'
            if isinstance(value, str):
                if '-' in value or '감소' in value or '하락' in value:
                    change_class = 'negative'
                elif '+' in value or '증가' in value or '상승' in value:
                    change_class = 'positive'
            
            html += f"""
                <div class="{card_class}">
                    <div class="metric-label">{key.replace('_', ' ').title()}</div>
                    <div class="metric-value">{value}</div>
                </div>
"""
    
    html += """
            </div>
        </div>
        
        <div class="section">
            <div class="section-title">🔴 Critical Risk</div>
            <ul class="risk-list">
"""
    
    for risk in page1.get('critical_risks', []):
        html += f"<li>{risk}</li>"
    
    html += """
            </ul>
        </div>
        
        <div class="section">
            <div class="section-title">🟢 Positive Signal</div>
            <ul class="signal-list">
"""
    
    for signal in page1.get('positive_signals', []):
        html += f"<li>{signal}</li>"
    
    html += f"""
            </ul>
        </div>
        
        <div class="section">
            <div class="section-title">💡 {month}월 Key Point</div>
            <p>{page1.get('key_points', '')}</p>
        </div>
    </div>
"""
    
    # Page 2: Detailed Analysis
    page2 = report_data.get('page2', {}).get('detailed_analysis', {})
    html += f"""
    <div class="page">
        <div class="header">
            <div class="header-left">
                <h1>{brand_name} 브랜드 {year}년 {month}월 실적 보고서</h1>
                <div class="subtitle">경영관리팀 FP&A | 보고일: {report_date} | 상세 분석</div>
            </div>
            <div class="header-right">
                {brand_name} 브랜드 {year}년 {month}월 실적 보고서 | Page 2/3
            </div>
        </div>
        
        <div class="section">
            <div class="section-title">2. 채널별 상세 분석</div>
"""
    
    channel_analysis = page2.get('channel_analysis', {})
    if channel_analysis.get('high_growth_channels'):
        html += """
            <h3>🚀 고성장 채널</h3>
            <table class="table">
                <thead>
                    <tr>
                        <th>채널</th>
                        <th>매출</th>
                        <th>YoY</th>
                        <th>직접이익률</th>
                    </tr>
                </thead>
                <tbody>
"""
        for channel in channel_analysis['high_growth_channels']:
            yoy = channel.get('yoy', '')
            yoy_class = 'positive' if isinstance(yoy, str) and ('+' in yoy or int(yoy.replace('%', '').replace('+', '').replace('-', '')) > 100 if yoy.replace('%', '').replace('+', '').replace('-', '').isdigit() else False) else 'negative' if isinstance(yoy, str) and '-' in yoy else ''
            html += f"""
                    <tr>
                        <td>{channel.get('channel', '')}</td>
                        <td>{channel.get('sales', '')}</td>
                        <td class="{yoy_class}">{yoy}</td>
                        <td>{channel.get('profit_rate', '')}</td>
                    </tr>
"""
        html += """
                </tbody>
            </table>
"""
    
    html += """
        </div>
        
        <div class="section">
            <div class="section-title">3. 상품/재고 분석</div>
"""
    
    product_analysis = page2.get('product_inventory_analysis', {})
    if product_analysis:
        html += "<p>상품 및 재고 분석 내용...</p>"
    
    html += """
        </div>
        
        <div class="section">
            <div class="section-title">4. 매장 효율성 분석</div>
"""
    
    store_analysis = page2.get('store_efficiency_analysis', {})
    if store_analysis:
        html += "<p>매장 효율성 분석 내용...</p>"
    
    html += """
        </div>
    </div>
"""
    
    # Page 3: Strategy & Action Plan
    page3 = report_data.get('page3', {}).get('strategy_plan', {})
    html += f"""
    <div class="page">
        <div class="header">
            <div class="header-left">
                <h1>{brand_name} 브랜드 {year}년 {month}월 실적 보고서</h1>
                <div class="subtitle">경영관리팀 FP&A | 보고일: {report_date} | 전략 & Action Plan</div>
            </div>
            <div class="header-right">
                {brand_name} 브랜드 {year}년 {month}월 실적 보고서 | Page 3/3
            </div>
        </div>
        
        <div class="section">
            <div class="section-title">5. 전략 & Action Plan</div>
"""
    
    if page3.get('immediate_actions'):
        html += """
            <h3>⚡ 즉시 실행 (1개월 내)</h3>
"""
        for action in page3['immediate_actions']:
            html += f"""
            <div class="action-item">
                <div class="action-title">{action.get('category', '')}</div>
                <ul class="action-list">
"""
            for act in action.get('actions', []):
                html += f"<li>{act}</li>"
            html += """
                </ul>
                <p><strong>목표:</strong> """ + action.get('target', '') + """</p>
                <p><strong>효과:</strong> """ + action.get('effect', '') + """</p>
            </div>
"""
    
    if page3.get('key_message'):
        html += f"""
        <div class="key-message">
            <h3>📌 경영진 핵심 메시지</h3>
            <p>{page3['key_message']}</p>
        </div>
"""
    
    html += """
        </div>
    </div>
</body>
</html>
"""
    
    return html

# ============================================================================
# 메인 실행
# ============================================================================
if __name__ == '__main__':
    reset_token_counter()
    
    # 분석 기간 및 브랜드 설정
    yyyymm = '202512'  # 분석할 연월
    brd_cd = 'M'  # 브랜드 코드 (M: MLB, I: MLB KIDS, X: DISCOVERY, V: DUVETICA, ST: SERGIO TACCHINI, W: SUPRA)
    
    # 여러 브랜드 분석 시
    # brands = ['M', 'I', 'X', 'V', 'ST', 'W']
    # for brd_cd in brands:
    #     generate_executive_report(yyyymm, brd_cd)
    
    try:
        report = generate_executive_report(yyyymm, brd_cd)
        
        # 토큰 사용량 출력
        total_tokens = get_total_tokens()
        print(f"\n{'='*60}")
        print(f"토큰 사용량")
        print(f"{'='*60}")
        print(f"입력 토큰: {total_tokens['input']:,} 토큰")
        print(f"출력 토큰: {total_tokens['output']:,} 토큰")
        print(f"총 토큰: {total_tokens['input'] + total_tokens['output']:,} 토큰")
        print(f"{'='*60}\n")
        
        if report:
            print(f"[SUCCESS] 보고서 생성 완료!")
            print(f"  - JSON: {OUTPUT_REPORT_PATH}/KR_{yyyymm[2:]}_{brd_cd}_대표님보고서_3페이지.json")
            print(f"  - HTML: {OUTPUT_REPORT_PATH}/KR_{yyyymm[2:]}_{brd_cd}_대표님보고서_3페이지.html")
            if PDF_AVAILABLE:
                print(f"  - PDF: {OUTPUT_REPORT_PATH}/KR_{yyyymm[2:]}_{brd_cd}_대표님보고서_3페이지.pdf")
            else:
                print(f"  - PDF: weasyprint 설치 후 자동 생성됩니다 (또는 HTML을 브라우저에서 PDF로 저장)")
    except Exception as e:
        print(f"[ERROR] 보고서 생성 실패: {e}")
        import traceback
        traceback.print_exc()
