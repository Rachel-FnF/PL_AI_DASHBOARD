# PL AI Dashboard

F&F 그룹의 중국(CN) 및 한국(PL) 시장 데이터를 AI로 분석하는 대시보드 도구입니다.

## 프로젝트 구조

- `cn_analysis.py`: 중국 시장 분석 (모든 기능 통합)
- `pl_analysis.py`: 한국 시장 분석 (모든 기능 통합)
- `cn_output/`: 중국 분석 결과 (JSON, Markdown)
- `kr_output/`: 한국 분석 결과 (JSON, Markdown)

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

### 중국 시장 분석 (cn_analysis.py)

1. `cn_analysis.py` 파일의 메인 함수에서 설정 변경:
```python
# 분석 기간 설정
yyyymm_list = generate_yyyymm_list('202509')  # 한 달만 분석
# yyyymm_list = generate_yyyymm_list('202501', '202510')  # 여러 달 분석

# 브랜드 선택
brands_to_analyze = [
    'M',   # MLB
    'I',   # MLB KIDS
    'X',   # DISCOVERY
    'V',   # DUVETICA
    'ST',  # SERGIO TACCHINI
    'W',   # SUPRA
]

# 분석 함수 선택 (주석 해제)
analyze_retail_channel_top3_sales(yyyymm, brd_cd)  # 리테일매출 채널별 매출분석
analyze_outbound_category_sales(yyyymm, brd_cd)  # 출고매출 카테고리별 분석
analyze_agent_store_sales(yyyymm, brd_cd)  # 오프라인 대리상 점당매출 분석
analyze_discount_rate(yyyymm, brd_cd)  # 할인율 종합분석
analyze_operating_expense(yyyymm, brd_cd)  # 영업비 종합분석
analyze_monthly_channel_sales_trend(yyyymm, brd_cd)  # 월별 채널별 매출 추세 분석
analyze_monthly_item_sales_trend(yyyymm, brd_cd)  # 월별 아이템별 매출 추세 분석
analyze_monthly_item_stock_trend(yyyymm, brd_cd)  # 월별 아이템별 재고 추세 분석
```

2. 실행:
```bash
python cn_analysis.py
```

3. 결과 확인:
- JSON: `cn_output/json/` 폴더
- Markdown: `cn_output/md/` 폴더

### 한국 시장 분석 (pl_analysis.py)

1. `pl_analysis.py` 파일의 메인 함수에서 설정 변경
2. 실행:
```bash
python pl_analysis.py
```

3. 결과 확인:
- JSON: `kr_output/json/` 폴더
- Markdown: `kr_output/md/` 폴더

## 주요 분석 함수

### 중국 시장 분석 (cn_analysis.py)

#### 1. 리테일매출 채널별 매출분석 (`analyze_retail_channel_top3_sales`)
- **인사이트**: 각 채널별 당해 당월 매출 베스트 아이템 3개를 전년대비 주요변화로 분석
- **Key**: `리테일`, **Sub_key**: `채널별매출분석`
- 채널별 TOP3 아이템 분석 및 브랜드 전체 채널 종합분석

#### 2. 출고매출 카테고리별 분석 (`analyze_outbound_category_sales`)
- **인사이트**: ACC/의류 카테고리별 강세/약세 아이템 분석 및 전년대비 주요변화 도출
- **Key**: `출고매출`, **Sub_key**: `카테고리별매출분석`
- 카테고리별 수익성 평가 및 전략 최적화 방안 제시

#### 3. 오프라인 대리상 점당매출 분석 (`analyze_agent_store_sales`)
- **인사이트**: 오프라인 대리상의 월별 매출 추이를 당해/전년 비교로 분석하여, 우수 대리상의 성공 요인과 수익성 개선이 필요한 대리상의 원인을 도출하고, 대리상별 성과 차이의 원인과 개선 방안을 제시
- **Key**: `(대리상오프)점당매출`, **Sub_key**: `(대리상오프)점당매출 AI 분석`
- 분석 기간: 당해 1월~지정한 연월, 전년 1월~전년 동일 월
- 섹션: 우수 대리상, 수익성 개선 필요, 인사이트

#### 4. 할인율 종합분석 (`analyze_discount_rate`)
- **인사이트**: 채널별 할인율을 전년월 VS 당해월 비교 및 추세 분석하여 할인율 전략이 우수한 채널과 주의 필요 채널을 식별
- **Key**: `할인율`, **Sub_key**: `종합분석`
- 채널별 할인율 전략 평가 및 AI 권장사항 제시

#### 5. 영업비 종합분석 (`analyze_operating_expense`)
- **인사이트**: 전년/당해 동월 비교, 누적 YTD 비교, 1년 추세 분석, 법인 전체 대비 브랜드 비중 분석
- **Key**: `영업비`, **Sub_key**: `종합분석`
- 영업비 계정별(광고비, 인건비, 복리후생비 등) 분석 및 전략적 시사점 제시

#### 6. 월별 채널별 매출 추세 분석 (`analyze_monthly_channel_sales_trend`)
- **인사이트**: 당해 1월부터 지정한 연월까지의 채널별 매출 추이를 분석하여 월별 주요 인사이트, 채널 트렌드, 전략 포인트 도출
- **Key**: `월별채널별매출추세`
- 분석 기간: 함수 파라미터 `yyyymm`으로 지정한 연월까지 분석

#### 7. 월별 아이템별 매출 추세 분석 (`analyze_monthly_item_sales_trend`)
- **인사이트**: 시즌별 의류(당시즌, 전시즌, 차기시즌, 과시즌 등)와 카테고리별 ACC(모자, 신발, 가방, 기타)의 월별 매출 추이를 분석
- **Key**: `월별아이템별매출추세`
- 분석 기간: 당해 1월부터 지정한 연월까지
- 섹션: 시즌 트렌드, 카테고리, 핵심 액션

#### 8. 월별 아이템별 재고 추세 분석 (`analyze_monthly_item_stock_trend`)
- **인사이트**: 월별 아이템별 재고 추이를 분석하여 재고 위험 아이템을 조기 경보하고, 재고 최적화가 잘 되는 아이템의 성공 요인을 파악하며, 재고 관리 개선을 위한 실행 방안을 제시
- **Key**: `월별아이템별재고추세`
- 분석 기간: 당해 1월부터 지정한 연월까지
- 섹션: 조기경보, 긍정신호, 핵심액션

### 한국 시장 분석 (pl_analysis.py)

#### 1. 영업비 광고선전비 분석 (`analyze_operating_expense_by_ctgr1`)
- **인사이트**: 전년 동월 대비 당해 동월 광고선전비 변화를 계정별(CTGR2, CTGR3)로 분석하고, 12개월 추세를 반영하여 투자 효율성과 최적화 방안을 제시
- **Key**: `영업비`, **Sub_key**: `광고선전비` 등
- 섹션: 투자 방향성 종합 평가, 효율적 투자 영역, 주의 필요 영역, 이상징후 및 리스크 감지, 마케팅 전략 최적화 방안

## 브랜드 코드

- `M`: MLB
- `I`: MLB KIDS
- `X`: DISCOVERY
- `V`: DUVETICA
- `ST`: SERGIO TACCHINI
- `W`: SUPRA

## 주요 유틸리티 함수

- `get_db_engine()`: Snowflake DB 연결 엔진 생성
- `run_query(sql, engine)`: SQL 쿼리 실행하고 Polars DataFrame 반환
- `call_llm(prompt, max_tokens, temperature)`: Claude API 호출
- `save_markdown(content, filename)`: Markdown 파일 저장 (YAML frontmatter 포함)
- `save_json(data, filename)`: JSON 파일 저장 (필드 순서 보장)
- `generate_yyyymm_list(start_yyyymm, end_yyyymm)`: 년월 리스트 생성
- `json_dumps_safe(obj)`: Decimal 타입을 안전하게 처리하는 JSON 인코더

## 분석 기간 설정

모든 분석 함수는 `yyyymm` 파라미터를 받아서 분석 기간을 결정합니다:
- **월별 추세 분석**: 당해 1월부터 `yyyymm`으로 지정한 연월까지 분석
- **동월 비교 분석**: 전년 동월(`yyyymm_py`) vs 당해 동월(`yyyymm`) 비교

예시:
```python
analyze_monthly_channel_sales_trend('202503', 'M')  # 2025년 1월~3월 분석
analyze_agent_store_sales('202503', 'I')  # 2025년 1월~3월 vs 2024년 1월~3월 분석
```

## 출력 형식

모든 분석 결과는 다음 형식으로 저장됩니다:

### JSON 구조
```json
{
  "country": "CN" | "KR",
  "brand_cd": "M",
  "brand_name": "MLB",
  "yyyymm": "202509",
  "yyyymm_py": "202409",
  "key": "리테일",
  "sub_key": "채널별매출분석",
  "analysis_data": {
    "title": "...",
    "sections": [
      {
        "div": "종합분석-1",
        "sub_title": "...",
        "ai_text": "..."
      }
    ]
  },
  "summary": {...},
  "raw_data": {...}
}
```

### Markdown 구조
- YAML frontmatter 포함
- 각 섹션별로 제목과 AI 분석 내용 포함

## 주의사항

1. **API 크레딧**: Claude API 사용량에 따라 비용이 발생할 수 있습니다. `get_total_tokens()` 함수로 토큰 사용량을 확인할 수 있습니다.
2. **분석 기간**: 함수 파라미터 `yyyymm`을 통해 분석 종료 시점을 지정할 수 있습니다. 기본적으로 당해 1월부터 시작합니다.
3. **데이터 형식**: 모든 금액은 천 단위(k) 또는 백만원 단위로 표시됩니다. LLM 프롬프트에서 단위를 명확히 지정합니다.

## 문제 해결

### API 오류
- `balance is too low`: Anthropic API 계정 잔액이 부족합니다. [Anthropic Console](https://console.anthropic.com/)에서 충전하세요.

### JSON 파싱 오류
- LLM 응답이 마크다운 코드 블록으로 감싸져 있을 경우 자동으로 제거됩니다.
- 파싱 실패 시 기본 구조로 대체되어 저장됩니다.

## 라이선스

내부 사용 전용
