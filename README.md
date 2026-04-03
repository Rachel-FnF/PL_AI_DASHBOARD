# PL AI Dashboard

F&F 그룹 브랜드별 경영실적(P&L) 데이터를 Snowflake에서 조회하고, Claude AI(LLM)를 활용하여 자동 분석 리포트를 생성하는 도구입니다.

## 개요

Snowflake 데이터웨어하우스에 적재된 매출/손익/재고 데이터를 SQL로 조회한 뒤, Claude API에 전달하여 경영관리팀 대상의 전략적 분석 리포트를 자동 생성합니다. 분석 결과는 Markdown(보고용)과 JSON(데이터 활용) 두 가지 형태로 저장됩니다.

## 대상 법인 및 브랜드

| 분석 스크립트 | 법인 | 대상 브랜드 | 통화 단위 |
|---|---|---|---|
| `pl_analysis.py` | 한국(KR) | MLB, MLB KIDS, DISCOVERY, DUVETICA, SERGIO TACCHINI, SUPRA | 백만원 |
| `cn_analysis.py` | 중국(CN) | MLB, MLB KIDS, DISCOVERY, DUVETICA, SUPRA | 백만원 |
| `hkmc_analysis.py` | 홍콩/마카오(HKMC) | MLB, DISCOVERY | K (천 HKD) |
| `tw_analysis.py` | 대만(TW) | MLB, MLB KIDS, DISCOVERY | K (천 HKD 환산) |

## 분석 항목

### 한국 (`pl_analysis.py`)
- 실판매출 채널별 매출분석 (당월/전년 비교)
- 성별 제품별 통합 분석 (남성/여성/공용)
- 영업이익 아이템별 직접이익
- 영업이익 매장별 직접이익
- 영업비 계정별 분석 (광고선전비, 인건비, 지급수수료 등)
- 할인율 종합분석
- 매장효율성 종합분석
- 월별 채널별 매출추세 (최근 12개월)
- 월별 아이템별 매출추세
- 월별 아이템별 재고추세

### 중국 (`cn_analysis.py`)
- 리테일매출 채널별 TOP3 분석
- 출고매출 카테고리별 분석
- 대리상/오프라인 점당매출 종합분석
- 할인율 종합분석
- 영업비 종합분석
- 월별 채널별/아이템별 매출추세
- 월별 아이템별 재고추세

### 홍콩/마카오 & 대만
한국/중국과 유사한 분석 항목을 각 법인 특성에 맞게 적용합니다.

## 프로젝트 구조

```
PL_AI_DASHBOARD/
├── pl_analysis.py        # 한국 법인 분석 (메인)
├── cn_analysis.py        # 중국 법인 분석
├── hkmc_analysis.py      # 홍콩/마카오 법인 분석
├── tw_analysis.py        # 대만 법인 분석
├── utils/                # 유틸리티 모듈
├── requirements.txt      # Python 의존성
├── .env                  # 환경 변수 (Snowflake 접속 정보, API 키)
├── kr_output/            # 한국 분석 결과
│   ├── json/             #   구조화된 JSON 데이터
│   ├── md/               #   Markdown 분석 리포트
│   └── reports/          #   종합 보고서
├── cn_output/            # 중국 분석 결과
│   ├── json/
│   └── md/
├── hkmc_output/          # 홍콩/마카오 분석 결과
│   ├── json/
│   └── md/
└── tw_output/            # 대만 분석 결과
    ├── json/
    └── md/
```

## 동작 방식

1. **데이터 조회** - Snowflake에 SQL 쿼리를 실행하여 매출/손익/재고 데이터를 Polars DataFrame으로 조회
2. **프롬프트 생성** - 조회된 데이터를 텍스트로 변환하고, 분석 가이드라인을 포함한 프롬프트 작성
3. **LLM 분석** - Claude API(`claude-sonnet-4`)를 호출하여 전략적 분석 리포트 생성
4. **결과 저장** - 분석 결과를 Markdown(보고용)과 JSON(데이터 활용) 파일로 저장

## 출력 파일 네이밍 규칙

```
{법인}_{YYMM}_{브랜드코드}_{분석항목}.{확장자}

예시:
KR_2602_M_실판매출_채널별매출분석.json
CN_2601_I_리테일매출_채널별매출분석.md
```

## 설치 및 실행

### 1. 의존성 설치

```bash
pip install -r requirements.txt
```

### 2. 환경 변수 설정

`.env` 파일에 다음 항목을 설정합니다:

```env
# Snowflake 접속 정보
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_AUTHENTICATOR=
SNOWFLAKE_DATABASE=
SNOWFLAKE_WAREHOUSE=

# Claude API 키
CLAUDE_API_KEY=
```

### 3. 분석 실행

각 법인별 스크립트를 직접 실행합니다. 스크립트 하단의 `__main__` 블록에서 분석 기간과 브랜드를 설정할 수 있습니다.

```bash
# 한국 법인 분석
python pl_analysis.py

# 중국 법인 분석
python cn_analysis.py

# 홍콩/마카오 법인 분석
python hkmc_analysis.py

# 대만 법인 분석
python tw_analysis.py
```

### 분석 기간 설정 (스크립트 내부)

```python
# 한 달만 분석
yyyymm_list = generate_yyyymm_list('202602')

# 여러 달 분석
yyyymm_list = generate_yyyymm_list('202401', '202510')

# 직접 지정
yyyymm_list = ['202509', '202510', '202511']
```

## 기술 스택

| 영역 | 기술 |
|---|---|
| 언어 | Python |
| 데이터베이스 | Snowflake |
| 데이터 처리 | Polars, Pandas |
| LLM | Anthropic Claude API (claude-sonnet-4) |
| DB 연결 | SQLAlchemy + snowflake-sqlalchemy |
| 환경 변수 | python-dotenv |
