"""
KR 대표님 보고용 통합 요약 생성 도구
- 기존 분석 JSON 파일들을 읽어서 월별 브랜드별 통합 요약 생성
- 월별 전체 브랜드 통합 요약 생성
- A4 용지 절반 분량으로 요약
"""

import os
import json
import glob
import anthropic
from datetime import datetime
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

OUTPUT_JSON_PATH = './kr_output/json'
OUTPUT_MD_PATH = './kr_output/md'

# 출력 폴더 생성
os.makedirs(OUTPUT_JSON_PATH, exist_ok=True)
os.makedirs(OUTPUT_MD_PATH, exist_ok=True)

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
  - 잘못된 예: 1.2백만원, 0.5백만원, 1,234.5백만원

📝 **작성 원칙**
- 구체적이고 실용적인 내용으로 작성
- 불릿 포인트는 마크다운 형식(-, •) 사용 가능
- 줄바꿈은 반드시 \\n을 사용하여 표시
- ai_text 내에서 여러 문단이나 항목을 나눌 때는 \\n\\n을 사용
- 불릿 포인트나 리스트 항목 사이에는 \\n을 사용
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

def get_common_prompt_guidelines():
    """공통 프롬프트 가이드라인 텍스트 반환"""
    return """- 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
- 숫자는 백만원 단위로 표시하고 절대 변형하지 말 것
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
def extract_key_from_filename(filename):
    """
    파일명에서 KEY와 sub_key를 추출 (브랜드 코드 제외)
    
    파일명 형식: KR_{yyyymm_short}_{brd_cd}_{분석타입}_{세부분석}
    예시: KR_2401_I_실판매출_채널별매출분석
    
    Returns:
        tuple: (key, sub_key, country)
    """
    parts = filename.replace('.json', '').replace('.md', '').split('_')
    
    if len(parts) < 4:
        return None, None, 'KR'
    
    # KR_2401_I_실판매출_채널별매출분석
    # parts[0] = KR
    # parts[1] = 2401
    # parts[2] = I
    # parts[3:] = ['실판매출', '채널별매출분석']
    
    country = parts[0] if parts[0] in ['CN', 'KR'] else 'KR'
    
    if len(parts) >= 5:
        key = parts[3]
        sub_key = '_'.join(parts[4:])
    elif len(parts) == 4:
        key = parts[3]
        sub_key = ''
    else:
        key = None
        sub_key = None
    
    return key, sub_key, country

def save_json(data, filename):
    """JSON 파일 저장"""
    file_path = os.path.join(OUTPUT_JSON_PATH, f"{filename}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[OK] JSON 저장: {file_path}")
    return file_path

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

# ============================================================================
# 통합 요약 생성 함수
# ============================================================================
def load_json_files_for_month_brand(yyyymm, brd_cd):
    """
    특정 월, 특정 브랜드의 모든 JSON 파일을 로드하여 분석 데이터 수집
    
    Args:
        yyyymm: 년월 (예: '202401')
        brd_cd: 브랜드 코드 (예: 'I')
    
    Returns:
        list: 각 분석의 analysis_data와 메타데이터를 포함한 딕셔너리 리스트
    """
    yyyymm_short = yyyymm[2:]  # 202401 -> 2401
    pattern = os.path.join(OUTPUT_JSON_PATH, f"KR_{yyyymm_short}_{brd_cd}_*.json")
    
    json_files = glob.glob(pattern)
    analysis_results = []
    
    # 디버깅: 찾은 파일 목록 출력
    if not json_files:
        print(f"[DEBUG] 패턴 '{pattern}'에 해당하는 파일을 찾지 못했습니다.")
        # 전체 파일 목록 확인 (디버깅용)
        all_files = glob.glob(os.path.join(OUTPUT_JSON_PATH, f"KR_{yyyymm_short}_*.json"))
        if all_files:
            print(f"[DEBUG] 해당 월의 다른 파일들: {[os.path.basename(f) for f in all_files[:5]]}")
        else:
            print(f"[DEBUG] 해당 월({yyyymm_short})의 파일이 전혀 없습니다.")
    
    for json_file in json_files:
        # 통합 요약 파일은 제외
        if '월별브랜드통합요약' in json_file or '월별전체브랜드통합요약' in json_file:
            continue
            
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if 'analysis_data' in data and data['analysis_data']:
                analysis_results.append({
                    'filename': os.path.basename(json_file),
                    'key': data.get('key', ''),
                    'sub_key': data.get('sub_key', ''),
                    'analysis_data': data['analysis_data'],
                    'summary': data.get('summary', {})
                })
        except Exception as e:
            print(f"[WARNING] {json_file} 파일 읽기 실패: {e}")
            continue
    
    return analysis_results

def generate_monthly_brand_summary(yyyymm, brd_cd):
    """
    특정 월, 특정 브랜드의 모든 분석 결과를 통합하여 A4 절반 분량으로 요약
    
    Args:
        yyyymm: 년월 (예: '202401')
        brd_cd: 브랜드 코드 (예: 'I')
    
    Returns:
        dict: 통합 요약 JSON 데이터
    """
    print(f"\n{'='*60}")
    print(f"월별 브랜드별 통합 요약 생성 시작: {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({yyyymm})")
    print(f"{'='*60}")
    
    # 해당 월/브랜드의 모든 JSON 파일 로드
    analysis_results = load_json_files_for_month_brand(yyyymm, brd_cd)
    
    if not analysis_results:
        print(f"[WARNING] {yyyymm}년 {yyyymm[4:6]}월 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드의 분석 결과가 없습니다.")
        return None
    
    print(f"분석 결과 {len(analysis_results)}개 발견")
    
    # 모든 분석 데이터를 하나의 텍스트로 통합
    all_sections_text = []
    analysis_summary = []
    
    for result in analysis_results:
        key = result['key']
        sub_key = result['sub_key']
        analysis_data = result['analysis_data']
        summary = result.get('summary', {})
        
        # 분석 제목과 섹션들 수집
        title = analysis_data.get('title', f"{key} - {sub_key}")
        sections = analysis_data.get('sections', [])
        
        analysis_summary.append({
            'title': title,
            'key': key,
            'sub_key': sub_key,
            'section_count': len(sections)
        })
        
        # 각 섹션의 내용 수집
        for section in sections:
            sub_title = section.get('sub_title', '')
            ai_text = section.get('ai_text', '')
            if ai_text:
                all_sections_text.append(f"## {title} - {sub_title}\n{ai_text}\n")
    
    # 통합된 텍스트
    combined_text = "\n".join(all_sections_text)
    
    # 분석 항목 요약
    analysis_list = "\n".join([f"- {item['title']}" for item in analysis_summary])
    
    # LLM 프롬프트 생성 (A4 절반 분량 = 약 300-400단어)
    prompt = f"""
너는 F&F 그룹의 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 전략 분석 전문가야. {yyyymm[:4]}년 {yyyymm[4:6]}월의 모든 분석 결과를 종합하여 A4 용지 절반 분량(약 300-400단어)으로 요약해줘.

**분석 기간**: {yyyymm[:4]}년 {yyyymm[4:6]}월

**포함된 분석 항목**:
{analysis_list}

**전체 분석 내용**:
{combined_text[:8000]}  # 더 많은 컨텍스트 제공

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

{{
  "title": "{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 {yyyymm[:4]}년 {yyyymm[4:6]}월 종합 분석",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "전체 성과 요약",
      "ai_text": "해당 월의 전체 성과를 구체적으로 요약. 주요 매출액(백만원 단위), 전년대비 성장률, 주요 지표 변화 등을 구체적인 수치와 함께 제시. (3-4줄)"
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "주요 인사이트",
      "ai_text": "가장 중요한 인사이트 3-4개를 불릿 포인트 형식으로 제시. 각 인사이트는 구체적인 데이터와 수치를 포함하여 작성. (3-4줄)"
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "브랜드별 강점",
      "ai_text": "해당 브랜드의 강점과 우수한 영역을 구체적으로 분석. 성장한 카테고리, 우수한 채널, 효과적인 전략 등을 수치와 함께 제시. (3-4줄)"
    }},
    {{
      "div": "종합분석-4",
      "sub_title": "브랜드별 리스크",
      "ai_text": "해당 브랜드의 리스크 요소와 주의가 필요한 영역을 구체적으로 분석. 감소한 카테고리, 문제가 있는 채널, 개선이 필요한 영역 등을 수치와 함께 제시. (3-4줄)"
    }},
    {{
      "div": "종합분석-5",
      "sub_title": "전략 제안",
      "ai_text": "다음 달을 위한 구체적이고 실행 가능한 전략 제안 3-4개를 제시. 각 전략은 구체적인 액션 아이템과 예상 효과를 포함하여 작성. (3-4줄)"
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- **전체 성과 요약**: 주요 매출액, 전년대비 성장률, 주요 지표(할인율, 수익률, 채널별 매출 등)를 구체적인 수치와 함께 제시
- **주요 인사이트**: 모든 분석 항목을 종합하여 가장 중요한 발견사항 3-4개를 제시. 각 인사이트는 구체적인 데이터와 수치를 포함
- **브랜드별 강점**: 성장한 카테고리, 우수한 채널, 효과적인 제품/아이템, 성공 요인 등을 구체적인 수치와 함께 분석
- **브랜드별 리스크**: 감소한 카테고리, 문제가 있는 채널, 개선이 필요한 영역, 위험 신호 등을 구체적인 수치와 함께 분석
- **전략 제안**: 강점을 활용하고 리스크를 해소하기 위한 구체적이고 실행 가능한 전략 3-4개를 제시. 각 전략은 구체적인 액션 아이템과 예상 효과를 포함
- 전체 분량은 A4 용지 절반 분량(약 300-400단어)으로 작성하되, 각 섹션이 균형있게 배분되도록 작성
- 모든 수치는 백만원 단위로 표시하고 구체적인 수치를 반드시 포함
- 실행 가능한 전략을 제시

{get_common_prompt_footer()}
"""
    
    # LLM 호출
    analysis_response = call_llm(prompt, max_tokens=2000)
    
    # JSON 파싱
    analysis_data = parse_llm_json_response(analysis_response, f"{BRAND_CODE_MAP.get(brd_cd, brd_cd)} 브랜드 {yyyymm[:4]}년 {yyyymm[4:6]}월 종합 분석")
    
    # JSON 데이터 생성
    json_data = {
        'country': 'KR',
        'brand_cd': brd_cd,
        'brand_name': BRAND_CODE_MAP.get(brd_cd, brd_cd),
        'yyyymm': yyyymm,
        'key': '월별브랜드통합요약',
        'sub_key': '종합분석',
        'analysis_data': analysis_data,
        'summary': {
            'analysis_count': len(analysis_results),
            'analysis_items': analysis_summary,
            'analysis_period': f"{yyyymm[:4]}년 {yyyymm[4:6]}월"
        },
        'source_files': [r['filename'] for r in analysis_results]
    }
    
    # 파일 저장
    yyyymm_short = yyyymm[2:]
    filename = f"KR_{yyyymm_short}_{brd_cd}_월별브랜드통합요약"
    save_json(json_data, filename)
    
    # Markdown도 저장
    markdown_content = f"# {analysis_data.get('title', '월별 브랜드 통합 요약')}\n\n"
    for section in analysis_data.get('sections', []):
        markdown_content += f"## {section.get('sub_title', '')}\n\n"
        markdown_content += f"{section.get('ai_text', '')}\n\n"
    save_markdown(markdown_content, filename)
    
    print(f"[OK] 월별 브랜드별 통합 요약 완료!\n")
    return json_data

def generate_monthly_all_brands_summary(yyyymm):
    """
    특정 월의 모든 브랜드 분석 결과를 통합하여 A4 절반 분량으로 요약
    
    Args:
        yyyymm: 년월 (예: '202401')
    
    Returns:
        dict: 통합 요약 JSON 데이터
    """
    print(f"\n{'='*60}")
    print(f"월별 전체 브랜드 통합 요약 생성 시작: {yyyymm}")
    print(f"{'='*60}")
    
    yyyymm_short = yyyymm[2:]
    pattern = os.path.join(OUTPUT_JSON_PATH, f"KR_{yyyymm_short}_*_월별브랜드통합요약.json")
    
    brand_summary_files = glob.glob(pattern)
    
    if not brand_summary_files:
        print(f"[WARNING] {yyyymm}년 {yyyymm[4:6]}월의 브랜드별 통합 요약 파일이 없습니다.")
        print(f"[INFO] 먼저 각 브랜드별 통합 요약을 생성해주세요.")
        return None
    
    print(f"브랜드별 통합 요약 파일 {len(brand_summary_files)}개 발견")
    
    # 모든 브랜드 통합 요약 데이터 수집
    all_brand_summaries = []
    
    for json_file in brand_summary_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if 'analysis_data' in data and data['analysis_data']:
                brand_name = data.get('brand_name', data.get('brand_cd', ''))
                analysis_data = data['analysis_data']
                
                all_brand_summaries.append({
                    'brand_name': brand_name,
                    'brand_cd': data.get('brand_cd', ''),
                    'analysis_data': analysis_data
                })
        except Exception as e:
            print(f"[WARNING] {json_file} 파일 읽기 실패: {e}")
            continue
    
    if not all_brand_summaries:
        print(f"[WARNING] 유효한 브랜드 통합 요약 데이터가 없습니다.")
        return None
    
    # 모든 브랜드의 분석 내용을 통합
    combined_text = []
    brand_list = []
    
    for brand_data in all_brand_summaries:
        brand_name = brand_data['brand_name']
        analysis_data = brand_data['analysis_data']
        title = analysis_data.get('title', f"{brand_name} 종합 분석")
        sections = analysis_data.get('sections', [])
        
        brand_list.append(brand_name)
        
        for section in sections:
            sub_title = section.get('sub_title', '')
            ai_text = section.get('ai_text', '')
            if ai_text:
                combined_text.append(f"## {brand_name} - {sub_title}\n{ai_text}\n")
    
    combined_text_str = "\n".join(combined_text)
    
    # LLM 프롬프트 생성
    prompt = f"""
너는 F&F 그룹의 전략 분석 전문가야. {yyyymm[:4]}년 {yyyymm[4:6]}월의 모든 브랜드 분석 결과를 종합하여 A4 용지 절반 분량(약 300-400단어)으로 요약해줘.

**분석 기간**: {yyyymm[:4]}년 {yyyymm[4:6]}월

**포함된 브랜드**: {', '.join(brand_list)}

**전체 브랜드 분석 내용**:
{combined_text_str[:8000]}  # 더 많은 컨텍스트 제공

<요구사항>
아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

{{
  "title": "전체 브랜드 {yyyymm[:4]}년 {yyyymm[4:6]}월 종합 분석",
  "sections": [
    {{
      "div": "종합분석-1",
      "sub_title": "전체 성과 요약",
      "ai_text": "모든 브랜드를 통합한 전체 성과를 구체적으로 요약. 전체 매출액(백만원 단위), 전년대비 성장률, 브랜드별 기여도, 주요 지표 변화 등을 구체적인 수치와 함께 제시. (3-4줄)"
    }},
    {{
      "div": "종합분석-2",
      "sub_title": "브랜드별 주요 인사이트",
      "ai_text": "각 브랜드의 주요 인사이트를 브랜드별로 요약하여 제시. 각 브랜드의 핵심 성과, 특징적인 변화, 주목할 만한 사항 등을 구체적인 수치와 함께 작성. (4-5줄)"
    }},
    {{
      "div": "종합분석-3",
      "sub_title": "브랜드별 강점",
      "ai_text": "각 브랜드의 강점과 우수한 영역을 브랜드별로 분석. 성장한 브랜드, 우수한 성과를 보인 브랜드, 성공 요인 등을 구체적인 수치와 함께 제시. (3-4줄)"
    }},
    {{
      "div": "종합분석-4",
      "sub_title": "브랜드별 리스크",
      "ai_text": "각 브랜드의 리스크 요소와 주의가 필요한 영역을 브랜드별로 분석. 감소한 브랜드, 문제가 있는 영역, 개선이 필요한 브랜드 등을 구체적인 수치와 함께 제시. (3-4줄)"
    }},
    {{
      "div": "종합분석-5",
      "sub_title": "전략 제안",
      "ai_text": "모든 브랜드를 고려한 통합 전략 제안 3-4개를 제시. 브랜드 간 시너지, 리소스 배분, 우선순위 설정 등 구체적이고 실행 가능한 전략을 제시. 각 전략은 구체적인 액션 아이템과 예상 효과를 포함. (3-4줄)"
    }}
  ]
}}

<작성 가이드라인>
{get_common_prompt_guidelines()}
- **전체 성과 요약**: 모든 브랜드를 통합한 전체 매출액, 전년대비 성장률, 브랜드별 기여도, 주요 지표(할인율, 수익률 등)를 구체적인 수치와 함께 제시
- **브랜드별 주요 인사이트**: 각 브랜드의 핵심 성과와 특징적인 변화를 브랜드명과 함께 구체적으로 제시. 각 브랜드의 고유한 특성과 성과를 명확히 구분하여 작성
- **브랜드별 강점**: 성장한 브랜드, 우수한 성과를 보인 브랜드, 성공 요인을 브랜드명과 함께 구체적인 수치로 제시. 브랜드 간 비교 관점도 포함
- **브랜드별 리스크**: 감소한 브랜드, 문제가 있는 영역, 개선이 필요한 브랜드를 브랜드명과 함께 구체적인 수치로 제시. 브랜드 간 비교 관점도 포함
- **전략 제안**: 모든 브랜드를 고려한 통합 전략 제안. 브랜드 간 시너지 창출, 리소스 배분 최적화, 우선순위 설정 등 구체적이고 실행 가능한 전략을 제시. 각 전략은 구체적인 액션 아이템과 예상 효과를 포함
- 전체 분량은 A4 용지 절반 분량(약 300-400단어)으로 작성하되, 각 섹션이 균형있게 배분되도록 작성
- 모든 수치는 백만원 단위로 표시하고 구체적인 수치를 반드시 포함
- 브랜드 간 비교와 통합 관점을 제시
- 실행 가능한 전략을 제시

{get_common_prompt_footer()}
"""
    
    # LLM 호출
    analysis_response = call_llm(prompt, max_tokens=2000)
    
    # JSON 파싱
    analysis_data = parse_llm_json_response(analysis_response, f"전체 브랜드 {yyyymm[:4]}년 {yyyymm[4:6]}월 종합 분석")
    
    # JSON 데이터 생성
    json_data = {
        'country': 'KR',
        'yyyymm': yyyymm,
        'key': '월별전체브랜드통합요약',
        'sub_key': '종합분석',
        'analysis_data': analysis_data,
        'summary': {
            'brand_count': len(all_brand_summaries),
            'brands': brand_list,
            'analysis_period': f"{yyyymm[:4]}년 {yyyymm[4:6]}월"
        },
        'source_brands': [b['brand_cd'] for b in all_brand_summaries]
    }
    
    # 파일 저장
    yyyymm_short = yyyymm[2:]
    filename = f"KR_{yyyymm_short}_ALL_월별전체브랜드통합요약"
    save_json(json_data, filename)
    
    # Markdown도 저장
    markdown_content = f"# {analysis_data.get('title', '월별 전체 브랜드 통합 요약')}\n\n"
    for section in analysis_data.get('sections', []):
        markdown_content += f"## {section.get('sub_title', '')}\n\n"
        markdown_content += f"{section.get('ai_text', '')}\n\n"
    save_markdown(markdown_content, filename)
    
    print(f"[OK] 월별 전체 브랜드 통합 요약 완료!\n")
    return json_data

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
    print(f"KR 대표님 보고용 통합 요약 생성 시작")
    print(f"시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # 토큰 카운터 초기화
    reset_token_counter()
    
    # ========================================================================
    # 분석 기간 설정 (아래 3가지 방법 중 하나를 선택하세요)
    # ========================================================================
    
    # 방법 1: 한 달만 분석 (예: 2024년 1월만)
    yyyymm_list = generate_yyyymm_list('202512')
    
    # 방법 2: 여러 달 분석 (시작월 ~ 종료월)
    # 예: 2024년 1월부터 12월까지
    # yyyymm_list = generate_yyyymm_list('202401', '202412')
    
    # 방법 3: 직접 리스트 지정 (원하는 월만 선택)
    # 예: 2024년 9월, 10월, 11월, 12월만
    # yyyymm_list = ['202409', '202410', '202411', '202412']
    
    if len(yyyymm_list) == 1:
        print(f"요약 생성할 기간: {len(yyyymm_list)}개월 ({yyyymm_list[0]})")
    else:
        print(f"요약 생성할 기간: {len(yyyymm_list)}개월 ({yyyymm_list[0]} ~ {yyyymm_list[-1]})")
    
    # 브랜드 선택 (원하는 브랜드만 주석 해제)
    brands_to_analyze = [
        'M',   # MLB
        'I',   # MLB KIDS
        'X',   # DISCOVERY
        'V',   # DUVETICA
        'ST',  # SERGIO TACCHINI
        'W',   # SUPRA
    ]
    
    # 기간별, 브랜드별 통합 요약 생성
    for yyyymm in yyyymm_list:
        print(f"\n{'='*60}")
        print(f"기간 요약 생성 시작: {yyyymm} ({yyyymm[:4]}년 {yyyymm[4:6]}월)")
        print(f"{'='*60}\n")
        
        # 각 브랜드별 통합 요약 생성
        print(f"\n{'='*60}")
        print(f"브랜드별 통합 요약 생성 시작: {yyyymm}")
        print(f"{'='*60}\n")
        
        brand_summary_success_count = 0
        for brd_cd in brands_to_analyze:
            try:
                result = generate_monthly_brand_summary(yyyymm, brd_cd)
                if result is not None:
                    brand_summary_success_count += 1
                    print(f"[OK] 브랜드 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({brd_cd}) 통합 요약 생성 완료")
                else:
                    print(f"[SKIP] 브랜드 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({brd_cd}) 통합 요약 생성 건너뜀 (분석 결과 없음)")
            except Exception as e:
                print(f"[ERROR] 브랜드 {BRAND_CODE_MAP.get(brd_cd, brd_cd)} ({brd_cd}) 통합 요약 생성 중 오류 발생: {e}")
                print(f"[ERROR] 다음 브랜드로 계속 진행합니다...\n")
                continue
        
        print(f"\n[INFO] 브랜드별 통합 요약 생성 완료: {brand_summary_success_count}/{len(brands_to_analyze)}개 성공")
        
        # 전체 브랜드 통합 요약 생성 (브랜드별 요약이 하나라도 성공했을 때만)
        if brand_summary_success_count > 0:
            print(f"\n{'='*60}")
            print(f"전체 브랜드 통합 요약 생성 시작: {yyyymm}")
            print(f"{'='*60}\n")
            
            try:
                generate_monthly_all_brands_summary(yyyymm)
            except Exception as e:
                print(f"[ERROR] 전체 브랜드 통합 요약 생성 중 오류 발생: {e}")
                print(f"[ERROR] 다음 월로 계속 진행합니다...\n")
        else:
            print(f"\n[WARNING] 브랜드별 통합 요약이 하나도 생성되지 않아 전체 브랜드 통합 요약을 건너뜁니다.")
            print(f"[INFO] 먼저 pl_analysis.py를 실행하여 해당 월의 분석 결과를 생성해주세요.\n")
    
    # 종료 시간 기록
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    
    # 토큰 사용량 조회
    total_tokens = get_total_tokens()
    total_token_count = total_tokens['input'] + total_tokens['output']
    
    print(f"\n{'='*60}")
    print(f"전체 통합 요약 생성 완료!")
    print(f"소요 시간: {elapsed_time}")
    print(f"총 토큰 사용량: {total_token_count:,} 토큰 (입력: {total_tokens['input']:,}, 출력: {total_tokens['output']:,})")
    print(f"{'='*60}\n")
