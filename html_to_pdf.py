"""
HTML 파일을 PDF로 변환하는 스크립트
기존에 생성된 HTML 보고서를 PDF로 변환합니다.

사용 가능한 방법:
1. playwright (권장) - pip install playwright && playwright install chromium
2. weasyprint - pip install weasyprint (Windows에서 설치 어려울 수 있음)
"""

import os
import sys

# PDF 생성 라이브러리 확인 (여러 방법 시도)
PDF_METHOD = None

# 방법 1: playwright 시도
try:
    from playwright.sync_api import sync_playwright
    PDF_METHOD = 'playwright'
    print("[INFO] playwright를 사용하여 PDF를 생성합니다.")
except ImportError:
    pass

# 방법 2: weasyprint 시도
if PDF_METHOD is None:
    try:
        from weasyprint import HTML
        PDF_METHOD = 'weasyprint'
        print("[INFO] weasyprint을 사용하여 PDF를 생성합니다.")
    except ImportError:
        pass

# 방법 3: pdfkit 시도
if PDF_METHOD is None:
    try:
        import pdfkit
        PDF_METHOD = 'pdfkit'
        print("[INFO] pdfkit을 사용하여 PDF를 생성합니다.")
    except ImportError:
        pass

if PDF_METHOD is None:
    print("\n[ERROR] PDF 생성 라이브러리가 설치되지 않았습니다.")
    print("\n[설치 방법]")
    print("방법 1 (권장): playwright")
    print("  pip install playwright")
    print("  playwright install chromium")
    print("\n방법 2: weasyprint")
    print("  pip install weasyprint")
    print("\n방법 3: pdfkit (wkhtmltopdf 필요)")
    print("  pip install pdfkit")
    print("  그리고 wkhtmltopdf를 별도로 설치해야 합니다.")
    print("\n또는 HTML 파일을 브라우저에서 열어 '인쇄 > PDF로 저장'을 사용하세요.")
    sys.exit(1)

def convert_html_to_pdf_playwright(html_path, pdf_path):
    """playwright를 사용하여 PDF 생성"""
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        
        # HTML 파일을 file:// URL로 로드
        file_url = f"file:///{html_path.replace(os.sep, '/')}"
        page.goto(file_url, wait_until='networkidle')
        
        # PDF 생성
        page.pdf(
            path=pdf_path,
            format='A4',
            print_background=True,
            margin={
                'top': '0mm',
                'right': '0mm',
                'bottom': '0mm',
                'left': '0mm'
            }
        )
        
        browser.close()

def convert_html_to_pdf_weasyprint(html_path, pdf_path):
    """weasyprint를 사용하여 PDF 생성"""
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    HTML(string=html_content).write_pdf(pdf_path)

def convert_html_to_pdf_pdfkit(html_path, pdf_path):
    """pdfkit를 사용하여 PDF 생성"""
    import pdfkit
    pdfkit.from_file(html_path, pdf_path, options={
        'page-size': 'A4',
        'margin-top': '0mm',
        'margin-right': '0mm',
        'margin-bottom': '0mm',
        'margin-left': '0mm',
        'encoding': 'UTF-8',
        'no-outline': None
    })

def convert_html_to_pdf(html_path, pdf_path=None):
    """
    HTML 파일을 PDF로 변환
    
    Args:
        html_path: HTML 파일 경로
        pdf_path: PDF 저장 경로 (None이면 HTML과 같은 경로에 저장)
    """
    if not os.path.exists(html_path):
        print(f"[ERROR] 파일을 찾을 수 없습니다: {html_path}")
        return False
    
    if pdf_path is None:
        # HTML 파일과 같은 경로에 PDF 저장
        pdf_path = html_path.replace('.html', '.pdf')
    
    try:
        print(f"[INFO] HTML 파일: {html_path}")
        print(f"[INFO] PDF 생성 중: {pdf_path}")
        print(f"[INFO] 사용 방법: {PDF_METHOD}")
        
        if PDF_METHOD == 'playwright':
            convert_html_to_pdf_playwright(html_path, pdf_path)
        elif PDF_METHOD == 'weasyprint':
            convert_html_to_pdf_weasyprint(html_path, pdf_path)
        elif PDF_METHOD == 'pdfkit':
            convert_html_to_pdf_pdfkit(html_path, pdf_path)
        
        print(f"[SUCCESS] PDF 생성 완료: {pdf_path}")
        return True
        
    except Exception as e:
        print(f"[ERROR] PDF 생성 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    # 기본 파일 경로
    default_html = './kr_output/reports/KR_2512_M_대표님보고서_3페이지.html'
    
    # 명령줄 인자로 파일 경로를 받을 수 있음
    if len(sys.argv) > 1:
        html_file = sys.argv[1]
    else:
        html_file = default_html
    
    # 절대 경로로 변환
    html_file = os.path.abspath(html_file)
    
    print(f"{'='*60}")
    print(f"HTML to PDF 변환 도구")
    print(f"{'='*60}\n")
    print(f"입력 파일: {html_file}\n")
    
    success = convert_html_to_pdf(html_file)
    
    if success:
        pdf_file = html_file.replace('.html', '.pdf')
        print(f"\n{'='*60}")
        print(f"변환 완료!")
        print(f"PDF 파일: {pdf_file}")
        print(f"{'='*60}")
    else:
        print(f"\n{'='*60}")
        print(f"변환 실패!")
        print(f"{'='*60}")
        sys.exit(1)
