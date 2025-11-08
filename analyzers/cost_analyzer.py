"""
비용 분석 모듈
- 광고선전비, 간접비, 직접비 분석을 수행합니다
"""

import sys
import os
import json
import time
from datetime import datetime

# 상위 디렉토리 경로 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from core.base_analyzer import BaseAnalyzer
from config.sql_queries import (
    get_ad_expense_total_query,
    get_ad_expense_detail_query,
    get_ad_expense_trend_query,
    get_indirect_cost_query,
    get_direct_cost_query
)


class CostAnalyzer(BaseAnalyzer):
    """
    비용 분석 클래스
    
    사용 예시:
        analyzer = CostAnalyzer(yyyymm='202509', brd_cd='M')
        analyzer.analyze_ad_expense()      # 광고선전비 분석
        analyzer.analyze_indirect_cost()   # 간접비 분석
        analyzer.analyze_direct_cost()     # 직접비 분석
    """
    
    def analyze_ad_expense(self):
        """
        07번: 광고선전비 추이분석
        
        전년 동월과 당해 동월의 광고선전비를 비교 분석합니다.
        JSON과 Markdown 파일을 모두 생성합니다.
        """
        print(f"\n{'='*60}")
        print(f"📊 [{self.brd_name}] 광고선전비 추이분석 시작...")
        print(f"{'='*60}")
        
        try:
            # 분석 대상 월 추출
            current_year = self.yyyymm[:4]
            current_month = self.yyyymm[4:6]
            previous_year = self.yyyymm_py[:4]
            
            # 1. 전체 합계 데이터 조회
            total_sql = get_ad_expense_total_query(
                yyyymm=self.yyyymm,
                yyyymm_py=self.yyyymm_py,
                brd_cd=self.brd_cd
            )
            df_total = self.execute_query(total_sql)
            total_records = df_total.to_dicts()
            
            # 2. 세부 내역 데이터 조회
            detail_sql = get_ad_expense_detail_query(
                yyyymm=self.yyyymm,
                yyyymm_py=self.yyyymm_py,
                brd_cd=self.brd_cd
            )
            df_detail = self.execute_query(detail_sql)
            detail_records = df_detail.to_dicts()
            
            # 3. 12개월 추세 데이터 조회
            trend_months = self._generate_trend_months()
            trend_total_sql = get_ad_expense_trend_query(trend_months, self.brd_cd)
            df_trend_total = self.execute_query(trend_total_sql)
            trend_total_records = df_trend_total.to_dicts()
            
            # 4. 요약 정보 계산
            total_by_year = {}
            for row in total_records:
                year = row['PST_YYYYMM'][:4]
                total_by_year[year] = row['TOTAL_AMT']
            
            prev_year_total = total_by_year.get(previous_year, 0)
            curr_year_total = total_by_year.get(current_year, 0)
            change_amount = curr_year_total - prev_year_total
            change_pct = (change_amount / prev_year_total * 100) if prev_year_total != 0 else 0
            
            print(f"📈 {previous_year}년 {current_month}월: {prev_year_total:,.0f}원")
            print(f"📈 {current_year}년 {current_month}월: {curr_year_total:,.0f}원")
            print(f"📊 전년대비 변화: {change_amount:+,.0f}원 ({change_pct:+.1f}%)")
            
            # 5. LLM 프롬프트 생성
            prompt = f"""
            너는 F&F 그룹의 {self.brd_name} 브랜드 마케팅 전략 책임자야. {previous_year}년 {current_month}월과 {current_year}년 {current_month}월의 광고선전비를 비교 분석하여 마케팅 투자 효율성과 최적화 방안을 제시해야 해.
            
            **분석 기간**
            - 당해: {current_year}년 {current_month}월
            - 전년: {previous_year}년 {current_month}월

            <분석 목표>
            {self.brd_name} 브랜드의 {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월 광고선전비 투자 변화를 분석하여 마케팅 전략의 효과성과 향후 예산 배분 전략을 경영관리팀에게 수립해줘.

            <전체 합계 데이터>
            {total_records}
            
            <세부 계정별 데이터>
            {detail_records}

            <요구사항>
            아래 JSON 형식으로 분석 결과를 반환해줘. 반드시 유효한 JSON 형식이어야 하고, 마크다운 코드 블록 없이 순수 JSON만 반환해줘.

            {{
              "title": "광고비 분석",
              "sections": [
                {{
                  "sub_title": "투자 방향성 종합 평가",
                  "ai_text": "전년대비 {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월 광고비 변화를 종합적으로 평가한 내용"
                }},
                {{
                  "sub_title": "효율적 투자 영역",
                  "ai_text": "효과적인 투자 영역들을 불릿 포인트로 나열"
                }},
                {{
                  "sub_title": "주의 필요 영역",
                  "ai_text": "주의가 필요한 영역들을 불릿 포인트로 나열"
                }},
                {{
                  "sub_title": "이상징후 및 리스크 감지",
                  "ai_text": "이상징후와 리스크를 구체적으로 설명"
                }},
                {{
                  "sub_title": "마케팅 전략 최적화 방안",
                  "ai_text": "단기 전략 방향과 중장기 전략 방향을 구체적으로 제시"
                }}
              ]
            }}

            <작성 가이드라인>
            - 각 섹션의 ai_text는 구체적이고 실용적인 내용으로 작성
            - 숫자는 백만원 단위로 표시하고 변형하지 말 것
            - 모든 광고선전비 계정 (CTGR3) 누락 없이 분석
            - 전년대비 변화에 대한 구체적 원인과 효과 분석
            - 즉시 실행 가능한 예산 최적화 방안 제시
            - 불릿 포인트는 마크다운 형식(-, •, **) 사용 가능
            - 줄바꿈은 반드시 \\n을 사용하여 표시
            - 반드시 유효한 JSON 형식으로만 응답 (마크다운 코드 블록 없이)

            위 데이터를 바탕으로 JSON 형식으로 분석 결과를 반환해줘:
            """
            
            # 6. LLM 호출
            response = self.call_llm(prompt)
            
            # 7. JSON 파싱
            response_clean = response.strip()
            if response_clean.startswith('```json'):
                response_clean = response_clean[7:]
            elif response_clean.startswith('```'):
                response_clean = response_clean[3:]
            if response_clean.endswith('```'):
                response_clean = response_clean[:-3]
            response_clean = response_clean.strip()
            
            try:
                ai_analysis_json = json.loads(response_clean)
            except json.JSONDecodeError as e:
                print(f"❌ JSON 파싱 오류: {e}")
                print(f"응답 내용: {response_clean[:500]}")
                ai_analysis_json = {
                    "title": "광고비 분석",
                    "sections": [{
                        "sub_title": "분석 오류",
                        "ai_text": response
                    }]
                }
            
            # 8. Markdown 파일 생성
            md_content = f"# 📊 {self.brd_name} 브랜드 광고선전비 비교 분석 보고서 ({previous_year}.{current_month} vs {current_year}.{current_month})\n\n"
            for section in ai_analysis_json.get('sections', []):
                md_content += f"## {section.get('sub_title', '')}\n\n"
                ai_text = section.get('ai_text', '').replace('\\n', '\n')
                md_content += f"{ai_text}\n\n"
            
            filename = self.format_filename("07", "광고선전비_추이분석")
            self.save_markdown(md_content, filename)
            
            # 9. 카테고리별 데이터 구조화
            categories_by_year = {}
            for row in detail_records:
                year = row['PST_YYYYMM'][:4]
                ctgr2 = row.get('CTGR2', '기타')
                ctgr3 = row.get('CTGR3', '기타')
                gl_nm = row.get('GL_NM', '')
                amount = row.get('TTL_USE_AMT', 0)
                
                category_key = f"{ctgr2}|{ctgr3}|{gl_nm}"
                if category_key not in categories_by_year:
                    categories_by_year[category_key] = {
                        'ctgr2': ctgr2,
                        'ctgr3': ctgr3,
                        'gl_nm': gl_nm,
                        'prev_year': 0,
                        'curr_year': 0
                    }
                
                if year == previous_year:
                    categories_by_year[category_key]['prev_year'] = float(amount)
                elif year == current_year:
                    categories_by_year[category_key]['curr_year'] = float(amount)
            
            # 카테고리별 변화량 계산 (백만원 단위)
            categories_list = []
            for category_key, category_data in categories_by_year.items():
                prev_year_m = float(category_data['prev_year']) / 1000000
                curr_year_m = float(category_data['curr_year']) / 1000000
                change = curr_year_m - prev_year_m
                change_pct_cat = (change / prev_year_m * 100) if prev_year_m != 0 else (100 if curr_year_m > 0 else 0)
                
                category_data['prev_year'] = round(prev_year_m, 2)
                category_data['curr_year'] = round(curr_year_m, 2)
                category_data['change'] = round(change, 2)
                category_data['change_pct'] = round(change_pct_cat, 1)
                category_data['is_new'] = prev_year_m == 0 and curr_year_m > 0
                category_data['is_discontinued'] = prev_year_m > 0 and curr_year_m == 0
                
                categories_list.append(category_data)
            
            categories_list.sort(key=lambda x: abs(x['change']), reverse=True)
            
            # 10. JSON 데이터 생성
            json_data = {
                'brand_cd': self.brd_cd,
                'yyyymm': self.yyyymm,
                'analysis_data': {
                    'title': ai_analysis_json.get('title', '광고비 분석'),
                    'sections': ai_analysis_json.get('sections', [])
                },
                'summary': {
                    'prev_year_total': round(float(prev_year_total) / 1000000, 2),
                    'curr_year_total': round(float(curr_year_total) / 1000000, 2),
                    'change_amount': round(float(change_amount) / 1000000, 2),
                    'change_pct': round(change_pct, 1),
                    'investment_direction': '확대' if change_amount > 0 else '축소' if change_amount < 0 else '유지'
                },
                'categories': categories_list,
                'category_summary': {
                    'increased': [c for c in categories_list if c['change'] > 0],
                    'decreased': [c for c in categories_list if c['change'] < 0],
                    'new_investments': [c for c in categories_list if c['is_new']],
                    'discontinued': [c for c in categories_list if c['is_discontinued']]
                },
                'raw_data': {
                    'total_records': self.convert_decimal_to_float(total_records),
                    'detail_records': self.convert_decimal_to_float(detail_records)
                },
                'trend_data': {
                    'trend_months': trend_months,
                    'monthly_totals': [
                        {
                            'yyyymm': row['PST_YYYYMM'],
                            'total_amount': round(float(row['TOTAL_AMT']) / 1000000, 2)
                        }
                        for row in self.convert_decimal_to_float(trend_total_records)
                    ]
                }
            }
            
            # 11. JSON 파일 저장
            self.save_json(json_data, filename)
            
            print(f"✅ [{self.brd_name}] 광고선전비 추이분석 완료! (MD + JSON)\n")
            return json_data
            
        except Exception as e:
            error_msg = f"❌ 분석 실패: {e}"
            print(error_msg)
            raise
    
    def _generate_trend_months(self):
        """
        12개월 추세 데이터를 위한 월 리스트 생성
        
        Returns:
            list: 12개월 월 리스트 (예: ['202410', '202411', ...])
        """
        current_year = int(self.yyyymm[:4])
        current_month = int(self.yyyymm[4:6])
        trend_months = []
        
        for i in range(12):
            year = current_year
            month = current_month - i
            
            while month <= 0:
                month += 12
                year -= 1
            
            trend_months.append(f"{year:04d}{month:02d}")
        
        trend_months.sort()
        return trend_months
    
    def analyze_indirect_cost(self):
        """
        10번: 간접비 분석
        
        전년 동월과 당해 동월의 간접비를 비교 분석합니다.
        """
        print(f"\n{'='*60}")
        print(f"📊 [{self.brd_name}] 간접비 분석 시작...")
        print(f"{'='*60}")
        
        try:
            current_year = self.yyyymm[:4]
            current_month = self.yyyymm[4:6]
            previous_year = self.yyyymm_py[:4]
            
            # SQL 쿼리 실행
            sql = get_indirect_cost_query(
                yyyymm=self.yyyymm,
                yyyymm_py=self.yyyymm_py,
                brd_cd=self.brd_cd
            )
            df = self.execute_query(sql)
            records = df.to_dicts()
            
            if not records:
                print(f"⚠️ 데이터가 없습니다: {self.brd_name} 브랜드 간접비 데이터")
                return None
            
            # LLM 프롬프트 생성
            prompt = f"""
            너는 F&F 그룹의 {self.brd_name} 브랜드 간접비 관리 전문가야. {previous_year}년 {current_month}월과 {current_year}년 {current_month}월의 간접비를 상세 비교 분석하여 비용 효율성 개선과 수익성 제고 방안을 제시해야 해.
            
            **분석 기간**
            - 당해: {current_year}년 {current_month}월
            - 전년: {previous_year}년 {current_month}월

            <분석 목표>
            {self.brd_name} 브랜드의 {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월 간접비 투자 변화를 분석하여 비용 최적화와 운영 효율성 향상을 위한 실행 가능한 전략을 경영관리팀에게 제시해줘.

            <핵심 분석 요구사항>

            1. **📊 {previous_year}년 vs {current_year}년 {current_month}월 간접비 요약 비교 (가장 먼저 작성)**
               - **{previous_year}년 {current_month}월 총 간접비**: X,XXX백만원
               - **{current_year}년 {current_month}월 총 간접비**: X,XXX백만원
               - **전년대비 증감**: ±X,XXX백만원 (±X.X%)
               - **비용 관리 평가**: 효율화/비효율화 및 그 원인

            2. **간접비 카테고리별 상세 변화 분석**
               - CTGR1별 {previous_year}년 vs {current_year}년 투자 변화와 비중 분석
               - 증가한 간접비 카테고리의 사업적 필요성 평가
               - 감소한 간접비 카테고리의 운영 효율성 개선 효과
               - 신규 발생/중단된 간접비 항목 식별
               - 모든 간접비 계정을 누락 없이 포함하여 분석

            3. **간접비 효율성 및 적정성 평가**
               - 전년 동월 대비 간접비 증감률과 변화 요인 분석
               - 고정비 vs 변동비 성격의 간접비 구조 분석
               - 규모의 경제 실현 여부와 비용 효율성 평가

            4. **이상징후 및 리스크 감지**
               - 급증한 간접비 카테고리와 그 원인 분석
               - 과도한 고정비 부담으로 인한 수익성 압박 요인
               - 비효율적 간접비 지출 패턴 및 개선 가능 영역

            5. **간접비 구조 최적화 방안**
               - 고효율 간접비 카테고리로의 재배분 전략
               - 비효율적 간접비의 단계적 축소 방안
               - 브랜드 운영 기여도 대비 간접비 투자 우선순위 재조정

            <작성 가이드라인>
            - 맨 처음에 {previous_year}년 vs {current_year}년 {current_month}월 간접비 요약 비교를 명확히 제시
            - 모든 간접비 카테고리 (CTGR1, CTGR2, CTGR3) 누락 없이 분석
            - 전년대비 변화에 대한 구체적 원인과 비용 효율성 분석
            - 즉시 실행 가능한 비용 최적화 방안 제시
            - 최대 100줄까지 작성
            - 숫자는 변형하지 말 것 (단위: 백만원)

            <데이터>
            {records}

            위 데이터를 바탕으로 {self.brd_name} 브랜드의 {previous_year}년 vs {current_year}년 {current_month}월 간접비 비교 분석 및 비용 최적화 전략 보고서를 작성해줘:
            """
            
            # LLM 호출
            response = self.call_llm(prompt)
            
            # 파일 저장
            filename = self.format_filename("10", "간접비_분석")
            self.save_markdown(response, filename)
            
            print(f"✅ [{self.brd_name}] 간접비 분석 완료!\n")
            return response
            
        except Exception as e:
            error_msg = f"❌ 분석 실패: {e}"
            print(error_msg)
            raise
    
    def analyze_direct_cost(self):
        """
        11번: 직접비 분석
        
        전년 동월과 당해 동월의 직접비를 비교 분석합니다.
        """
        print(f"\n{'='*60}")
        print(f"📊 [{self.brd_name}] 직접비 분석 시작...")
        print(f"{'='*60}")
        
        try:
            current_year = self.yyyymm[:4]
            current_month = self.yyyymm[4:6]
            previous_year = self.yyyymm_py[:4]
            
            # SQL 쿼리 실행
            sql = get_direct_cost_query(
                yyyymm=self.yyyymm,
                yyyymm_py=self.yyyymm_py,
                brd_cd=self.brd_cd
            )
            df = self.execute_query(sql)
            records = df.to_dicts()
            
            if not records:
                print(f"⚠️ 데이터가 없습니다: {self.brd_name} 브랜드 직접비 데이터")
                return None
            
            # LLM 프롬프트 생성
            prompt = f"""
            너는 F&F 그룹의 {self.brd_name} 브랜드 직접비 관리 전문가야. {previous_year}년 {current_month}월과 {current_year}년 {current_month}월의 직접비를 상세 비교 분석하여 운영 효율성 개선과 수익성 제고 방안을 제시해야 해.
            
            **분석 기간**
            - 당해: {current_year}년 {current_month}월
            - 전년: {previous_year}년 {current_month}월

            <분석 목표>
            {self.brd_name} 브랜드의 {previous_year}년 {current_month}월 vs {current_year}년 {current_month}월 직접비 투자 변화를 분석하여 운영비 최적화와 채널별 효율성 향상을 위한 실행 가능한 전략을 경영관리팀에게 제시해줘.

            <핵심 분석 요구사항>

            1. **📊 {previous_year}년 vs {current_year}년 {current_month}월 직접비 요약 비교 (가장 먼저 작성)**
               - **{previous_year}년 {current_month}월 총 직접비**: X,XXX백만원
               - **{current_year}년 {current_month}월 총 직접비**: X,XXX백만원
               - **전년대비 증감**: ±X,XXX백만원 (±X.X%)
               - **비용 관리 평가**: 효율화/비효율화 및 그 원인

            2. **직접비 항목별 상세 변화 분석**
               - 로열티, 매장임차료, 판매직수수료, 카드수수료, 물류보관비, 매장감가상각비별 {previous_year}년 vs {current_year}년 변화
               - 증가한 직접비 항목의 운영상 필요성 평가
               - 감소한 직접비 항목의 효율성 개선 효과
               - 신규 발생/중단된 직접비 항목 식별
               - 모든 직접비 계정을 누락 없이 포함하여 분석

            3. **직접비 효율성 및 적정성 평가**
               - 전년 동월 대비 직접비 증감률과 변화 요인 분석
               - 고정비 vs 변동비 성격의 직접비 구조 분석
               - 채널별 직접비 효율성과 운영 특성 평가

            4. **이상징후 및 리스크 감지**
               - 급증한 직접비 항목과 그 원인 분석
               - 과도한 고정 직접비 부담으로 인한 수익성 압박 요인
               - 비효율적 직접비 지출 패턴 및 개선 가능 영역

            5. **직접비 구조 최적화 방안**
               - 고효율 직접비 항목으로의 재배분 전략
               - 비효율적 직접비의 단계적 축소 방안
               - 채널별 운영 특성에 맞는 직접비 투자 우선순위 재조정

            <작성 가이드라인>
            - 맨 처음에 {previous_year}년 vs {current_year}년 {current_month}월 직접비 요약 비교를 명확히 제시
            - 모든 직접비 항목 (로열티, 임차료, 수수료, 물류비 등) 누락 없이 분석
            - 전년대비 변화에 대한 구체적 원인과 운영 효율성 분석
            - 즉시 실행 가능한 운영비 최적화 방안 제시
            - 최대 100줄까지 작성
            - 숫자는 변형하지 말 것 (단위: 백만원)

            <데이터>
            {records}

            위 데이터를 바탕으로 {self.brd_name} 브랜드의 {previous_year}년 vs {current_year}년 {current_month}월 직접비 비교 분석 및 운영 최적화 전략 보고서를 작성해줘:
            """
            
            # LLM 호출
            response = self.call_llm(prompt)
            
            # 파일 저장
            filename = self.format_filename("11", "직접비_분석")
            self.save_markdown(response, filename)
            
            print(f"✅ [{self.brd_name}] 직접비 분석 완료!\n")
            return response
            
        except Exception as e:
            error_msg = f"❌ 분석 실패: {e}"
            print(error_msg)
            raise

