"""
브랜드 분석 모듈
- 브랜드별 내수/수출 손익분석을 수행합니다
"""

import sys
import os
import json
import time
from datetime import datetime

# 상위 디렉토리 경로 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from core.base_analyzer import BaseAnalyzer
from config.sql_queries import get_brand_domestic_query, get_brand_export_query


class BrandAnalyzer(BaseAnalyzer):
    """
    브랜드별 손익분석 클래스
    
    사용 예시:
        analyzer = BrandAnalyzer(yyyymm='202509', brd_cd='M')
        analyzer.analyze_domestic_profit_loss()  # 내수 손익분석
        analyzer.analyze_export_profit_loss()    # 수출 손익분석
    """
    
    def analyze_domestic_profit_loss(self):
        """
        01번: 브랜드별 내수 손익분석(월)
        
        전년 동월과 당해 동월을 비교하여 브랜드의 내수 손익을 분석합니다.
        결과는 JSON과 Markdown 파일로 저장됩니다.
        """
        print(f"\n{'='*60}")
        print(f"📊 [{self.brd_name}] 브랜드 내수 손익분석 시작...")
        print(f"{'='*60}")
        
        try:
            # 1. SQL 쿼리 실행
            sql = get_brand_domestic_query(
                yyyymm=self.yyyymm,
                yyyymm_py=self.yyyymm_py,
                brd_cd=self.brd_cd
            )
            df = self.execute_query(sql)
            records = df.to_dicts()
            
            if not records:
                print(f"⚠️ 데이터가 없습니다: {self.brd_name} 브랜드 내수 데이터")
                return None
            
            # 2. LLM 프롬프트 생성
            prompt = f"""
            너는 F&F 그룹의 수석 재무분석가야. {self.brd_name} 브랜드의 손익분석을 통해 경영관리팀이 즉시 행동할 수 있는 인사이트를 제공해야 해.
            
            **분석 기간**
            - 당해: {self.yyyymm[:4]}년 {self.yyyymm[4:6]}월
            - 전년: {self.yyyymm_py[:4]}년 {self.yyyymm_py[4:6]}월
            
            <분석 목표>
            경영관리팀이 {self.brd_name} 브랜드의 전략적 의사결정을 내릴 수 있도록 핵심 성과, 위험요소, 기회요소를 명확하게 제시해줘.
            
            <핵심 분석 요구사항>
            
            **🎯 {self.brd_name} 브랜드 핵심 성과표**
            다음 형태로 작성:
            
            ### {self.brd_name}
            **내수**: 매출액 X,XXX백만원 | 영업이익 X,XXX백만원 | 영업이익률 X.X%
            **전년대비 평가**: 매출 ±X.X% | 영업이익 ±X.X% | 종합평가(상승/하락/유지)
            **할인율 분석**: 당해 X.X% vs 전년 X.X% (±X.X%p 변화) | 할인 정책 평가
            
            💡 **{self.brd_name} 브랜드 상세 분석**:
            - **수익성 진단**: 영업이익률 수준 평가 (높은/낮은 비용 항목 식별)
            - **비용 구조**: 효율적이거나 과도한 비용 항목 분석
            - **할인 전략**: 할인율 변화가 매출과 수익성에 미친 영향 분석
            - **경쟁력 평가**: {self.brd_name}의 상대적 강점/약점
            - **개선 포인트**: 즉시 개선 가능한 구체적 비용 최적화 및 가격 전략 방안
            
            1. **수익성 구조 분석**
               - 매출총이익률과 영업이익률의 차이 원인 분석
               - 직접비와 영업비의 비중 평가
               - 비용 효율성 진단
            
            2. **채널별 전략적 인사이트**
               - 내수 vs 수출 채널의 수익성 비교
               - 채널별 비용 구조의 차이점과 최적화 방안
            
            3. **위험요소 및 기회요소**
               - 전년대비 성과 변화의 원인 분석
               - 성장 잠재력과 성장 요인
               - 비용 증가율 대비 매출 증가율 분석
            
            4. **경영진 행동 권고사항**
               - 즉시 개선이 필요한 영역과 우선순위
               - 성공 요인 강화 방안
               - 다음 분기 예상 성과와 대응 전략
             
            <작성 가이드라인>
            - {self.brd_name} 브랜드의 성과표를 반드시 작성
            - 숫자는 절대 변형하지 말 것 (단위: 백만원, 3자리마다 쉼표)
            - 비율은 소수점 첫째자리까지 표현
            - 경영관리팀이 즉시 이해할 수 있는 명확한 언어 사용
            - 최대 60줄까지 작성
            
            <데이터>
            {records}
            
            위 요구사항에 따라 경영관리팀이 전략적 의사결정을 내릴 수 있는 {self.brd_name} 브랜드 분석 보고서를 작성해줘:
            """
            
            # 3. LLM 호출
            response = self.call_llm(prompt)
            
            # 4. 파일 저장
            filename = self.format_filename("01", "브랜드_내수_손익분석(월)")
            self.save_markdown(response, filename)
            
            # 5. JSON 데이터 생성 (필요한 경우)
            # 현재는 MD만 저장하지만, 필요하면 JSON도 생성 가능
            # json_data = {
            #     "brand_cd": self.brd_cd,
            #     "yyyymm": self.yyyymm,
            #     "analysis_text": response,
            #     "raw_data": self.convert_decimal_to_float(records)
            # }
            # self.save_json(json_data, filename)
            
            print(f"✅ [{self.brd_name}] 브랜드 내수 손익분석 완료!\n")
            return response
            
        except Exception as e:
            error_msg = f"❌ 분석 실패: {e}"
            print(error_msg)
            raise
    
    def analyze_export_profit_loss(self):
        """
        02번: 브랜드별 수출 손익분석(월)
        
        전년 동월과 당해 동월을 비교하여 브랜드의 수출 손익을 분석합니다.
        """
        print(f"\n{'='*60}")
        print(f"📊 [{self.brd_name}] 브랜드 수출 손익분석 시작...")
        print(f"{'='*60}")
        
        try:
            # 1. SQL 쿼리 실행
            sql = get_brand_export_query(
                yyyymm=self.yyyymm,
                yyyymm_py=self.yyyymm_py,
                brd_cd=self.brd_cd
            )
            df = self.execute_query(sql)
            records = df.to_dicts()
            
            if not records:
                print(f"⚠️ 데이터가 없습니다: {self.brd_name} 브랜드 수출 데이터")
                return None
            
            # 2. LLM 프롬프트 생성
            prompt = f"""
            너는 F&F 그룹의 수석 재무분석가야. {self.brd_name} 브랜드의 수출 손익분석을 통해 경영관리팀이 즉시 행동할 수 있는 인사이트를 제공해야 해.
            
            **분석 기간**
            - 당해: {self.yyyymm[:4]}년 {self.yyyymm[4:6]}월
            - 전년: {self.yyyymm_py[:4]}년 {self.yyyymm_py[4:6]}월
            
            <분석 목표>
            {self.brd_name} 브랜드의 수출 성과를 분석하여 수출 전략의 효과성을 평가하고 개선 방안을 제시해줘.
            
            <핵심 분석 요구사항>
            
            1. **수출 성과 종합 평가**
               - {self.brd_name} 브랜드 수출 매출, 매출총이익, 영업이익의 전년대비 성장률
               - 수출 성과와 전년대비 증감률 분석
            
            2. **수출 수익성 구조 분석**
               - 수출 매출총이익률과 영업이익률 비교
               - 수출 비용 효율성과 전년대비 개선/악화 요인 분석
               - 수익성 수준 평가와 원인
            
            3. **수출 전략 성과**
               - 수출 채널의 수익성 평가
               - 수출 성과 변화와 전략적 의미
            
            4. **수출 성장 패턴 분석**
               - 수출 성장률 평가
               - 성장 요인 또는 정체 요인 분석
               - 수출 비용 증가율과 매출 증가율의 관계 분석
            
            5. **수출 전략적 시사점**
               - 수출 성과를 바탕으로 한 다음 분기 전략 방향성
               - {self.brd_name} 브랜드의 수출 모범 사례 또는 개선 필요 사항
            
            <작성 가이드라인>
            - {self.brd_name} 브랜드 수출 성과 핵심 요약 (2-3줄)
            - 수출 핵심 성과 상세 분석
            - 숫자는 절대 변형하지 말 것 (단위: 백만원, 3자리마다 쉼표)
            - 비율은 소수점 첫째자리까지 표현
            - 전년대비 증감률을 명확하게 제시
            - 최대 50줄까지 작성
            
            <데이터>
            {records}
            
            위 요구사항에 따라 {self.brd_name} 브랜드의 수출 성과를 종합적으로 분석한 보고서를 작성해줘:
            """
            
            # 3. LLM 호출
            response = self.call_llm(prompt)
            
            # 4. 파일 저장
            filename = self.format_filename("02", "브랜드_수출_손익분석(월)")
            self.save_markdown(response, filename)
            
            print(f"✅ [{self.brd_name}] 브랜드 수출 손익분석 완료!\n")
            return response
            
        except Exception as e:
            error_msg = f"❌ 분석 실패: {e}"
            print(error_msg)
            raise

