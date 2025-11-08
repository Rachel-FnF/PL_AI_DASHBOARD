"""
채널별 매출 분석 모듈
- 채널별 매출 분석 (12개월 추이 - 기간, 채널, 아이템)
- 채널별로 어떤 아이템이 잘 팔리는지, 12개월 추이를 분석합니다
"""

import sys
import os
import json
from datetime import datetime

# 상위 디렉토리 경로 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from core.base_analyzer import BaseAnalyzer
from config.sql_queries import get_channel_sales_trend_query


class ChannelSalesAnalyzer(BaseAnalyzer):
    """
    채널별 매출 분석 클래스 (12개월 추이)
    
    사용 예시:
        analyzer = ChannelSalesAnalyzer(yyyymm='202509', brd_cd='M')
        analyzer.analyze_channel_sales_trend()  # 채널별 매출 분석
    """
    
    def analyze_channel_sales_trend(self):
        """
        채널별 매출 분석 (12개월 추이 - 기간, 채널, 아이템)
        
        12개월간의 채널별 매출 추이를 분석하고,
        채널별로 어떤 아이템(클래스3)이 잘 팔리는지 분석합니다.
        결과는 JSON과 Markdown 파일로 저장됩니다.
        """
        print(f"\n{'='*60}")
        print(f"📊 [{self.brd_name}] 채널별 매출 분석 (12개월 추이) 시작...")
        print(f"{'='*60}")
        
        try:
            # 분석 기간 설정 (12개월)
            # 현재 월부터 12개월 전까지
            current_year = int(self.yyyymm[:4])
            current_month = int(self.yyyymm[4:6])
            
            # 12개월 전 계산
            start_year = current_year
            start_month = current_month - 11
            
            # 월이 0 이하가 되면 전년도로 조정
            while start_month <= 0:
                start_month += 12
                start_year -= 1
            
            yyyymm_start = f"{start_year:04d}{start_month:02d}"
            yyyymm_end = self.yyyymm
            
            print(f"📅 분석 기간: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월 (12개월)")
            
            # 1. SQL 쿼리 실행
            sql = get_channel_sales_trend_query(
                yyyymm_start=yyyymm_start,
                yyyymm_end=yyyymm_end,
                brd_cd=self.brd_cd
            )
            df = self.execute_query(sql)
            records = df.to_dicts()
            
            if not records:
                print(f"⚠️ 데이터가 없습니다: {self.brd_name} 브랜드 채널별 매출 데이터")
                return None
            
            # 2. 데이터 요약 통계 생성
            total_sales = sum(float(record.get('SALE_AMT', 0)) for record in records)
            unique_channels = len(set(record.get('CHNL_NM', '') for record in records))
            unique_items = len(set(record.get('CLASS3', '') for record in records))
            unique_months = len(set(record.get('PST_YYYYMM', '') for record in records))
            
            print(f"📈 총 매출액: {total_sales:,.0f}원")
            print(f"📊 채널 수: {unique_channels}개")
            print(f"📦 아이템 수: {unique_items}개")
            print(f"📅 분석 월 수: {unique_months}개월")
            
            # 3. 채널별 요약 데이터 생성 (JSON용)
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
            
            # 채널별 상위 아이템 추출 (전체 기간 기준)
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
                        'total_sales': round(item['total_sales'] / 1000000, 2)  # 백만원 단위
                    }
                    for item in channel_items[:5]
                ]
                channel_summary[chnl_nm]['total_sales'] = round(
                    channel_summary[chnl_nm]['total_sales'] / 1000000, 2
                )
            
            # 4. LLM 프롬프트 생성
            prompt = f"""
            너는 F&F 그룹의 {self.brd_name} 브랜드 채널 전략 전문가야. 12개월간의 채널별 매출 추이를 분석하여 채널별 성과와 아이템 포트폴리오 전략을 제시해야 해.
            
            **분석 기간**
            - 시작: {yyyymm_start[:4]}년 {yyyymm_start[4:6]}월
            - 종료: {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월
            - 기간: {unique_months}개월
            
            **전체 요약**
            - 총 매출액: {total_sales:,.0f}원
            - 분석 채널 수: {unique_channels}개
            - 분석 아이템 수: {unique_items}개

            <분석 목표>
            {self.brd_name} 브랜드의 12개월간 채널별 매출 추이를 분석하여:
            1. 채널별 성과와 성장 패턴 파악
            2. 채널별 핵심 아이템(클래스3) 식별
            3. 채널별 매출 기여도와 비중 분석
            4. 채널별 전략적 인사이트 제시

            <핵심 분석 요구사항>

            1. **채널별 성과 종합 평가**
               - 채널별 총 매출액과 전체 대비 비중
               - 채널별 매출 추이 (증가/감소/유지)
               - 채널별 성장률 평가

            2. **채널별 핵심 아이템 분석**
               - 각 채널에서 매출 기여도가 높은 상위 아이템(클래스3) TOP 5
               - 채널별 아이템 포트폴리오 특성
               - 채널별 아이템 집중도 분석

            3. **월별 추이 분석**
               - 채널별 월별 매출 패턴 (계절성, 트렌드)
               - 특정 월에 급증/급감한 채널 식별
               - 월별 채널 순위 변화

            4. **채널별 전략적 인사이트**
               - 성장 잠재력이 높은 채널
               - 개선이 필요한 채널
               - 채널별 아이템 전략 제안

            5. **이상징후 감지**
               - 매출이 급격히 변화한 채널
               - 특정 아이템에 과도하게 의존하는 채널
               - 비정상적인 매출 패턴

            <작성 가이드라인>
            - 채널별로 섹션을 나누어 분석
            - 숫자는 백만원 단위로 표시하고 변형하지 말 것
            - 구체적인 수치와 비율을 함께 제시
            - 즉시 실행 가능한 전략 제안
            - 최대 120줄까지 작성

            <데이터>
            {json.dumps(records[:100], ensure_ascii=False, indent=2)}  # 상위 100개만 샘플로 전달
            
            위 데이터를 바탕으로 {self.brd_name} 브랜드의 채널별 매출 분석 (12개월 추이) 보고서를 작성해줘:
            """
            
            # 5. LLM 호출
            response = self.call_llm(prompt)
            
            # 6. Markdown 파일 저장
            filename = self.format_filename("12", "채널별_매출분석(12개월추이)")
            self.save_markdown(response, filename)
            
            # 7. JSON 데이터 생성
            json_data = {
                'brand_cd': self.brd_cd,
                'yyyymm_start': yyyymm_start,
                'yyyymm_end': yyyymm_end,
                'analysis_period': f"{yyyymm_start[:4]}년 {yyyymm_start[4:6]}월 ~ {yyyymm_end[:4]}년 {yyyymm_end[4:6]}월",
                'summary': {
                    'total_sales': round(total_sales / 1000000, 2),  # 백만원 단위
                    'unique_channels': unique_channels,
                    'unique_items': unique_items,
                    'unique_months': unique_months
                },
                'channel_summary': channel_summary,
                'analysis_text': response,
                'raw_data': {
                    'sample_records': self.convert_decimal_to_float(records[:50]),  # 샘플만 저장
                    'total_records_count': len(records)
                }
            }
            
            # 8. JSON 파일 저장
            self.save_json(json_data, filename)
            
            print(f"✅ [{self.brd_name}] 채널별 매출 분석 (12개월 추이) 완료! (MD + JSON)\n")
            return json_data
            
        except Exception as e:
            error_msg = f"❌ 분석 실패: {e}"
            print(error_msg)
            import traceback
            traceback.print_exc()
            raise

