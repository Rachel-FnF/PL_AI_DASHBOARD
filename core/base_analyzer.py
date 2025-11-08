"""
기본 분석 클래스
- 모든 분석 클래스가 상속받아야 하는 기본 클래스
- 공통 기능(DB 연결, 파일 저장, LLM 호출 등)을 제공합니다
"""

import sys
import os
import polars as pl
from datetime import datetime

# 프로젝트 루트의 utils를 import하기 위한 경로 추가
# utils.py는 프로젝트 루트에 생성해야 합니다
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils import SQLUtil
from config.analysis_config import BRAND_CODE_MAP
from core.llm_client import LLMClient
from core.file_manager import FileManager


class BaseAnalyzer:
    """
    모든 분석의 기본 클래스
    
    이 클래스를 상속받아서 새로운 분석을 만들면 됩니다.
    
    사용 예시:
        class MyAnalyzer(BaseAnalyzer):
            def analyze(self):
                # SQL 쿼리 실행
                df = self.execute_query("SELECT * FROM ...")
                
                # LLM 호출
                response = self.call_llm("분석해줘...")
                
                # 파일 저장
                self.save_markdown(response, "분석결과")
    """
    
    def __init__(self, yyyymm, brd_cd=None):
        """
        분석기 초기화
        
        Args:
            yyyymm (str): 분석할 년월 (예: '202509')
            brd_cd (str, optional): 브랜드 코드 (예: 'M', 'X'). None이면 전체 브랜드 분석
        """
        # DB 연결
        self.engine = SQLUtil.get_snowflake_engine()
        
        # 분석 기간 설정
        self.yyyymm = yyyymm  # 당해 년월
        self.yyyymm_py = str(int(yyyymm[:4]) - 1) + yyyymm[4:]  # 전년 동월
        
        # 브랜드 정보
        self.brd_cd = brd_cd
        self.brd_name = BRAND_CODE_MAP.get(brd_cd, brd_cd) if brd_cd else "전체"
        
        # 유틸리티 초기화
        self.llm_client = LLMClient()
        self.file_manager = FileManager()
        
        print(f"🔧 분석기 초기화: {self.brd_name} ({yyyymm})")
    
    def __del__(self):
        """소멸자 - DB 연결 종료"""
        if hasattr(self, 'engine'):
            self.engine.dispose()
    
    def execute_query(self, sql_query):
        """
        SQL 쿼리를 실행하고 결과를 DataFrame으로 반환
        
        Args:
            sql_query (str): 실행할 SQL 쿼리
        
        Returns:
            polars.DataFrame: 쿼리 결과
        
        사용 예시:
            df = self.execute_query("SELECT * FROM table WHERE yyyymm = '202509'")
            records = df.to_dicts()  # 딕셔너리 리스트로 변환
        """
        try:
            print(f"📊 SQL 쿼리 실행 중...")
            df = pl.read_database(sql_query, self.engine)
            print(f"✅ 쿼리 실행 완료: {len(df)}개 행 조회")
            return df
        except Exception as e:
            error_msg = f"❌ SQL 쿼리 실행 실패: {e}"
            print(error_msg)
            raise Exception(error_msg)
    
    def call_llm(self, prompt, use_system_prompt=True):
        """
        LLM을 호출하여 분석 텍스트 생성
        
        Args:
            prompt (str): LLM에 전달할 프롬프트
            use_system_prompt (bool): 공통 시스템 프롬프트 사용 여부
        
        Returns:
            str: LLM이 생성한 분석 텍스트
        
        사용 예시:
            response = self.call_llm("이 데이터를 분석해주세요: {data}")
        """
        return self.llm_client.send_message(prompt, use_system_prompt)
    
    def save_markdown(self, content, filename):
        """
        Markdown 파일로 저장
        
        Args:
            content (str): 저장할 마크다운 내용
            filename (str): 파일명 (확장자 제외)
        
        사용 예시:
            self.save_markdown(response, "01.M_브랜드_내수_손익분석")
        """
        return self.file_manager.save_markdown(content, filename)
    
    def save_json(self, data, filename):
        """
        JSON 파일로 저장
        
        Args:
            data (dict): 저장할 JSON 데이터
            filename (str): 파일명 (확장자 제외)
        
        사용 예시:
            self.save_json({"result": "..."}, "01.M_브랜드_내수_손익분석")
        """
        return self.file_manager.save_json(data, filename)
    
    def read_markdown(self, filename):
        """
        기존에 저장된 Markdown 파일 읽기
        
        Args:
            filename (str): 파일명 (확장자 제외)
        
        Returns:
            str: 파일 내용
        
        사용 예시:
            content = self.read_markdown("05.M_채널별_전략분석")
        """
        return self.file_manager.read_markdown(filename)
    
    def convert_decimal_to_float(self, obj):
        """
        Decimal 타입을 float로 변환 (JSON 직렬화용)
        
        Args:
            obj: 변환할 객체 (dict, list, Decimal 등)
        
        Returns:
            변환된 객체
        
        사용 예시:
            records = self.convert_decimal_to_float(df.to_dicts())
        """
        import decimal
        
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, dict):
            return {k: self.convert_decimal_to_float(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.convert_decimal_to_float(item) for item in obj]
        return obj
    
    def format_filename(self, prefix, suffix):
        """
        파일명을 일관된 형식으로 생성
        
        Args:
            prefix (str): 파일명 앞부분 (예: "01", "07")
            suffix (str): 파일명 뒷부분 (예: "브랜드_내수_손익분석")
        
        Returns:
            str: 완성된 파일명
        
        사용 예시:
            filename = self.format_filename("01", f"{self.brd_cd}_브랜드_내수_손익분석")
            # 결과: "01.M_브랜드_내수_손익분석"
        """
        if self.brd_cd:
            return f"{prefix}.{self.brd_cd}_{suffix}"
        else:
            return f"{prefix}.{suffix}"

