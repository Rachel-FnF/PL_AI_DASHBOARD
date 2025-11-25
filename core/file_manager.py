"""
파일 관리 모듈
- JSON과 Markdown 파일을 저장하는 기능을 제공합니다
- 모든 분석 결과는 이 모듈을 통해 저장됩니다
"""

import json
import os
from pathlib import Path
from config.analysis_config import OUTPUT_JSON_PATH, OUTPUT_MD_PATH


class FileManager:
    """
    분석 결과 파일을 저장하는 클래스
    
    사용 예시:
        manager = FileManager()
        manager.save_markdown(content, "01.M_브랜드_내수_손익분석")
        manager.save_json(data, "01.M_브랜드_내수_손익분석")
    """
    
    def __init__(self):
        """파일 매니저 초기화 - 출력 폴더 생성"""
        # 출력 폴더가 없으면 생성
        os.makedirs(OUTPUT_JSON_PATH, exist_ok=True)
        os.makedirs(OUTPUT_MD_PATH, exist_ok=True)
        print(f"📁 출력 폴더 확인: {OUTPUT_JSON_PATH}, {OUTPUT_MD_PATH}")
    
    def save_markdown(self, content, filename):
        """
        Markdown 파일로 저장
        
        Args:
            content (str): 저장할 마크다운 내용
            filename (str): 파일명 (확장자 제외)
        
        Returns:
            str: 저장된 파일의 전체 경로
        """
        try:
            file_path = os.path.join(OUTPUT_MD_PATH, f"{filename}.md")
            
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            
            print(f"✅ Markdown 파일 저장 완료: {file_path}")
            return file_path
            
        except Exception as e:
            error_msg = f"❌ Markdown 저장 실패 ({filename}): {e}"
            print(error_msg)
            raise Exception(error_msg)
    
    def save_json(self, data, filename):
        """
        JSON 파일로 저장
        
        Args:
            data (dict): 저장할 JSON 데이터
            filename (str): 파일명 (확장자 제외)
        
        Returns:
            str: 저장된 파일의 전체 경로
        """
        try:
            file_path = os.path.join(OUTPUT_JSON_PATH, f"{filename}.json")
            
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"✅ JSON 파일 저장 완료: {file_path}")
            return file_path
            
        except Exception as e:
            error_msg = f"❌ JSON 저장 실패 ({filename}): {e}"
            print(error_msg)
            raise Exception(error_msg)
    
    def read_markdown(self, filename):
        """
        Markdown 파일 읽기
        
        Args:
            filename (str): 파일명 (확장자 제외)
        
        Returns:
            str: 파일 내용
        """
        try:
            file_path = os.path.join(OUTPUT_MD_PATH, f"{filename}.md")
            
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            return content
            
        except FileNotFoundError:
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {filename}.md")
        except Exception as e:
            raise Exception(f"파일 읽기 실패 ({filename}): {e}")
    
    def read_json(self, filename):
        """
        JSON 파일 읽기
        
        Args:
            filename (str): 파일명 (확장자 제외)
        
        Returns:
            dict: JSON 데이터
        """
        try:
            file_path = os.path.join(OUTPUT_JSON_PATH, f"{filename}.json")
            
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            return data
            
        except FileNotFoundError:
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {filename}.json")
        except Exception as e:
            raise Exception(f"파일 읽기 실패 ({filename}): {e}")





