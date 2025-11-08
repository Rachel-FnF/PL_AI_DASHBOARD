"""
LLM 클라이언트 모듈
- Claude API를 호출하는 공통 기능을 제공합니다
- 모든 분석에서 이 모듈을 사용하여 LLM을 호출합니다
"""

import anthropic
import time
import sys
import os

# 프로젝트 루트의 settings를 import하기 위한 경로 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from settings import Config
from config.analysis_config import COMMON_SYSTEM_PROMPT, LLM_CONFIG


class LLMClient:
    """
    Claude API 호출을 담당하는 클래스
    
    사용 예시:
        client = LLMClient()
        response = client.send_message("분석할 데이터는...")
    """
    
    def __init__(self):
        """LLM 클라이언트 초기화"""
        self.client = anthropic.Anthropic(
            api_key=Config.CLAUDE_API_KEY,
            timeout=LLM_CONFIG['timeout']
        )
    
    def send_message(self, prompt, use_system_prompt=True, retry_count=None):
        """
        Claude API에 메시지를 전송하고 응답을 받습니다
        
        Args:
            prompt (str): LLM에 전달할 프롬프트 (분석 요청 내용)
            use_system_prompt (bool): 공통 시스템 프롬프트를 사용할지 여부 (기본: True)
            retry_count (int): 재시도 횟수 (None이면 설정값 사용)
        
        Returns:
            str: LLM이 생성한 분석 텍스트
        
        Raises:
            Exception: 모든 재시도 실패 시 예외 발생
        """
        if retry_count is None:
            retry_count = LLM_CONFIG['retry_count']
        
        # 시스템 프롬프트 적용 여부 결정
        if use_system_prompt:
            full_prompt = COMMON_SYSTEM_PROMPT + "\n\n" + prompt
        else:
            full_prompt = prompt
        
        # 재시도 로직
        for attempt in range(retry_count):
            try:
                print(f"Claude API 호출 시도 {attempt + 1}/{retry_count}")
                
                message = self.client.messages.create(
                    model=Config.CLAUDE_MODEL_VERSION,
                    max_tokens=LLM_CONFIG['max_tokens'],
                    temperature=LLM_CONFIG['temperature'],
                    messages=[{"role": "user", "content": full_prompt}]
                )
                
                # 응답이 잘렸는지 확인
                if message.stop_reason == "max_tokens":
                    print("⚠️ 경고: 응답이 잘렸습니다! (max_tokens 초과)")
                
                print("✅ Claude API 호출 성공!")
                return message.content[0].text
                
            except Exception as e:
                print(f"❌ 시도 {attempt + 1} 실패: {e}")
                
                # 마지막 시도가 아니면 대기 후 재시도
                if attempt < retry_count - 1:
                    wait_time = (attempt + 1) * 5  # 5초, 10초, 15초 대기
                    print(f"⏳ {wait_time}초 대기 후 재시도...")
                    time.sleep(wait_time)
                else:
                    # 모든 시도 실패
                    error_msg = f"API 호출 실패 (네트워크 오류): {e}"
                    print(f"❌ {error_msg}")
                    raise Exception(error_msg)
        
        # 이 코드는 실행되지 않아야 하지만, 안전을 위해 추가
        raise Exception("예상치 못한 오류 발생")

