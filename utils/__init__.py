# 유틸리티 모듈
import sys
import os
import importlib.util

# 프로젝트 루트의 utils.py에서 SQLUtil import (순환 import 방지)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
utils_py_path = os.path.join(project_root, 'utils.py')

if os.path.exists(utils_py_path):
    spec = importlib.util.spec_from_file_location("utils_module", utils_py_path)
    utils_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(utils_module)
    SQLUtil = utils_module.SQLUtil
else:
    raise ImportError(f"utils.py 파일을 찾을 수 없습니다: {utils_py_path}")

__all__ = ['SQLUtil']

