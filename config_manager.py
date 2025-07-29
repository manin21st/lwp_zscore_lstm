# config_manager.py
import os
import configparser
from cryptography.fernet import Fernet

def load_key():
    """환경 변수에서 암호화 키를 로드합니다."""
    key = os.getenv('CONFIG_KEY')
    if not key:
        raise ValueError("암호화 키를 찾을 수 없습니다. 'CONFIG_KEY' 환경 변수를 설정해주세요.")
    return key.encode()

def load_encrypted_config(encrypted_config_path='config.enc'):
    """암호화된 설정 파일을 복호화하여 config 객체로 반환합니다."""
    key = load_key()
    fernet = Fernet(key)

    try:
        with open(encrypted_config_path, 'rb') as f:
            encrypted_data = f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"암호화된 설정 파일 '{encrypted_config_path}'을 찾을 수 없습니다. 먼저 암호화 스크립트를 실행했는지 확인하세요.")

    decrypted_data = fernet.decrypt(encrypted_data).decode('utf-8')

    config = configparser.ConfigParser()
    config.read_string(decrypted_data)
    return config