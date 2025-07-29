# encrypt_config.py
from cryptography.fernet import Fernet
import os

# 1. 키 생성
key = Fernet.generate_key()
print("="*60)
print("Generated Encryption Key (!!! STORE THIS SECURELY !!!)")
print(f"Key (string): {key.decode()}")
print("="*60)
print("\n이 키를 'CONFIG_KEY'라는 이름의 환경 변수로 설정해야 합니다.")
print("이 키가 없으면 애플리케이션이 설정을 읽을 수 없습니다.")
print("\n--- 예시 ---")
print("Windows (CMD):       setx CONFIG_KEY \"your_generated_key_string\"")
print("Windows (PowerShell): $env:CONFIG_KEY=\"your_generated_key_string\"")
print("Linux/macOS:         export CONFIG_KEY=\"your_generated_key_string\"")
print("-" * 12 + "\n")

# 2. config.ini 파일 읽기
try:
    with open('config.ini', 'rb') as f:
        config_data = f.read()
except FileNotFoundError:
    print("[ERROR] 'config.ini' 파일을 찾을 수 없습니다. 스크립트와 같은 위치에 있는지 확인하세요.")
    exit()

# 3. 데이터 암호화
fernet = Fernet(key)
encrypted_data = fernet.encrypt(config_data)

# 4. 암호화된 파일 저장
with open('config.enc', 'wb') as f:
    f.write(encrypted_data)

print("✅ 'config.ini' 파일이 'config.enc' 파일로 성공적으로 암호화되었습니다.")
print("배포 시에는 'config.enc' 파일만 포함하고, 'config.ini'는 제외해야 합니다.")

# 5. .gitignore에 추가 제안
gitignore_path = '.gitignore'
sensitive_files = ["\n# Security sensitive files\n", "config.ini\n", "secret.key\n"]

if not os.path.exists(gitignore_path):
    with open(gitignore_path, 'w') as f:
        f.writelines(sensitive_files)
    print(f"✅ 보안을 위해 '{gitignore_path}' 파일을 생성하고 'config.ini'를 추가했습니다.")
else:
    with open(gitignore_path, 'a+') as f:
        f.seek(0)
        content = f.read()
        if 'config.ini' not in content:
            f.writelines(sensitive_files)
            print(f"✅ 보안을 위해 '{gitignore_path}' 파일에 'config.ini'를 추가했습니다.")