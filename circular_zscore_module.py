import pandas as pd
import numpy as np
import cx_Oracle
from scipy.stats import circmean, circstd
import os
import time
from datetime import datetime
from config_manager import load_encrypted_config

# 오라클 클라이언트 경로 설정 (Windows 환경 기준)
# os.environ["PATH"] = r"C:\instantclient_18_5;" + os.environ["PATH"]
client_path = os.getenv("ORACLE_CLIENT_PATH")
if client_path:
    os.environ["PATH"] = client_path + ";" + os.environ["PATH"]

# 오라클 DB 접속 정보
config = load_encrypted_config()
user = config['database']['user']
password = config['database']['password']
host = config['database']['host']
service_name = config['database']['service_name']

# 원형 각도 차이 계산
def angular_difference(a, b):
    return (a - b + 180) % 360 - 180

# 원형 각도에 대한 zscore 계산
def detect_zscores_for_all_cams(df, window_size=30):
    from scipy.stats import circmean, circstd

    cam_columns = ['CAM1', 'CAM2', 'CAM3', 'CAM4', 'CAM5', 'CAM6']
    zscore_result = {}

    for cam in cam_columns:
        data = df[cam].values
        zscores = np.full(len(df), np.nan)

        for i in range(window_size, len(data)):
            window = data[i - window_size:i]

            # numpy로 고속 처리
            sin_vals = np.sin(np.deg2rad(window))
            cos_vals = np.cos(np.deg2rad(window))

            avg_angle_rad = np.arctan2(np.mean(sin_vals), np.mean(cos_vals))
            mu = np.rad2deg(avg_angle_rad)

            # 원형 표준편차 계산
            R = np.sqrt(np.mean(sin_vals)**2 + np.mean(cos_vals)**2)
            sigma = np.sqrt(-2 * np.log(R)) * (180 / np.pi)  # 라디안 → 도

            if sigma < 0.1:
                sigma = 0.1

            # 현재 값의 원형 차이
            diff = (data[i] - mu + 180) % 360 - 180
            z = diff / sigma
            zscores[i] = z

        zscore_result[f'ZSC_{cam}'] = zscores

    # 결과를 DataFrame에 붙이기
    for col, vals in zscore_result.items():
        df[col] = vals

    return df

# 측정된 CAM 위상각 조회
def load_zscore_data():
    query = """
        SELECT BARCODE, MODEL_NAME, LINE_NO, RDATE, CAM1, CAM2, CAM3, CAM4, CAM5, CAM6
        FROM ZSCORE2
        --WHERE ROWNUM < 5001
        ORDER BY RDATE
    """
    dsn = cx_Oracle.makedsn(host, 1521, service_name=service_name)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# 계산된 zscore 업데이트
def update_zscore_direct(df, chunk_size=5000):
    from datetime import datetime
    import time

    dsn = cx_Oracle.makedsn(host, 1521, service_name=service_name)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()

    update_sql = """
        UPDATE ZSCORE2
        SET ZSC_CAM1 = :1, ZSC_CAM2 = :2, ZSC_CAM3 = :3,
            ZSC_CAM4 = :4, ZSC_CAM5 = :5, ZSC_CAM6 = :6
        WHERE RDATE = :7
    """

    df_update = df.dropna(subset=[
        'ZSC_CAM1', 'ZSC_CAM2', 'ZSC_CAM3',
        'ZSC_CAM4', 'ZSC_CAM5', 'ZSC_CAM6'
    ])

    update_data = [
        (
            float(row['ZSC_CAM1']), float(row['ZSC_CAM2']), float(row['ZSC_CAM3']),
            float(row['ZSC_CAM4']), float(row['ZSC_CAM5']), float(row['ZSC_CAM6']),
            row['RDATE']
        )
        for _, row in df_update.iterrows()
    ]

    total_rows = len(update_data)
    print(f"📦 총 {total_rows:,}건 갱신 준비됨")

    start_time = time.time()
    start_dt = datetime.now()
    print(f"🕒 UPDATE 시작: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")

    for i in range(0, total_rows, chunk_size):
        chunk = update_data[i:i + chunk_size]
        cursor.executemany(update_sql, chunk)
        conn.commit()
        print(f"  🔄 Chunk {i // chunk_size + 1}: {len(chunk):,}건 완료")

    end_time = time.time()
    end_dt = datetime.now()
    duration = end_time - start_time

    print(f"\n✅ UPDATE 종료: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⌛ 총 소요 시간: {duration:.2f}초")
    print(f"📈 처리 속도: {total_rows / duration:.2f}건/초")

    cursor.close()
    conn.close()

# 계산된 zscore 복제 테이블에 인서트
def insert_zscore_to_zscore3(df, chunk_size=5000):
    """
    ZSCORE3 테이블의 모든 데이터를 삭제한 후,
    계산된 Z-Score를 포함한 데이터를 새로 삽입합니다.
    """

    dsn = cx_Oracle.makedsn(host, 1521, service_name=service_name)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()

    # --- 기존 데이터 삭제 ---
    print("🗑️  ZSCORE3 테이블의 기존 데이터를 삭제합니다...")
    cursor.execute("DELETE FROM ZSCORE3")
    print(f"✅ {cursor.rowcount:,}건의 기존 데이터 삭제 완료.")
    conn.commit()

    # ZSCORE3 테이블에 삽입할 INSERT 문
    insert_sql = """
        INSERT INTO ZSCORE3 (
            BARCODE, MODEL_NAME, LINE_NO, RDATE,
            CAM1, CAM2, CAM3, CAM4, CAM5, CAM6,
            ZSC_CAM1, ZSC_CAM2, ZSC_CAM3, ZSC_CAM4, ZSC_CAM5, ZSC_CAM6
        ) VALUES (
            :1, :2, :3,
            :4,
            :5, :6, :7, :8, :9, :10,
            :11, :12, :13, :14, :15, :16
        )
    """

    # executemany에 사용할 데이터 리스트 생성
    # Z-Score가 없는(NaN) 행도 포함하며, 이 경우 DB에 NULL로 입력되도록 None으로 변환합니다.
    insert_data = [
        (
            row['BARCODE'],
            row['MODEL_NAME'],
            row['LINE_NO'],
            row['RDATE'],
            float(row['CAM1']), float(row['CAM2']), float(row['CAM3']),
            float(row['CAM4']), float(row['CAM5']), float(row['CAM6']),
            None if pd.isna(row['ZSC_CAM1']) else float(row['ZSC_CAM1']),
            None if pd.isna(row['ZSC_CAM2']) else float(row['ZSC_CAM2']),
            None if pd.isna(row['ZSC_CAM3']) else float(row['ZSC_CAM3']),
            None if pd.isna(row['ZSC_CAM4']) else float(row['ZSC_CAM4']),
            None if pd.isna(row['ZSC_CAM5']) else float(row['ZSC_CAM5']),
            None if pd.isna(row['ZSC_CAM6']) else float(row['ZSC_CAM6'])
        )
        for _, row in df.iterrows()
    ]

    total_rows = len(insert_data)
    print(f"📦 총 {total_rows:,}건 ZSCORE3에 삽입 준비됨")

    start_time = time.time()
    print(f"🕒 INSERT 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 데이터를 chunk 단위로 나누어 삽입
    for i in range(0, total_rows, chunk_size):
        chunk = insert_data[i:i + chunk_size]
        cursor.executemany(insert_sql, chunk)
        conn.commit()

    duration = time.time() - start_time
    print(f"\n✅ INSERT 종료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⌛ 총 소요 시간: {duration:.2f}초")
    if duration > 0:
        print(f"📈 처리 속도: {total_rows / duration:.2f}건/초")

    cursor.close()
    conn.close()
