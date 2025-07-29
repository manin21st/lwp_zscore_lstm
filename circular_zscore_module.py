import pandas as pd
import numpy as np
import cx_Oracle
from scipy.stats import circmean, circstd
import os
from config_manager import load_encrypted_config

# 오라클 클라이언트 경로 설정 (Windows 환경 기준)
# os.environ["PATH"] = r"C:\instantclient_18_5;" + os.environ["PATH"]
client_path = os.getenv("ORACLE_CLIENT_PATH")
if client_path:
    os.environ["PATH"] = client_path + ";" + os.environ["PATH"]

config = load_encrypted_config()

def angular_difference(a, b):
    return (a - b + 180) % 360 - 180

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

def load_zscore_data():
    user = config['database']['user']
    password = config['database']['password']
    host = config['database']['host']
    service_name = config['database']['service_name']

    query = """
        SELECT RDATE, CAM1, CAM2, CAM3, CAM4, CAM5, CAM6
        FROM ZSCORE2
        WHERE ROWNUM < 5001
        ORDER BY RDATE
    """
    dsn = cx_Oracle.makedsn(host, 1521, service_name=service_name)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    df = pd.read_sql(query, conn)
    conn.close()
    # 🔽 여기 수정 (날짜포맷 혼용 대응 가능)
    # df['RDATE'] = pd.to_datetime(df['RDATE'])
    df['RDATE'] = pd.to_datetime(df['RDATE'], format='mixed', errors='coerce')
    # index 지정 제거
    # df.set_index('RDATE', inplace=True)
    return df
def update_zscore_to_oracle(df, user, password, host, service_name):
    user = config['database']['user']
    password = config['database']['password']
    host = config['database']['host']
    service_name = config['database']['service_name']

    dsn = cx_Oracle.makedsn(host, 1521, service_name=service_name)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()
    update_sql = """
        UPDATE ZSCORE2
        SET ZSC_CAM1 = :1, ZSC_CAM2 = :2, ZSC_CAM3 = :3,
            ZSC_CAM4 = :4, ZSC_CAM5 = :5, ZSC_CAM6 = :6
        WHERE RDATE = :7
    """
    for idx, row in df.iterrows():
        if pd.isna(row['ZSC_CAM1']):
            continue  # 아직 계산되지 않은 행은 skip
        cursor.execute(update_sql, (
            float(row['ZSC_CAM1']), float(row['ZSC_CAM2']), float(row['ZSC_CAM3']),
            float(row['ZSC_CAM4']), float(row['ZSC_CAM5']), float(row['ZSC_CAM6']),
            idx.strftime('%Y-%m-%d %H:%M:%S')
        ))
    conn.commit()
    cursor.close()
    conn.close()

def update_zscore_direct(df, chunk_size=5000):
    from datetime import datetime
    import time

    user = config['database']['user']
    password = config['database']['password']
    host = config['database']['host']
    service_name = config['database']['service_name']

    dsn = cx_Oracle.makedsn(host, 1521, service_name=service_name)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()

    update_sql = """
        UPDATE ZSCORE
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
            row['RDATE'].to_pydatetime()  # ✅ 원본 컬럼 RDATE 사용
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
