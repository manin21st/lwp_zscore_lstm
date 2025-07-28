
# lstm_anomaly_module.py

import cx_Oracle
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense
import os

# 오라클 클라이언트 64bit 적용
os.environ["PATH"] = r"C:\instantclient_18_5;" + os.environ["PATH"]

# ====================
# 1. Oracle DB 연동
# ====================
def load_data_from_oracle(user, password, host, service_name, query):
    dsn = cx_Oracle.makedsn(host, 1521, service_name=service_name)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    df = pd.read_sql(query, conn)
    conn.close()
    df['측정시각'] = pd.to_datetime(df['측정시각'])
    df.set_index('측정시각', inplace=True)
    return df

def save_results_to_oracle(df_result, user, password, host, service_name):
    dsn = cx_Oracle.makedsn(host, 1521, service_name=service_name)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()

    insert_sql = """
        MERGE INTO SENSOR_ANOMALY_RESULT T
        USING (SELECT :ts 측정시각 FROM dual) S
        ON (T.측정시각 = S.측정시각)
        WHEN MATCHED THEN UPDATE SET
            센서값 = :val, zscore = :zs, z_outlier = :zo,
            pred = :pred, error = :err, lstm_anomaly = :la, 저장일시 = SYSDATE
        WHEN NOT MATCHED THEN INSERT (
            측정시각, 센서값, zscore, z_outlier, pred, error, lstm_anomaly, 저장일시
        ) VALUES (
            :ts, :val, :zs, :zo, :pred, :err, :la, SYSDATE
        )
    """

    for idx, row in df_result.iterrows():
        cursor.execute(insert_sql, {
            'ts': idx.to_pydatetime(),
            'val': float(row['센서값']),
            'zs': float(row['zscore']),
            'zo': 'Y' if row['z_outlier'] else 'N',
            'pred': float(row['pred']),
            'err': float(row['error']),
            'la': 'Y' if row['lstm_anomaly'] else 'N'
        })

    conn.commit()
    cursor.close()
    conn.close()
    print("이상 탐지 결과가 Oracle DB에 저장되었습니다.")

# ====================
# 2. Z-Score 이상치 탐지
# ====================
def detect_outliers_zscore(df, threshold=3):
    mean = df['센서값'].mean()
    std = df['센서값'].std()
    df['zscore'] = (df['센서값'] - mean) / std
    df['z_outlier'] = df['zscore'].abs() > threshold
    return df

# ====================
# 3. LSTM 데이터 전처리
# ====================
def create_lstm_dataset(values, window_size=10):
    X, y = [], []
    for i in range(len(values) - window_size):
        X.append(values[i:i+window_size])
        y.append(values[i+window_size])
    return np.array(X), np.array(y)

# ====================
# 4. 모델 학습 및 저장
# ====================
def train_and_save_lstm_model(df, window_size=10, model_path='lstm_model.h5'):
    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(df[['센서값']])
    X, y = create_lstm_dataset(scaled, window_size)
    X = X.reshape((X.shape[0], X.shape[1], 1))

    model = Sequential([
        LSTM(50, input_shape=(window_size, 1)),
        Dense(1)
    ])
    model.compile(optimizer='adam', loss='mse')
    model.fit(X, y, epochs=10, batch_size=16, verbose=0)
    model.save(model_path)
    return scaler

# ====================
# 5. 모델 로딩 및 예측
# ====================
def load_model_and_predict(df, scaler, window_size=10, model_path='lstm_model.h5', threshold=0.05):
    scaled = scaler.transform(df[['센서값']])
    X, y = create_lstm_dataset(scaled, window_size)
    X = X.reshape((X.shape[0], X.shape[1], 1))

    model = load_model(model_path)
    preds = model.predict(X)
    errors = np.abs(preds - y)

    df = df.iloc[window_size:].copy()
    df['pred'] = scaler.inverse_transform(preds)
    df['error'] = errors
    df['lstm_anomaly'] = df['error'] > threshold
    return df
