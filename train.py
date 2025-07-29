
# train.py
from lstm_anomaly_module import load_data_from_oracle, detect_outliers_zscore, train_and_save_lstm_model
import pickle

# Oracle 쿼리
query = """
    SELECT 측정시각, 센서값
    FROM SENSOR_ANOMALY_RESULT
    ORDER BY 측정시각
"""

# 1. 데이터 로드
df = load_data_from_oracle(query)

# 2. Z-Score 이상 감지
df = detect_outliers_zscore(df)

# 3. 모델 학습 및 scaler 저장
scaler = train_and_save_lstm_model(df, window_size=60, model_path='lstm_model.keras')

# 4. Scaler 객체 저장
with open('scaler.pkl', 'wb') as f:
    pickle.dump(scaler, f)

print("모델 및 스케일러 저장 완료 (lstm_model.h5, scaler.pkl)")
