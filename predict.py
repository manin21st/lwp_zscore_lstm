
# predict.py
from lstm_anomaly_module import load_data_from_oracle, detect_outliers_zscore, load_model_and_predict
import pickle

# Oracle 접속 정보
user = 'ERPMAN'
password = 'ERPMAN'
host = '119.195.124.33'
service_name = 'DEMOERP'
query = """
    SELECT 측정시각, 센서값
    FROM SENSOR_ANOMALY_RESULT
    ORDER BY 측정시각
"""

# 1. 데이터 로드
df = load_data_from_oracle(user, password, host, service_name, query)

# 2. Z-Score 이상 감지
df = detect_outliers_zscore(df)

# 3. 저장된 scaler 불러오기
with open('scaler.pkl', 'rb') as f:
    scaler = pickle.load(f)

# 4. 모델 로딩 및 예측
df_result = load_model_and_predict(df, scaler, window_size=60, model_path='lstm_model.h5', threshold=0.05)

# 5. 이상치 출력
print(df_result[df_result['lstm_anomaly'] | df_result['z_outlier']][['센서값', 'zscore', 'z_outlier', 'pred', 'error', 'lstm_anomaly']])

# 6. 이상 탐지 결과 DB 저장
save_results_to_oracle(df_result, user, password, host, service_name)
