import time
from datetime import datetime
from circular_zscore_module import load_zscore_data, detect_zscores_for_all_cams, update_zscore_direct

user = 'ERPMAN'
password = 'ERPMAN'
host = '119.195.124.33'
service_name = 'DEMOERP'

start_time = time.time()
start_dt = datetime.now()

print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - start! ")

df = load_zscore_data(user, password, host, service_name)

end_time = time.time()
duration = end_time - start_time
print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ({duration:.2f}초 경과) : load_zscore_data")

df = detect_zscores_for_all_cams(df)

end_time = time.time()
duration = end_time - start_time
print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ({duration:.2f}초 경과) : detect_zscores_for_all_cams")

update_zscore_direct(df, user, password, host, service_name)
