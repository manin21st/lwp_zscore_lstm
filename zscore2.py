import time
from datetime import datetime
from circular_zscore_module import load_zscore_data, detect_zscores_for_all_cams, update_zscore_direct, insert_zscore_to_zscore3

start_time = time.time()
start_dt = datetime.now()

print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - start! ")

df = load_zscore_data()

end_time = time.time()
duration = end_time - start_time
print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ({duration:.2f}초 경과) : load_zscore_data")

df = detect_zscores_for_all_cams(df)

end_time = time.time()
duration = end_time - start_time
print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ({duration:.2f}초 경과) : detect_zscores_for_all_cams")

#insert_zscore_to_zscore3(df)
update_zscore_direct(df)

end_time = time.time()
duration = end_time - start_time
print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ({duration:.2f}초 경과) : insert_zscore_to_zscore3")

total_duration = time.time() - start_time
print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 모든 작업이 완료되었습니다.")
print(f"총 실행 시간: {total_duration:.2f}초")
