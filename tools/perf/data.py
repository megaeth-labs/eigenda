import json
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

# 获取当前时间字符串
current_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")

# 读取并过滤数据
data = []
#/nvme0/eigenda/eigenda_data-07241917.log
#with open('/nvme0/eigenda/eigenda_data-07241917.log', 'r') as file:
with open('/Users/clay/workspace/eigenda/tools/perf/eigenda07291408.log', 'r') as file:
    i =0
    for line in file:
        i = i + 1
        print("i:", i)
        if not line.startswith("{"):
            print("hhhhhhhhhhh")
            continue
        log_entry = json.loads(line)
        if log_entry.get('msg', '').startswith('MegaETH'):
            data.append(log_entry)

# 将数据转化为DataFrame
df = pd.DataFrame(data)

# 提取blobKey和时间戳
df['blobKey'] = df['info']
df['time'] = pd.to_datetime(df['time'])

# 按blobKey分组，计算每个阶段花费的时间
df = df.sort_values(by=['blobKey', 'time'])  # 先按blobKey和time排序
df['previous_time'] = df.groupby('blobKey')['time'].shift(1)
df['time_diff'] = (df['time'] - df['previous_time']).dt.total_seconds()

# 创建新的DataFrame来记录每个blob的开始和结束时间
start_end_times = df.groupby('blobKey').agg(start_time=('time', 'first'), end_time=('time', 'last')).reset_index()

# 计算每分钟完成处理的blob数量
start_end_times['minute'] = start_end_times['end_time'].dt.floor('min')
completed_blobs_per_minute = start_end_times.groupby('minute')['blobKey'].count()

average_blobs_per_minute = completed_blobs_per_minute.mean()

# 计算每秒完成处理的blob数量
start_end_times['second'] = start_end_times['end_time'].dt.floor('s')
completed_blobs_per_second = start_end_times.groupby('second')['blobKey'].count()

# 假设每个blob的大小为2 MB
blob_size_mb = 2

# 计算每分钟的吞吐量 (MB)
throughput_per_minute = completed_blobs_per_minute * blob_size_mb

# 计算每秒的吞吐量 (MB)
throughput_per_second = completed_blobs_per_second * blob_size_mb

# 按分钟计算每分钟的平均时间消耗
average_time_per_minute = df.groupby(df['time'].dt.floor('min'))['time_diff'].mean()

# 绘制每分钟完成处理的blob数量折线图
plt.figure(figsize=(14, 7))
plt.plot(completed_blobs_per_minute.index, completed_blobs_per_minute.values, marker='o', linestyle='-')
plt.title('Number of Completed Blobs Per Minute')
plt.xlabel('Time (Minute)')
plt.ylabel('Number of Blobs')
plt.grid(True)
plt.savefig(f'completed_blobs_per_minute_{current_time_str}.png')
plt.show()

# 绘制每分钟的平均时间消耗折线图
plt.figure(figsize=(14, 7))
plt.plot(average_time_per_minute.index, average_time_per_minute.values, marker='o', linestyle='-')
plt.title('Average Time Spent on Each Stage Per Minute')
plt.xlabel('Time (Minute)')
plt.ylabel('Average Time Spent (seconds)')
plt.grid(True)
plt.savefig(f'average_time_per_minute_{current_time_str}.png')
plt.show()

# 输出结果
print("各个阶段处理所花费的时间：")
print(df[['blobKey', 'component', 'time_diff']])

print("\n每分钟完成处理的blob数量：")
print(completed_blobs_per_minute)

print("\n每秒完成处理的blob数量：")
print(completed_blobs_per_second)

print(f"\n平均每分钟处理的blob数量： {average_blobs_per_minute:.2f}")


print("\n每分钟的吞吐量 (MB)：")
print(throughput_per_minute)

print("\n每秒的吞吐量 (MB)：")
print(throughput_per_second)

# 将结果保存为CSV文件，文件名中包含时间戳
df[['blobKey', 'component', 'time_diff']].to_csv(f'time_diff_per_blob_{current_time_str}.csv', index=False)
throughput_per_minute.to_csv(f'throughput_per_minute_{current_time_str}.csv', index=True, header=['throughput_mb'])
throughput_per_second.to_csv(f'throughput_per_second_{current_time_str}.csv', index=True, header=['throughput_mb'])
