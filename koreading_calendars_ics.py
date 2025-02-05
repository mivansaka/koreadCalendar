import sqlite3
import pandas as pd
import sys
from icalendar import Calendar, Event  # 新增依赖库

# 连接 SQLite 数据库
db_path = "statistics.sqlite3"
try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
except sqlite3.Error as e:
    print(f"数据库连接失败: {e}")
    sys.exit(1)

# 查询阅读数据，并关联书名
query = """
    SELECT book.title, page_stat_data.start_time, page_stat_data.duration
    FROM page_stat_data
    JOIN book ON page_stat_data.id_book = book.id
    ORDER BY page_stat_data.start_time ASC;
"""
try:
    cursor.execute(query)
    data = cursor.fetchall()
except sqlite3.Error as e:
    print(f"数据库查询失败: {e}")
    conn.close()
    sys.exit(1)

# 关闭数据库连接
conn.close()

# 确保数据不为空
if not data:
    print("未找到阅读记录。")
    sys.exit(1)

# 转换为 DataFrame
df = pd.DataFrame(data, columns=["title", "start_time", "duration"])

# 确保 start_time 和 duration 有效
df.dropna(inplace=True)

# 转换时间格式并处理时区（UTC时间戳 -> 北京时间）
df["start_time"] = pd.to_datetime(df["start_time"], unit="s", utc=True).dt.tz_convert('Asia/Shanghai')  # 修改时区处理
df["end_time"] = df["start_time"] + pd.to_timedelta(df["duration"], unit="s")  # 自动继承时区

# 按照 start_time 排序
df.sort_values("start_time", inplace=True)

# 合并相邻小于 600 秒间隔的阅读记录
merged_sessions = []
current_title, current_start, current_end = None, None, None
merge_threshold = pd.Timedelta(seconds=600)

for _, row in df.iterrows():
    if current_title == row["title"] and (row["start_time"] - current_end) <= merge_threshold:
        current_end = row["end_time"]
    else:
        if current_title is not None:
            merged_sessions.append((current_title, current_start, current_end))
        current_title, current_start, current_end = row["title"], row["start_time"], row["end_time"]

if current_title is not None:
    merged_sessions.append((current_title, current_start, current_end))

# 创建ICS日历（新增部分）
cal = Calendar()
cal.add("prodid", "-//Reading Calendar//example.com//")
cal.add("version", "2.0")

for title, start, end in merged_sessions:
    event = Event()
    event.add("summary", title)
    event.add("dtstart", start.to_pydatetime())  # 转换时区感知的datetime对象
    event.add("dtend", end.to_pydatetime())
    cal.add_component(event)

# 保存为ICS文件
output_path = "reading_schedule.ics"
try:
    with open(output_path, "wb") as f:  # 注意使用二进制写入
        f.write(cal.to_ical())
    print(f"日历文件已生成: {output_path}")
except Exception as e:
    print(f"文件写入失败: {e}")
    sys.exit(1)