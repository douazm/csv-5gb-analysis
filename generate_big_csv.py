import pandas as pd
import os
import time
import psutil

data_folder = "data"
files = [
    "yellow_tripdata_2015-01.csv",
    "yellow_tripdata_2016-01.csv",
    "yellow_tripdata_2016-02.csv",
    "yellow_tripdata_2016-03.csv"
]

chunk_size = 100000  # عدد الصفوف في كل جزء

total_size_bytes = 0
total_rows = 0
total_time = 0
total_mem_used = 0

def get_memory_usage_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

for file_name in files:
    file_path = os.path.join(data_folder, file_name)
    print(f"\n📂 جاري تحليل: {file_name}")

    # حساب حجم الملف
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = round(file_size_bytes / (1024**2), 2)
    file_size_gb = round(file_size_bytes / (1024**3), 2)
    total_size_bytes += file_size_bytes

    # قياس الزمن والذاكرة قبل القراءة
    start_time = time.time()
    start_mem = get_memory_usage_mb()

    # قراءة الملف على شكل chunks
    row_count = 0
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        row_count += len(chunk)

    total_rows += row_count

    # قياس الزمن والذاكرة بعد القراءة
    end_time = time.time()
    end_mem = get_memory_usage_mb()

    elapsed_time = round(end_time - start_time, 2)
    mem_used = round(end_mem - start_mem, 2)

    total_time += elapsed_time
    total_mem_used += mem_used

    # طباعة النتائج لكل ملف
    print(f"📦 حجم الملف: {file_size_mb} MB ({file_size_gb} GB)")
    print(f"📊 عدد الصفوف: {row_count}")
    print(f"⏱️ الزمن المستغرق: {elapsed_time} ثانية")
    print(f"💾 استهلاك الذاكرة: {mem_used} MB")

# النتائج النهائية
total_size_gb = round(total_size_bytes / (1024**3), 2)
print(f"\n📊 الحجم الإجمالي لكل الملفات: {total_size_gb} GB")
print(f"📊 العدد الإجمالي للصفوف: {total_rows}")
print(f"⏱️ الزمن الكلي: {total_time} ثانية")
print(f"💾 الاستهلاك الكلي للذاكرة: {total_mem_used} MB")
