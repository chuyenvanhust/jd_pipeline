import os 
from confluent_kafka import Producer
import json
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1] 
# tao producer
producer = Producer({
    "bootstrap.servers": "localhost:9092" 
})

#tim folder data moi nhat
def get_latest_file(folder_path):
    
    find = sorted(os.listdir(folder_path), reverse=True)
    latest_file = find[0]
    return os.path.join(folder_path, latest_file)

# upload file json
def upload_json_file(file_path):
    print(f"Tìm thấy file raw, bắt đầu upload lên Kafka: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        jobs = json.load(f)       # Load cả list

    for i, job in enumerate(jobs, 1):
        producer.produce(
            topic = "job_raw",
            key   = job["job_id"].encode('utf-8'),            # Key = job_id
            value = json.dumps(job, ensure_ascii=False).encode("utf-8")           # Value = toàn bộ job
        )
        if i % 100 == 0:
            producer.poll(0)  # flush nhỏ mỗi 100 jobs
            print(f"  → {i}/{len(jobs)} jobs")
    producer.flush() 
    print(f"\nHoàn thành: upload {len(jobs)} raw file → Kafka topic 'job_raw'")

if __name__ == "__main__":
    
    latest_session = get_latest_file(PROJECT_ROOT / "docker" / "storage" / "data_lake" / "bronze" / "topcv")
    print(f"folder moi nhat: {latest_session}")
    json_file = os.path.join(latest_session, "topcv_jobs.json")  # ← thêm dòng này
    upload_json_file(json_file)