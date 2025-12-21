import logging
import time
import json
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

logger = logging.getLogger('produce_sensor_data')

BOOTSTRAP_SERVERS = ['localhost:29092']
TOPIC_NAME = 'smartfarm-sensor'

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_sensor_data(sensor_id):
    """
    스마트팜 센서 데이터 생성 함수
    정상 범위와 가끔 발생하는 이상 수치(Anomaly)를 시뮬레이션
    """
    # 95% 확률로 정상 데이터, 5% 확률로 이상 데이터(고온) 생성
    if random.random() < 0.95:
        temp = round(random.uniform(20.0, 30.0), 2)  # 정상: 20~30도
        status = "NORMAL"
    else:
        temp = round(random.uniform(35.0, 45.0), 2)  # 이상: 35도 이상
        status = "WARNING"

    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": temp,
        "humidity": round(random.uniform(40.0, 80.0), 2),
        "co2_level": random.randint(300, 600),
        "battery_voltage": round(random.uniform(3.0, 4.2), 2),
        "status": status,
    }

if __name__ == "__main__":
    print(f"🚀 Sending sensor data to Kafka topic: {TOPIC_NAME}")
    print("Press Ctrl+C to stop...")

    try:
        sensor_ids = ["sensor_A", "sensor_B", "sensor_C", "sensor_D", "sensor_E"]

        while True:
            for s_id in sensor_ids:
                data = generate_sensor_data(s_id)
                producer.send(TOPIC_NAME, value=data)
            
            producer.flush()
            
            print(f"✅ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Sent batch data: {len(sensor_ids)} events")
            
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
        producer.close()