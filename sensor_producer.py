"""Крок 1. Імітація одного датчика: щосекунди надсилає id, temperature, humidity, timestamp
в топік building_sensors.

Один запуск = один датчик зі сталим випадковим id. Щоб імітувати кілька датчиків,
запустіть скрипт у кількох терміналах.

Показники змінюються випадковим блуканням (а не незалежно на кожному кроці), тому
середні за хвилину значення поступово дрейфують і час від часу виходять за межі
з alerts_conditions.csv — інакше середнє з ~60 незалежних випадкових значень завжди
було б близьким до середини діапазону й алертів майже не було б.
"""
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from configs import SENSORS_TOPIC, kafka_config

SEND_INTERVAL_SEC = 1
TEMP_RANGE = (15.0, 55.0)
HUM_RANGE = (10.0, 90.0)

sensor_id = random.randint(1000, 9999)  # сталий для одного запуску
temperature = random.uniform(*TEMP_RANGE)
humidity = random.uniform(*HUM_RANGE)


def step(value: float, low: float, high: float, max_delta: float) -> float:
    """Один крок випадкового блукання з відбиттям від меж діапазону."""
    value += random.uniform(-max_delta, max_delta)
    if value < low:
        value = 2 * low - value
    elif value > high:
        value = 2 * high - value
    return value


producer = KafkaProducer(
    **kafka_config,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
)

print(f"Sensor {sensor_id} started, sending to '{SENSORS_TOPIC}'. Ctrl+C to stop.")
try:
    while True:
        temperature = step(temperature, *TEMP_RANGE, max_delta=2.0)
        humidity = step(humidity, *HUM_RANGE, max_delta=3.0)
        data = {
            "id": sensor_id,
            "temperature": round(temperature, 1),
            "humidity": round(humidity, 1),
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }
        # ключ = id датчика: повідомлення одного датчика потрапляють в одну партицію
        producer.send(SENSORS_TOPIC, key=sensor_id, value=data)
        producer.flush()
        print(f"Sent: {data}")
        time.sleep(SEND_INTERVAL_SEC)
except KeyboardInterrupt:
    print(f"\nSensor {sensor_id} stopped.")
finally:
    producer.close()
