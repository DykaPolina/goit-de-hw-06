"""Читає топік алертів і виводить кожне повідомлення як JSON."""
import json

from kafka import KafkaConsumer

from configs import ALERTS_TOPIC, kafka_config

consumer = KafkaConsumer(
    ALERTS_TOPIC,
    **kafka_config,
    group_id="polina_dyka_alerts_consumer",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"Listening to '{ALERTS_TOPIC}'. Ctrl+C to stop.")
try:
    for record in consumer:
        print(f"[p{record.partition} o{record.offset}] key={record.key}")
        print(json.dumps(record.value, indent=2, ensure_ascii=False))
except KeyboardInterrupt:
    print("\nConsumer stopped.")
finally:
    consumer.close()
