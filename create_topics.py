"""Створення вхідного топіку з даними датчиків і вихідного топіку алертів."""
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import ALERTS_TOPIC, MY_NAME, SENSORS_TOPIC, kafka_config

admin_client = KafkaAdminClient(**kafka_config)

for name in (SENSORS_TOPIC, ALERTS_TOPIC):
    try:
        admin_client.create_topics(
            new_topics=[NewTopic(name=name, num_partitions=2, replication_factor=1)],
            validate_only=False,
        )
        print(f"Topic '{name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{name}' already exists.")

time.sleep(1)  # даємо брокеру оновити метадані
print("\nMy topics:")
[print(topic) for topic in admin_client.list_topics() if MY_NAME in topic]

admin_client.close()
