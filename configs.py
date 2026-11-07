"""Спільні налаштування Kafka для всіх скриптів.

За замовчуванням — локальний брокер з docker-compose.yml (localhost:9092, без автентифікації).
Для віддаленого кластера задайте змінні оточення:
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_USERNAME, KAFKA_PASSWORD
(тоді використовується SASL_PLAINTEXT / PLAIN).
"""
import os

MY_NAME = os.getenv("KAFKA_TOPIC_PREFIX", "polina_dyka")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")

# Налаштування для kafka-python (producer / consumer / admin)
kafka_config = {"bootstrap_servers": BOOTSTRAP_SERVERS.split(",")}
if KAFKA_USERNAME:
    kafka_config.update(
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
    )

# Ті самі налаштування у форматі опцій Spark Kafka source / sink
spark_kafka_options = {"kafka.bootstrap.servers": BOOTSTRAP_SERVERS}
if KAFKA_USERNAME:
    spark_kafka_options.update({
        "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
        ),
    })

SENSORS_TOPIC = f"{MY_NAME}_building_sensors"
ALERTS_TOPIC = f"{MY_NAME}_alerts"
