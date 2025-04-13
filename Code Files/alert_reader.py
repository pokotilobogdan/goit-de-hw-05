from kafka import KafkaConsumer
from configs import kafka_config
import json


alert_consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group'   # Ідентифікатор групи споживачів
    )

my_name = 'Bohdan'
topics = [f'temperature_alerts_{my_name}', f'humidity_alerts_{my_name}']

alert_consumer.subscribe(topics)

print("Listening for alerts...")

try:
    for message in alert_consumer:
        print(message.value)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    alert_consumer.close()  # Закриття consumer

