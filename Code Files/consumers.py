from kafka import KafkaConsumer
from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
from colorama import Fore


# Створення Kafka Consumer
consumer = KafkaConsumer(
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


# Створимо producer, котрий надсилатиме alerts
alert_producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# Назва топіку, з котрого зчитуємо дані
my_name = "Bohdan"
topic_name = f'building_sensors_{my_name}'

# Підписка на тему
consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

# Обробка повідомлень з топіку
while True:
    try:
        for message in consumer:
        
            headers = dict(message.headers or [])

            producer_id = headers.get('client_id').decode('utf-8')
            message_time = headers.get('time').decode('utf-8')
            temperature = message.value['temperature']
            humidity = message.value['humidity']

            # Зробимо output красивішим
            print()
            print("Producer: " + Fore.YELLOW + f"{producer_id}")
            print(Fore.GREEN + f"{message_time}" + Fore.RESET)
            print("\ttemperature: " + (Fore.RED if temperature > 40 else "") + f"{temperature}°C" + Fore.RESET)
            print("\thumidity: "  + (Fore.RED if humidity < 20 or humidity > 80 else "") + f"{humidity}%" + Fore.RESET)
            print()
        
            # Опрацюємо результат
            if temperature > 40:
                alert = Fore.RED + "Temperature is too high!" + Fore.RESET
                alert_producer.send("temperature_alerts_Bohdan", key=str(uuid.uuid4()), value=alert, headers=[('producer_id', headers.get('client_id'))])
        
            if humidity < 20:
                alert = Fore.RED + "Humidity is too low!" + Fore.RESET
                alert_producer.send("humidity_alerts_Bohdan", key=str(uuid.uuid4()), value=alert, headers=[('producer_id', headers.get('client_id'))])
        
            if humidity > 80:
                alert = Fore.RED + "Humidity is too high!" + Fore.RESET
                alert_producer.send("humidity_alerts_Bohdan", key=str(uuid.uuid4()), value=alert, headers=[('producer_id', headers.get('client_id'))])


    except Exception as e:
        print(f"An error occurred: {e}")

    consumer.close()  # Закриття consumer

