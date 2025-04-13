from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
from random import randint

# create Kafka Producer

    # На жаль, я не розібрався, як простим чином автоматично брати client_id з кожного producer,
    # тому створю окрему змінну. Для даної задачі цього буде достатньо
client_id = str(uuid.uuid4())

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    client_id=client_id,    # тут додаємо унікальне ім'я для кожного запуску скрипта
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Куди producers посилатимуть дані:
my_name = "Bohdan"
topic_name = f'building_sensors_{my_name}'

for i in range(30):
    try:
        # Дані, які треба послати
        data = {
            "temperature": randint(25, 45),
            "humidity": randint(15, 85)
        }
        # Посилаємо дані в конкретний топік. Ключ - унікальний ідентифікатор повідомлення
        producer.send(topic_name,
                      key=str(uuid.uuid4()),
                      value=data,
                      headers=[('client_id', client_id.encode('utf-8')),
                               ('time', time.strftime("%d.%m.%Y %H:%M:%S").encode('utf-8'))])
        producer.flush()    # Очікування, поки всі повідомлення будуть відправлені
        
        print(f"Message {i} sent to topic '{topic_name}' successfully.")
        time.sleep(5)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Закриття producer
