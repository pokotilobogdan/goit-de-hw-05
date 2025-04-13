from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from configs import kafka_config
from colorama import Fore


# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)


# Визначення нових топіків
my_name = "Bohdan"
num_partitions = 2
replication_factor = 1

topics = []
topic_names = ['building_sensors', 'temperature_alerts', 'humidity_alerts']

for topic_name in topic_names:
    topics.append(NewTopic(name=f'{topic_name}_{my_name}', num_partitions=num_partitions, replication_factor=replication_factor))

# building_sensors = NewTopic(name=f'building_sensors_{my_name}', num_partitions=num_partitions, replication_factor=replication_factor)
# temperature_alerts = NewTopic(name=f'temperature_alerts_{my_name}', num_partitions=num_partitions, replication_factor=replication_factor)
# humidity_alerts = NewTopic(name=f'humidity_alerts_{my_name}', num_partitions=num_partitions, replication_factor=replication_factor)


# Створення нових топіків
for topic in topics:
    try:
    # Longer code just to check every topic:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(Fore.GREEN + "Topic "+ Fore.YELLOW + f"{topic.name}" + Fore.GREEN +" created successfully." + Fore.RESET)
    # admin_client.create_topics(new_topics=topics, validate_only=False)
    # print("Topics created successfully.")
    except TopicAlreadyExistsError:
        print(Fore.RED + "Topic " + Fore.YELLOW + f"{topic.name}" + Fore.RED + " already exists" + Fore.RESET)
    except Exception as e:
        print("An error occurred:" + Fore.RED + f"{e}" + "already exists" + Fore.RESET)    
print()


# Перевіряємо список існуючих топіків
print(Fore.BLUE + "All topics on the server:" + Fore.RESET)
print(admin_client.list_topics())


# Закриття зв'язку з клієнтом
admin_client.close()
