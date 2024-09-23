import os
import csv
import json
from kafka import KafkaProducer

kafka_server = 'localhost:9092'
topic = 'ingestion-topic'
log_file_path = 'messages.log'

producer = KafkaProducer(bootstrap_servers=kafka_server,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def send_messages_from_csv(directory):
    with open(log_file_path, 'a', encoding='ISO-8859-1') as log_file:
        for filename in os.listdir(directory):
            if filename.endswith('.csv'):
                try:
                    filepath = os.path.join(directory, filename)
                    with open(filepath, mode='r', encoding='ISO-8859-1', newline='') as csvfile:
                        print(filepath)
                        reader = csv.reader(csvfile)
                        next(reader)
                        for row in reader:
                            message = ';'.join(row)
                            producer.send(topic, value=message)
                            log_file.write(message + '\n')
                            print(f'Sent: {message}')
                except:
                    print(f'File {filepath} is empty!')

if __name__ == "__main__":
    path = '../Fonte de Dados/Reclamações/'
    send_messages_from_csv(path)
    producer.flush()
    producer.close()