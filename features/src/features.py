import pika
import json
import time

import numpy as np

from sklearn.datasets import load_diabetes
from datetime import datetime

INIT_CHANNEL_TRY_COUNT = 10

def init_channel():
    try_count = 0
    while try_count < INIT_CHANNEL_TRY_COUNT:
        try:
            time.sleep(10)
            try_count += 1

            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            channel = connection.channel()

            channel.queue_declare(queue='y_true')
            channel.queue_declare(queue='features')

            return channel
        except Exception as e:
            print(f'Не удалось подключиться к серверу с попытки {try_count}: {e}')

    print(f'Не удалось подключиться к серверу за {INIT_CHANNEL_TRY_COUNT} попыток - выход из приложения')
    return None

def send_message(channel, id, body, queue_name):
    message = json.dumps({ 'id': id,'body': body })
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    print(f'Сообщение {message} отправлено в очередь {queue_name}')

def main():
    X, y = load_diabetes(return_X_y=True)
    channel = init_channel()
    if channel is None:
        return

    while True:
        try:
            time.sleep(10)

            random_row = np.random.randint(0, X.shape[0]-1)
            message_id = datetime.timestamp(datetime.now())

            send_message(channel, message_id, y[random_row], 'y_true')
            send_message(channel, message_id, list(X[random_row]), 'features')
        except:
            print('Не удалось подключиться к очереди')

if __name__ == "__main__":
    main()