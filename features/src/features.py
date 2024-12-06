import pika
import json
import time

import numpy as np

from sklearn.datasets import load_diabetes
from datetime import datetime


X, y = load_diabetes(return_X_y=True)

connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='y_true')
channel.queue_declare(queue='features')


while True:
    try:
        time.sleep(10)

        random_row = np.random.randint(0, X.shape[0]-1)
        message_id = datetime.timestamp(datetime.now()),

        y_true_json = json.dumps({
            'id': message_id,
            'body': y[random_row]
            })
        channel.basic_publish(exchange='', routing_key='y_true', body=y_true_json)
        print(f'Сообщение {y_true_json} с правильным ответом отправлено в очередь')

        features_json = json.dumps({
            'id': message_id,
            'body': list(X[random_row])
            })
        channel.basic_publish(exchange='', routing_key='features', body=features_json)
        print(f'Сообщение {features_json} с вектором признаков отправлено в очередь')
    except:
        print('Не удалось подключиться к очереди')
   