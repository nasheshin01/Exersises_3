import pika
import pickle
import numpy as np
import json
import time


INIT_CHANNEL_TRY_COUNT = 10


def init_channel():
    try_count = 0
    while try_count < INIT_CHANNEL_TRY_COUNT:
        try:
            time.sleep(10)
            try_count += 1

            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            channel = connection.channel()

            channel.queue_declare(queue='features')
            channel.queue_declare(queue='y_pred')

            return channel
        except Exception as e:
            print(f'Не удалось подключиться к серверу с попытки {try_count}: {e}')

    print(f'Не удалось подключиться к серверу за {INIT_CHANNEL_TRY_COUNT} попыток - выход из приложения')
    return None


def send_message(channel, id, body, queue_name):
    message = json.dumps({ 'id': id,'body': body })
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    print(f'Сообщение {message} отправлено в очередь {queue_name}')


def callback(channel, regressor, body):
    message = json.loads(body)
    message_id, message_body = message['id'], message['body']

    features_array = np.array(message_body).reshape(1, -1)
    prediction = regressor.predict(features_array)[0]

    send_message(channel, message_id, prediction, 'y_pred')


def main():
    with open('src/myfile.pkl', 'rb') as pkl_file:
        regressor = pickle.load(pkl_file)

    channel = init_channel()
    if channel is None:
        return

    channel.basic_consume(
        queue='features',
        on_message_callback=lambda ch, method, properties, body: callback(channel, regressor, body),
        auto_ack=True
    )

    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()

if __name__ == "__main__":
    main()
