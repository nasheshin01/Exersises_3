import pika
import json
import time


INIT_CHANNEL_TRY_COUNT = 10


def send_message(channel, id, body, queue_name):
    message = json.dumps({ 'id': id,'body': body })
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    print(f'Сообщение {message} отправлено в очередь {queue_name}')


def process_abs_error(channel, message_id, y_dict):
    with open('./logs/metric_log.csv', 'a') as file:
        abs_error = abs(y_dict[message_id][0] - y_dict[message_id][1])
        file.write(f'{message_id},{y_dict[message_id][0]},{y_dict[message_id][1]},{y_dict[message_id][1]},{abs_error}\n')

    send_message(channel, message_id, abs_error, 'abs_error')


def y_pred_callback(channel, body, y_dict):
    try:
        message = json.loads(body)
        message_id, message_body = message['id'], message['body']

        if message_id in y_dict:
            y_dict[message_id][1] = message_body
            process_abs_error(channel, message_id, y_dict)
        else:
            y_dict[message_id] = [None, message_body]
    except Exception as e:
        print(f'Ошибка обработки сообщения {body} из очереди y_pred: {e}')


def y_true_callback(channel, body, y_dict):
    try:
        message = json.loads(body)
        message_id, message_body = message['id'], message['body']

        if message_id in y_dict:
            y_dict[message_id][0] = message_body
            process_abs_error(channel, message_id, y_dict)
        else:
            y_dict[message_id] = [message_body, None]
    except Exception as e:
        print(f'Ошибка обработки сообщения {body} из очереди y_true: {e}')


def init_channel():
    try_count = 0
    while try_count < INIT_CHANNEL_TRY_COUNT:
        try:
            time.sleep(10)
            try_count += 1

            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            channel = connection.channel()

            channel.queue_declare(queue='y_true')
            channel.queue_declare(queue='y_pred')
            channel.queue_declare(queue='abs_error')

            return channel
        except Exception as e:
            print(f'Не удалось подключиться к серверу с попытки {try_count}: {e}')

    print(f'Не удалось подключиться к серверу за {INIT_CHANNEL_TRY_COUNT} попыток - выход из приложения')
    return None


def main():
    channel = init_channel()
    if channel is None:
        return
    
    y_dict = {}
    channel.basic_consume(
        queue='y_true',
        on_message_callback=lambda ch, method, properties, body: y_true_callback(channel, body, y_dict),
        auto_ack=True
    )

    channel.basic_consume(
        queue='y_pred',
        on_message_callback=lambda ch, method, properties, body: y_pred_callback(channel, body, y_dict),
        auto_ack=True
    )

    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()

if __name__ == "__main__":
    main()