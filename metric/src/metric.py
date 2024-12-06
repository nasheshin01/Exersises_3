import pika
import json


if __name__ == "__main__":
    y_dict = {}

    def callback(ch, method, properties, body):
        message = json.loads(body)
        message_id, message_body = message['id'][0], message['body']

        if message_id in y_dict:
            if method.routing_key == 'y_true':
                y_dict[message_id][0] = message_body
            elif method.routing_key == 'y_pred':
                y_dict[message_id][1] = message_body
            else:
                raise Exception('Неизвестный поток у сообщения')
            
            with open('./logs/metric_log.csv', 'a') as file:
                abs_error = abs(y_dict[message_id][0] - y_dict[message_id][1])
                file.write(f'{message_id},{y_dict[message_id][0]},{y_dict[message_id][1]},{y_dict[message_id][1]},{abs_error}\n')

            channel.queue_declare(queue='abs_error')

            abs_error_json = json.dumps({
                'id': message_id,
                'body': abs_error
            })
            channel.basic_publish(exchange='', routing_key='abs_error', body=abs_error_json)

        else:
            y_dict[message_id] = [0, 0]
            if method.routing_key == 'y_true':
                y_dict[message_id][0] = message_body
            elif method.routing_key == 'y_pred':
                y_dict[message_id][1] = message_body
            else:
                raise Exception('Неизвестный поток у сообщения')
            
        print(f"Из очереди {method.routing_key} получено значение {body}")


    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='y_true')
    channel.queue_declare(queue='y_pred')

    channel.basic_consume(
        queue='y_true',
        on_message_callback=callback,
        auto_ack=True
    )

    channel.basic_consume(
        queue='y_pred',
        on_message_callback=callback,
        auto_ack=True
    )

    print('...Ожидание сообщений, для выхода нажмите CTRL+C')

    channel.start_consuming()