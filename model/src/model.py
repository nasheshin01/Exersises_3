import pika
import pickle
import numpy as np
import json


if __name__ == "__main__":
    def callback(ch, method, properties, body):
        print(f'Получен вектор признаков {body}')
        data_dict = json.loads(body)

        data = np.array(data_dict['body'])
        prediction = regressor.predict(data.reshape(1, -1))[0]

        prediction_json = json.dumps({
            'id': data_dict['id'],
            'body': prediction
        })
        channel.basic_publish(exchange='', routing_key='y_pred', body=prediction_json)
        print(f'Предсказание {prediction_json} отправлено в очередь y_pred')


    with open('src/myfile.pkl', 'rb') as pkl_file:
        regressor = pickle.load(pkl_file)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='features')
    channel.queue_declare(queue='y_pred')

    channel.basic_consume(
        queue='features',
        on_message_callback=callback,
        auto_ack=True
    )
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')

    channel.start_consuming()
