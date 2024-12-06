import pika
import pandas as pd

if __name__ == "__main__":
    def callback(ch, method, properties, body):
        print(f'Обновляем график')

        df = pd.read_csv('./logs/metric_log.csv')
        ax = df['absolute_error'].hist()
        fig = ax.get_figure()
        fig.savefig('./logs/error_distribution.png')
        


    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='abs_error')

    channel.basic_consume(
        queue='abs_error',
        on_message_callback=callback,
        auto_ack=True
    )
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')

    channel.start_consuming()