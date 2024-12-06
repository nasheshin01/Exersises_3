import pika
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
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
            channel.queue_declare(queue='abs_error')

            return channel
        except Exception as e:
            print(f'Не удалось подключиться к серверу с попытки {try_count}: {e}')

    print(f'Не удалось подключиться к серверу за {INIT_CHANNEL_TRY_COUNT} попыток - выход из приложения')
    return None


def callback(ch, method, properties, body):
    plt.clf()
    print(f'Обновляем график')

    df = pd.read_csv('./logs/metric_log.csv')
    sns.histplot(data=df['absolute_error'], kde=True, color="skyblue")

    plt.title('Распределение абсолютной ошибки')
    plt.xlabel("Значение ошибки")
    plt.ylabel("Кол-во значений")
    plt.savefig("./logs/error_distribution.png")
    plt.show()


def main():
    channel = init_channel()
    if channel is None:
        return
    
    channel.basic_consume(
        queue='abs_error',
        on_message_callback=callback,
        auto_ack=True
    )

    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    main()

    