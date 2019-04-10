import pika
import functools
from timeit import default_timer as timer
from ex1_rpc_worker_client import config

# TODO: https://github.com/pika/pika/blob/master/examples/asynchronous_consumer_example.py

def on_request(ch, method, props, body):
    parsed_body = body.decode('utf-8')
    print('{} => {}'.format(parsed_body, parsed_body.upper()))
    response = parsed_body.upper()

    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(
            correlation_id=props.correlation_id
        ),
        body=response.encode('utf-8')
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)


def on_topic(ch, method, props, body):
    parsed_body = body.decode('utf-8')
    print('on_topic: {} => {}'.format(parsed_body, parsed_body.upper()))


def on_open(connection):
    connection.channel(on_open_callback=on_channel1_open)
    connection.channel(on_open_callback=on_channel2_open)
    # connection.channel(on_channel2_open)


def on_channel1_open(channel):
    channel.queue_declare(
        queue=config.AMQP_TASK_QUEUE,
        durable=False,
        exclusive=False
    )
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=config.AMQP_TASK_QUEUE,
        on_message_callback=on_request
    )

    # # channel.basic_consume(callback, queue='queue2')
    # channel.exchange_declare(
    #     exchange=config.AMQP_MANAGEMENT_TOPIC,
    #     exchange_type='fanout'
    # )


def on_channel2_open(channel):
    channel.queue_declare(
        '',
        exclusive=True,
        auto_delete=True,
        callback=on_channel2_queue_declared
    )
    pass
    # queue_name = result.method.queue
    # channel.queue_bind(
    #     exchange=config.AMQP_MANAGEMENT_TOPIC,
    #     queue=queue_name,
    #     callback=on_channel2_queue_bind
    # )
    # channel.basic_consume(
    #     queue=queue_name,
    #     on_message_callback=on_topic,
    #     auto_ack=True
    # )
    #
    # print('Topic: [{}] -> {}'.format(config.AMQP_MANAGEMENT_TOPIC, queue_name))

def on_channel2_queue_declared(method_frame):
    queue_name = method_frame.method.queue
    channel.queue_bind(
        exchange=config.AMQP_MANAGEMENT_TOPIC,
        queue=queue_name,
        callback=on_channel2_queue_bind
    )


def on_channel2_queue_bind(**kargs):
    pass



if __name__ == '__main__':
    connection = pika.SelectConnection(
        pika.ConnectionParameters(
            host=config.AMQP_HOST,
            port=config.AMQP_PORT,
            credentials=pika.credentials.PlainCredentials(
                username=config.AMQP_USER,
                password=config.AMQP_PASSWORD
            )
        ),
        on_open_callback=on_open
    )


    # channel = connection.channel()
    #
    # channel.queue_declare(
    #     queue=config.AMQP_TASK_QUEUE,
    #     durable=False,
    #     exclusive=False
    # )
    # channel.basic_qos(prefetch_count=1)
    # channel.basic_consume(
    #     queue=config.AMQP_TASK_QUEUE,
    #     on_message_callback=on_request
    # )
    #
    # channel2 = connection.channel()
    #
    # channel2.exchange_declare(
    #     exchange=config.AMQP_MANAGEMENT_TOPIC,
    #     exchange_type='fanout'
    # )
    #
    # result = channel2.queue_declare(
    #     '',
    #     exclusive=True
    # )
    # queue_name = result.method.queue
    # channel2.queue_bind(exchange=config.AMQP_MANAGEMENT_TOPIC, queue=queue_name)
    # channel2.basic_consume(
    #     queue=queue_name,
    #     on_message_callback=on_topic,
    #     auto_ack=True
    # )
    #
    # print('Topic: [{}] -> {}'.format(config.AMQP_MANAGEMENT_TOPIC, queue_name))
    #
    # print(" [x] Awaiting RPC requests")
    # # channel.start_consuming()

    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()
