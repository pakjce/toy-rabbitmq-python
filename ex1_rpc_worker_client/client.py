import pika
import uuid
from ex1_rpc_worker_client import config
from timeit import default_timer as timer


class RpcClient(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=config.AMQP_HOST,
                port=config.AMQP_PORT,
                credentials=pika.credentials.PlainCredentials(
                    username=config.AMQP_USER,
                    password=config.AMQP_PASSWORD
                )
            )
        )

        self.channel = self.connection.channel()

        result = self.channel.queue_declare('', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body.decode('utf-8')

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=config.AMQP_TASK_QUEUE,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
                expiration=str(int(config.AMQP_TTL_REQUEST * 1000))
            ),
            body=str(n))
        # while self.response is None:

        self.connection.process_data_events(time_limit=config.AMQP_TTL_REQUEST)

        if self.response is None:
            raise TimeoutError('Timeout!')
        return self.response

    def close(self):
        self.connection.close()


if __name__ == '__main__':
    start_time = timer()
    rpc = RpcClient()
    time_elapsed = timer() - start_time
    print('[{:.3f} ms] connected'.format(time_elapsed*1000))

    for n in range(0, 10):
        start_time = timer()
        response = rpc.call('hello %d' % n)
        time_elapsed = timer() - start_time
        print('[{:.3f} ms] response: {}'.format(time_elapsed*1000, response))

