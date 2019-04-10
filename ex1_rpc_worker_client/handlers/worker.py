import pika
from typing import List
from .base import WorkerBaseHandler


class WorkerHandler(WorkerBaseHandler):
    def __init__(self, **kwargs):
        super(WorkerHandler, self).__init__(**kwargs)

    def on_message(self, channel, method, props, body):
        parsed_body = body.decode('utf-8')
        print('queue {} => {}'.format(parsed_body, parsed_body.upper()))
        response = parsed_body.upper()

        channel.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(
                correlation_id=props.correlation_id,
                expiration=self._ttl_response_ms
            ),
            body=response.encode('utf-8')
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def on_topic(self, channel, method, props, body):
        parsed_body = body.decode('utf-8')
        print('topic: {}'.format(parsed_body))

        channel.basic_ack(delivery_tag=method.delivery_tag)
