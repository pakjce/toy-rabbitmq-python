from typing import List
import pika
import time

import logging

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class WorkerBaseHandler:
    def __init__(
        self,
        host: str,
        queue_task: str,
        topic_ctrl: str,
        topic_binding_keys: List[str],
        port: int = 5672,
        virtualhost: str = '/',
        username: str = None,
        password: str = None,
        ttl_response: float = 60,
        prefetch_count: int = 1
    ):
        self.queue_task = queue_task
        self.topic_ctrl = topic_ctrl
        self.topic_binding_keys = topic_binding_keys

        if username is not None:
            credentials = pika.credentials.PlainCredentials(
                username=username,
                password=password
            )
        else:
            credentials = None
        self._conn_param = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtualhost,
            credentials=credentials
        )

        self.should_reconnect = False
        self.was_consuming = False
        self._ttl_response_ms = str(int(ttl_response * 1000))

        self._connection = None
        self._channel_ctrl = None
        self._channel_task = None
        self._closing = False
        self._consumer_tag_ctrl = None
        self._consumer_tag_task = None

        self._init_ok_ctrl = False
        self._init_ok_task = False

        self._consuming = False

        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = prefetch_count

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        raise NotImplementedError

    def on_topic(self, _unused_channel, basic_deliver, properties, body):
        raise NotImplementedError

    def connect(self):
        self._init_ok_ctrl = False
        self._init_ok_task = False

        return pika.SelectConnection(
            parameters=self._conn_param,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed
        )

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            print('Connection is closing or already closed')
        else:
            print('Closing connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        # LOGGER.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.
        """
        self._channel_ctrl = None
        self._channel_task = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            # LOGGER.warning('Connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def reconnect(self):
        """Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.
        """
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        # LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_task_open)
        self._connection.channel(on_open_callback=self.on_channel_ctrl_open)

    def on_channel_task_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        Since the channel is now open, we'll declare the exchange to use.
        :param pika.channel.Channel channel: The channel object
        """
        # LOGGER.info('Channel opened')
        self._channel_task = channel
        self._channel_task.add_on_close_callback(self.on_channel_closed)
        channel.queue_declare(
            queue=self.queue_task,
            durable=False,
            exclusive=False
        )
        channel.basic_qos(prefetch_count=self._prefetch_count)
        self._init_ok_task = True

    def on_channel_ctrl_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        Since the channel is now open, we'll declare the exchange to use.
        :param pika.channel.Channel channel: The channel object
        """
        # LOGGER.info('Channel opened')
        self._channel_ctrl = channel
        self._channel_ctrl.add_on_close_callback(self.on_channel_closed)
        self._channel_ctrl.basic_qos(prefetch_count=1)
        self.setup_exchange()

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.
        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed
        """
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self.close_connection()

    def setup_exchange(self):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.
        :param str|unicode exchange_name: The name of the exchange to declare
        """
        LOGGER.info('Declaring exchange: %s', self.topic_ctrl)
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called

        self._channel_ctrl.exchange_declare(
            exchange=self.topic_ctrl,
            exchange_type='topic',
            callback=self.on_exchange_declareok)

    def on_exchange_declareok(self, _unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.
        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)
        """
        self._channel_ctrl.queue_declare(
            '',
            exclusive=True,
            auto_delete=True,
            callback=self.on_queue_declareok
        )

    def on_queue_declareok(self, method_frame):
        queue_name = method_frame.method.queue
        self._topic_queue_name = queue_name
        for binding_key in self.topic_binding_keys:
            self._channel_ctrl.queue_bind(
                exchange=self.topic_ctrl,
                queue=queue_name,
                routing_key=binding_key
            )
        self._init_ok_ctrl = True
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.
        """
        # LOGGER.info('Issuing consumer related RPC commands')
        if self._init_ok_ctrl and self._init_ok_task:
            self._channel_ctrl.add_on_cancel_callback(self.on_consumer_ctrl_cancelled)
            self._channel_task.add_on_cancel_callback(self.on_consumer_task_cancelled)
            self._consumer_tag_task = self._channel_task.basic_consume(
                self.queue_task,
                auto_ack=False,
                on_message_callback=self.on_message
            )
            self._consumer_tag_ctrl = self._channel_ctrl.basic_consume(
                self._topic_queue_name,
                auto_ack=False,
                on_message_callback=self.on_topic
            )
            self.was_consuming = True
            self._consuming = True

    def on_consumer_task_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.
        :param pika.frame.Method method_frame: The Basic.Cancel frame
        """
        if self._channel_task:
            self._channel_task.close()

    def on_consumer_ctrl_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.
        :param pika.frame.Method method_frame: The Basic.Cancel frame
        """
        if self._channel_ctrl:
            self._channel_ctrl.close()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self._channel_ctrl:
            self._channel_ctrl.basic_cancel(self._consumer_tag_ctrl)
            self._channel_ctrl.close()
        if self._channel_task:
            self._channel_task.basic_cancel(self._consumer_tag_task)
            self._channel_task.close()

        self._consuming = False

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.
        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        if not self._closing:
            self._closing = True
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()


class ReconnectingConsumer(object):
    """This is a consumer that will reconnect if the nested
    ExampleConsumer indicates that a reconnect is necessary.
    """

    def __init__(self, consumerT: type, **kwargs):
        self._reconnect_delay = 0
        self._consumerT = consumerT
        self._consumer = self._consumerT(**kwargs)
        self._connect_args = kwargs

    def run(self):
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info('Reconnecting after %d seconds', reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer = self._consumerT(**self._connect_args)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
