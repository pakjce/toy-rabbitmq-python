from ex1_rpc_worker_client.handlers.worker import WorkerHandler
from ex1_rpc_worker_client.handlers.base import ReconnectingConsumer
from ex1_rpc_worker_client import config


if __name__ == '__main__':
    consumer = ReconnectingConsumer(
        WorkerHandler,
        host=config.AMQP_HOST,
        port=config.AMQP_PORT,
        username=config.AMQP_USER,
        password=config.AMQP_PASSWORD,
        queue_task=config.AMQP_TASK_QUEUE,
        topic_ctrl=config.AMQP_MANAGEMENT_TOPIC,
        topic_binding_keys=[
            'all',
            'fe'
        ],
        ttl_response=config.AMQP_TTL_RESPONSE,
        prefetch_count=2
    )

    consumer.run()

    # consumer = WorkerHandler(
    #     host=config.AMQP_HOST,
    #     port=config.AMQP_PORT,
    #     username=config.AMQP_USER,
    #     password=config.AMQP_PASSWORD,
    #     queue_task=config.AMQP_TASK_QUEUE,
    #     topic_ctrl=config.AMQP_MANAGEMENT_TOPIC,
    #     topic_binding_keys=[
    #         'all',
    #         'fe'
    #     ],
    #     prefetch_count=2
    # )
    #
    # while True:
    #     try:
    #         consumer.run()
    #     except KeyboardInterrupt:
    #         consumer.stop()
    #         break

    print('exit!')
