import os
from dotenv import load_dotenv
from os.path import join, dirname, abspath
from pathlib import Path

PROJECT_DIR = Path(join(dirname(abspath(__file__)), '..')).resolve()

load_dotenv(dotenv_path=join(PROJECT_DIR, '.env'))

AMQP_HOST = os.getenv('AMQP_HOST', 'localhost')
AMQP_PORT = int(os.getenv('AMQP_PORT', '5672'))
AMQP_USER = os.getenv('AMQP_USER', 'admin')
AMQP_PASSWORD = os.getenv('AMQP_PASSWORD', 'admin')
AMQP_VHOST = os.getenv('AMQP_VHOST', '/')
AMQP_TASK_QUEUE = os.getenv('AMQP_TASK_QUEUE', 'lens-worker-fe')
AMQP_MANAGEMENT_TOPIC = os.getenv('AMQP_MANAGEMENT_TOPIC', 'lens-worker-fe')

AMQP_TTL_RESPONSE = float(os.getenv('AMQP_TTL_RESPONSE', '60'))
AMQP_TTL_REQUEST = float(os.getenv('AMQP_TTL_REQUEST', '60'))
