import os

from app import logger, validate_and_crash, get_bool_env, dump_constants

logger.info('Reading environment variables...')

STORE_HEIGHT = int(os.getenv('STORE_HEIGHT', 10))
STORE_WIDTH = int(os.getenv('STORE_WIDTH', 6))

CUSTOMERS_AVERAGE_IN_STORE = int(os.getenv('CUSTOMERS_AVERAGE_IN_STORE', 6))
CUSTOMERS_LIST_FILE = os.getenv('CUSTOMERS_LIST_FILE', 'customers.csv')

MQTT_HOST = os.getenv('MQTT_HOST')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))
MQTT_NAME = os.getenv('MQTT_NAME', 'demoClient')
MQTT_USERNAME = os.getenv('MQTT_USERNAME', None)
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', None)
MQTT_BROKER_CERT_FILE = os.getenv('MQTT_BROKER_CERT_FILE', None)

CUSTOMER_ENTER_TOPIC = os.getenv('ENTER_TOPIC', 'customer/enter')
CUSTOMER_EXIT_TOPIC = os.getenv('EXIT_TOPIC', 'customer/exit')
CUSTOMER_MOVE_TOPIC = os.getenv('MOVE_TOPIC', 'customer/move')

TESTING_MOCK_MQTT = get_bool_env('TESTING_MOCK_MQTT', False)
# For bigger scale and volume, use Redis backend
USE_REDIS_BACKEND = get_bool_env('USE_REDIS_BACKEND', False)
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)


HIDDEN_CONSTANTS_KEYS = ['MQTT_PASSWORD', 'REDIS_PASSWORD']
dump_constants(logger.info, HIDDEN_CONSTANTS_KEYS)

REQUIRED_PARAM_MESSAGE = 'Cannot read {} env variable. Please, make sure it is set before starting the service.'
validate_and_crash(logger.error, MQTT_HOST, REQUIRED_PARAM_MESSAGE.format('MQTT_HOST'))
if USE_REDIS_BACKEND:
    validate_and_crash(logger.error, REDIS_HOST, REQUIRED_PARAM_MESSAGE.format('REDIS_HOST'))
