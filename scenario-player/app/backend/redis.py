from datetime import datetime
from typing import List, Tuple

import aioredis

from app import logger, get_bool_env
from app.backend.base import BaseTimelineBackend
from app.scenario.scenario_model import Scenario, Step, Location

TIMELINE_KEY = f'TIMELINE:CURRENT'
SCENARIO_KEY = 'SCENARIO'
# TODO consider datetime.datetime.fromisoformat (Python 3.7+) and datetime.datetime.isoformat
TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S.%f%z'

# while persisting the scenario, overwrite the existing one
# (otherwise new scenario steps will be appended to the existing one)
SCENARIO_OVERWRITE = get_bool_env('SCENARIO_OVERWRITE', True)


class RedisTimelineBackend(BaseTimelineBackend):

    def __init__(self, connection_url: str = 'redis://localhost', database: int = 0, redis_password: str = None):
        """
        Initialize internal fields.
        """
        super().__init__()
        self.connection_url = connection_url
        self.database = database
        self.redis_password = redis_password
        self.redis = None

    async def initialize(self):
        logger.info("Connecting to Redis...")
        logger.info(f'{self.connection_url}, {self.database},{self.redis_password}')
        try:
            self.redis = aioredis.from_url(self.connection_url, db=self.database, password=self.redis_password,
                                           encoding='utf-8', decode_responses=True)
        except Exception as e:
            logger.error(f"Error while connecting to redis: {e}")
            logger.exception(e)
            raise e

    ###
    # scenario related
    #

    @staticmethod
    def marshall_step(p: Step):
        tmpstmp = p.timestamp.strftime(TIMESTAMP_FORMAT)
        return f'{tmpstmp}|{p.type}|{p.location.x}|{p.location.y}'

    def serialize_steps(self, steps: List[Step]):
        return [self.marshall_step(x) for x in steps]

    async def store_scenario(self, scenario: Scenario, namespace: str = SCENARIO_KEY):
        logger.info('store_scenario')
        result = None
        try:
            scenario_key = f'{namespace}:{scenario.customer.customer_id}'

            if SCENARIO_OVERWRITE:
                logger.info(f'Overwriting scenario {scenario_key}')
                await self.redis.delete(scenario_key)

            values = self.serialize_steps(scenario.path)
            length = len(values)

            if length >= 1:
                logger.debug(f'Sending multiple values ... {values}')
                result = await self.redis.rpush(scenario_key, *values)

                result = scenario_key
        except Exception as e:
            logger.error(f"Error while talking to redis: {e}")
            logger.exception(e)

        return result

    ###
    # timeline related
    #

    @staticmethod
    def marshall_event(client_id: str, p: Step):
        tmpstmp = p.timestamp.strftime(TIMESTAMP_FORMAT)
        return f'{client_id}|{p.location.x}|{p.location.y}|{p.type}|{tmpstmp}'

    @staticmethod
    def unmarshall_event(s: str):
        logger.debug(f'unmarshal event {s}')
        parts = s.split(sep='|')

        client_id = parts[0]
        loc = Location(x=int(parts[1]), y=int(parts[2]))
        timestamp = datetime.strptime(parts[4], TIMESTAMP_FORMAT)

        return client_id, Step(location=loc, type=parts[3], timestamp=timestamp)

    async def add_to_timeline(self, customer_id: str, step: Step):
        """
        Store info about given step for a given customer.
        :param customer_id:
        :param step:
        :return:
        """
        logger.info(f'add_to_timeline: {customer_id} {step} ')

        result = False

        try:
            event_representation = self.marshall_event(customer_id, step)
            logger.debug(f'marshalled event_representation: {event_representation}')
            result = await self.redis.zadd(TIMELINE_KEY, {event_representation: int(step.timestamp.timestamp())})
        except Exception as e:
            logger.error(f"Error while talking to redis: {e}")
            logger.exception(e)

        return result

    async def get_events(self, unix_time: int, include_earlier: bool = True) -> List[Tuple[str, Step]]:
        logger.debug(f'get events from redis for timestamp {unix_time}')
        """
        Get events definitions for a given point in time
        """
        result = []
        min = "-inf" if include_earlier else unix_time
        try:
            async with self.redis.pipeline(transaction=True) as pipe:
                events, ok = await (
                    pipe.zrangebyscore(TIMELINE_KEY, min=min, max=unix_time)
                        .zremrangebyscore(TIMELINE_KEY, min=min, max=unix_time)
                ).execute()

            if ok:
                logger.debug(f'removed {ok} events from timeline')
                logger.debug(f'events: {events}')
            result = [self.unmarshall_event(e) for e in events]

        except Exception as e:
            logger.error(f"Error while talking to redis: {e}")
            logger.exception(e)
            logger.error(type(e))

        return result
