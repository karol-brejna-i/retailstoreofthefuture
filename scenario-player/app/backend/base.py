from typing import List, Tuple

from app.scenario.scenario_model import Scenario, Step, UtcDatetime


class BaseTimelineBackend:
    def __init__(self):
        """
        Initialize internal fields.
        """
        pass

    async def initialize(self):
        """
        Initialize the backend (for example, connect to the DB, etc.)
        :return:
        """
        pass

    async def store_scenario(self, scenario: Scenario):
        """
        Persist timeline metadata.
        :param scenario:
        :return:
        """
        raise NotImplementedError

    async def add_to_timeline(self, customer_id: str, step: Step):
        """
        Store info about given step for a given customer.
        :param customer_id:
        :param step:
        :return:
        """
        raise NotImplementedError

    async def get_events(self, timestamp: UtcDatetime) -> List[Tuple[str, Step]]:
        """

        :param timestamp: timezone-aware datetime
        :return: list of tuples (customer_id, step)
        """
        raise NotImplementedError
