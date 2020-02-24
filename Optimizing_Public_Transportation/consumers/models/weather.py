"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("weather process_message")
        #
        #
        # :: Process incoming weather messages. Set the temperature and status.
        #
        #
        try:
            value = json.loads(message.value())
            print("value_temp", value['temperature'])
            self.temperature = value['temperature']
            self.status = value['status']
        except Exception as e:
            logger.fatal(f"Weather issues: {value} {e}")
