"""Contains functionality related to Lines"""
import json
import logging

from models import Line


logger = logging.getLogger(__name__)


class Lines:
    """Contains all train lines"""

    def __init__(self):
        """Creates the Lines object"""
        self.red_line = Line("red")
        self.green_line = Line("green")
        self.blue_line = Line("blue")

    def process_message(self, message):
        """Processes a station message"""
        
        if "station_topic" in message.topic():
            value = message.value()
            if message.topic() == "SQL_stations_transformed":
                value = json.loads(value)
                logger.info(f"{message.topic()} processed in lines for color {value['line']}")
            if value["line"] == "green":
                self.green_line.process_message(message)
                logger.info(f"{message.topic()} processed in lines for color {value['line']}")
            elif value["line"] == "red":
                self.red_line.process_message(message)
                logger.info(f"{message.topic()} processed in lines for color {value['line']}")
            elif value["line"] == "blue":
                self.blue_line.process_message(message)
                logger.info(f"{message.topic()} processed in lines for color {value['line']}")
            else:
                logger.debug("discarding unknown line msg %s", value["line"])
        elif "TURNSTILE_SUMMARY" == message.topic():
            self.green_line.process_message(message)
            logger.info(f"{message.topic()} processed in lines for color green")
            self.red_line.process_message(message)
            logger.info(f"{message.topic()} processed in lines for color red")
            self.blue_line.process_message(message)
            logger.info(f"{message.topic()} processed in lines for color blue")
        else:
            logger.info("ignoring non-lines message %s", message.topic())
