"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    #
    # TODO: Define this value schema in `schemas/turnstile_value.json, then uncomment the below, done
    #
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        
        super().__init__(
            "turnstiles_topic", # TODO: Come up with a better topic name, done
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=1,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.info(f"{self.topic_name} : turnstile at station {self.station.station_id} turned for {num_entries} times at {timestamp}")


        # TODO: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated, done
        try:
            
            for _ in range(num_entries):
                
                #print(f"Produced entry with {self.station.station_id}, \
                #    {self.station.name} and {self.station.color.name}")
            
                self.producer.produce(
                    topic="turnstiles_topic",
                    key={"timestamp": self.time_millis()},
                    value={
                        "station_id": self.station.station_id,
                        "station_name": self.station.name,
                        "line": self.station.color.name
                    }, #stationid and name from HW class, but line?, done
                    value_schema=Turnstile.value_schema)
        except Exception as e:
            logger.info(f"turnstile kafka integration incomplete - skipping \
                         with Error: {e}")