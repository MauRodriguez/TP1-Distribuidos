import signal
from functools import partial 
import logging
import os
from configparser import ConfigParser
from harversine_distance import HaversineDistance

def initialize_config():

    config = ConfigParser(os.environ)
    config.read("weather_processor/config.ini")

    config_params = {}
    try:        
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["montreal_station_amount"] = os.getenv('MONTREAL_STATION_AMOUNT', config["DEFAULT"]["MONTREAL_STATION_AMOUNT"])
        config_params["montreal_trips_amount"] = os.getenv('MONTREAL_TRIPS_AMOUNT', config["DEFAULT"]["MONTREAL_TRIPS_AMOUNT"])
        config_params["distance_mean_amount"] = os.getenv("DISTANCE_MEAN_AMOUNT", config["DEFAULT"]["DISTANCE_MEAN_AMOUNT"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def initialize_log(logging_level):
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )    

def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    harversine_distance = HaversineDistance(int(config_params["montreal_station_amount"]),
                                            int(config_params["montreal_trips_amount"]),
                                            int(config_params["distance_mean_amount"]))
    signal.signal(signal.SIGTERM, partial(handle_sigterm, harversine_distance))
    harversine_distance.run()

def handle_sigterm(harversine_distance, signum, frame):
    harversine_distance.stop()
    logging.info(f"Sigterm received with signum {signum} frame {frame}")

main()