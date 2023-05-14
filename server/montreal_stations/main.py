import signal
from functools import partial 
import logging
import os
from configparser import ConfigParser
from montreal_stations import MontrealStations

def initialize_config():

    config = ConfigParser(os.environ)
    config.read("weather_processor/config.ini")

    config_params = {}
    try:        
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
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

    montreal_stations = MontrealStations()
    signal.signal(signal.SIGTERM, partial(handle_sigterm, montreal_stations))
    montreal_stations.run()

def handle_sigterm(montreal_stations, signum, frame):
    montreal_stations.stop()
    logging.info(f"Sigterm received with signum {signum} frame {frame}")

main()