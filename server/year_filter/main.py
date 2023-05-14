import signal
from functools import partial 
import logging
import os
from configparser import ConfigParser
from year_filter import YearFilter

def initialize_config():

    config = ConfigParser(os.environ)
    config.read("weather_processor/config.ini")

    config_params = {}
    try:        
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["trip_processor_amount"] = os.getenv('TRIP_PROCESSOR_AMOUNT', config["DEFAULT"]["TRIP_PROCESSOR_AMOUNT"])
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

    year_filter = YearFilter(int(config_params["trip_processor_amount"]))
    signal.signal(signal.SIGTERM, partial(handle_sigterm, year_filter))
    year_filter.run()

def handle_sigterm(year_filter, signum, frame):
    year_filter.stop()
    logging.info(f"Sigterm received with signum {signum} frame {frame}")
    
main()