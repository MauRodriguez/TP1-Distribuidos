from server import Server
from configparser import ConfigParser
import os
import logging
import signal
from functools import partial

def initialize_config():

    config = ConfigParser(os.environ)
    config.read("endpoint/config.ini")

    config_params = {}
    try:
        config_params["port"] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["trip_processor_amount"] = os.getenv('TRIP_PROCESSOR_AMOUNT', config["DEFAULT"]["TRIP_PROCESSOR_AMOUNT"])
        config_params["weather_processor_amount"] = os.getenv('WEATHER_PROCESSOR_AMOUNT', config["DEFAULT"]["WEATHER_PROCESSOR_AMOUNT"])
        config_params["station_processor_amount"] = os.getenv('STATION_PROCESSOR_AMOUNT', config["DEFAULT"]["STATION_PROCESSOR_AMOUNT"])
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
    logging_level = config_params["logging_level"]
    port = config_params["port"]    
    initialize_log(logging_level)
    server = Server(port, int(config_params["trip_processor_amount"]),
                    int(config_params["weather_processor_amount"]),
                    int(config_params["station_processor_amount"]))
    signal.signal(signal.SIGTERM, partial(handle_sigterm, server))
    server.run()

def handle_sigterm(server, signum, frame):
    server.stop()
    logging.info(f"Sigterm received with signum {signum} frame {frame}")

main()