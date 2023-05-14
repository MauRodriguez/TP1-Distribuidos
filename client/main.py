from client import Client
import logging
from configparser import ConfigParser
import os
import signal
from functools import partial

def initialize_config():

    config = ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["server_address"] = os.getenv("SERVER_ADDRESS", config["DEFAULT"]["SERVER_ADDRESS"])
        config_params["logging_level"] = os.getenv("LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

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
    server_address = config_params["server_address"] 
    initialize_log(logging_level)
    client = Client(server_address)
    signal.signal(signal.SIGTERM, partial(handle_sigterm, client))
    client.run()

def handle_sigterm(client, signum, frame):
    client.stop()
    logging.info(f"Sigterm received with signum {signum} frame {frame}") 

main()