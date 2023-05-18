SHELL := /bin/bash
PWD := $(shell pwd)

default: build

all:

docker-compose-up: 
	docker compose -f docker-compose-dev.yaml up -d --build \
	--scale trips_processor=4 --scale weather_processor=3 --scale stations_processor=3 \
	--scale montreal_stations=3 --scale harversine_distance=3 --scale montreal_trips=3 \
	--scale distance_mean=3 --scale duration_mean=3 --scale year_filter=3
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 40
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
