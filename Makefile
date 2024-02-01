.PHONY: build-and-up
build-and-up: build-image compose-up

.PHONY: build-image
build-image:
	docker build . --tag my_custom_airflow_with_requirements

.PHONY: compose-up
compose-up:
	docker-compose up
