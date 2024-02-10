.PHONY: build-and-up
build-and-up: build-image compose-up

.PHONY: build-image
build-image:
	docker build . --build-arg SMTP_USER="$SMTP_USER" --build-arg SMTP_PW="$SMTP_PW" --tag my_custom_airflow_with_requirements
.PHONY: compose-up
compose-up:
	docker-compose up
