run-all: download-amazon-meta build run populate-database start-dashboard stop

run-all-no-download: build run populate-database run-queries stop

.PHONY: download-amazon-meta
download-amazon-meta:
	@sh scripts/download-amazon-meta.sh

.PHONY: build
build:
	@docker-compose build

.PHONY: run
run:
	@docker-compose up -d

.PHONY: populate-database
populate-database:
	@docker exec -it tp1_aldemir_nilson_victor_python python3 src/tp1_3.2.py

.PHONY: run-queries
run-queries:
	@docker exec -it tp1_aldemir_nilson_victor_python python3 src/tp1_3.3.py

.PHONY: stop
stop:
	@docker-compose down

.PHONY: access-postgres
access-postgres:
	@docker exec -it tp1_aldemir_nilson_victor_postgres psql -U postgres

.PHONY: access-python
access-python:
	@docker exec -it tp1_aldemir_nilson_victor_python bash
