.PHONY: help venv data train benchmark stream-local replay docker-up docker-down

PYTHON = .venv/bin/python
PIP = .venv/bin/pip

help:
	@echo "Big Data Predictive Maintenance Commands:"
	@echo "  make data          - Download / bootstrap NASA C-MAPSS dataset"
	@echo "  make benchmark     - Run full training, evaluation & model export"
	@echo "  make replay        - Run streaming replay simulator"
	@echo "  make stream-local  - Run Spark streaming local verification"
	@echo "  make docker-up     - Launch Kafka, InfluxDB, Grafana & Spark cluster"
	@echo "  make docker-down   - Stop and tear down docker containers"

data:
	$(PYTHON) -c "from src.preprocessing.loader import load_data; load_data(mode='train'); load_data(mode='test')"

benchmark:
	$(PYTHON) -m src.benchmark.benchmark_runner

replay:
	$(PYTHON) -m src.producer.replay_simulator --rate 20

stream-local:
	$(PYTHON) -m src.streaming.stream_pipeline --local-test

docker-up:
	cd docker && docker-compose up -d

docker-down:
	cd docker && docker-compose down
