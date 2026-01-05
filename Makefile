.PHONY: help venv install generate-templates consumer

PY?=python3
VENV?=.venv
PYTHON:=$(shell if [ -x "$(VENV)/bin/python" ]; then echo "$(VENV)/bin/python"; else echo "$(PY)"; fi)

help:
	@echo "Targets:"
	@echo "  venv              Create local virtualenv (.venv)"
	@echo "  install           Install python dependencies"
	@echo "  generate-templates Generate data/Nifi_Templates_1500.csv"
	@echo "  consumer          Run Kafka->ClickHouse consumer"

venv:
	$(PY) -m venv $(VENV)
	@echo "Activate with: source $(VENV)/bin/activate"

install:
	$(VENV)/bin/python -m pip install --upgrade pip
	$(VENV)/bin/pip install -r requirements.txt

generate-templates:
	$(PYTHON) scripts/generate_nifi_template_csv.py --avg-year --output data/Nifi_Templates_1500.csv

consumer:
	$(PYTHON) scripts/kafka_to_clickhouse.py
