.PHONY: setup run test clean

# Default shell
SHELL := /bin/bash

# Venv directory
VENV := venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

# Setup the environment
setup:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

# Run the main pipeline
run:
	$(PYTHON) src/main.py

# Run tests
test:
	$(PYTHON) -m pytest tests/

# Clean up
clean:
	rm -rf $(VENV)
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
