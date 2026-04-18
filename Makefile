.PHONY: help setup-venv setup-conda install generate run dashboard test clean all

help:
	@echo "Credit Pipeline Demo — available commands:"
	@echo ""
	@echo "Setup (pick one):"
	@echo "  make setup-venv   Create a Python venv and install deps"
	@echo "  make setup-conda  Create a conda env from environment.yml"
	@echo "  make install      Install deps into the current Python env"
	@echo ""
	@echo "Run:"
	@echo "  make generate     Generate fake source data (500 customers)"
	@echo "  make run          Run the full pipeline DAG"
	@echo "  make dashboard    Launch Streamlit dashboard"
	@echo "  make test         Run test suite"
	@echo ""
	@echo "Utility:"
	@echo "  make clean        Remove generated data and caches"
	@echo "  make all          install → generate → run → test"

setup-venv:
	@bash setup.sh

setup-conda:
	@conda env create -f environment.yml || conda env update -f environment.yml
	@echo ""
	@echo "✅ Activate with:  conda activate credit-pipeline-demo"

install:
	pip install -r requirements.txt

generate:
	python -m src.data_generation.generate --customers 500 --output data/landing

run:
	python run.py

dashboard:
	streamlit run dashboard/app.py

test:
	pytest -v

clean:
	chmod -R u+w data 2>/dev/null || true
	rm -rf data
	rm -f serving.db audit.db orchestration.db
	rm -rf .pytest_cache
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true

all: install generate run test
