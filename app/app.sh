#!/bin/bash
set -euxo pipefail
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
rm -rf .venv
python3 -m venv .venv
echo "Activating virtual environment"
source .venv/bin/activate

# Ensure the fresh venv can install binary wheels.
python -m pip install --upgrade pip setuptools wheel -q

# Install any packages
echo "Installing requirements"
pip install -r requirements.txt -q

# Collect data
echo "Running prepare_data.sh"
bash prepare_data.sh

# Run the indexer
echo "Running index.sh"
bash index.sh

# Run the ranker
echo "Running search.sh"
bash search.sh "data"
