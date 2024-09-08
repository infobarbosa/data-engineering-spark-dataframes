#! /bin/bash
# Install Jupyter Notebook
pip install jupyter

# Generate Jupyter Notebook configuration file
jupyter notebook --generate-config

# Set Jupyter Notebook password
jupyter notebook password

# Start Jupyter Notebook server
jupyter notebook