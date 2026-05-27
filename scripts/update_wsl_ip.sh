#!/bin/bash
# Runs at WSL startup — updates the DAG with the current WSL IP
# and ensures SSH server is running.

# Start SSH server
sudo service ssh start

# Get current WSL IP
WSL_IP=$(hostname -I | awk '{print $1}')
echo "WSL IP: $WSL_IP"

# Update the DAG file
DAG_FILE="/home/sara/price-intelligence/airflow/dags/dags/scrape_and_export.py"
sed -i "s/WSL_HOST = '[0-9.]*'/WSL_HOST = '$WSL_IP'/" "$DAG_FILE"
echo "DAG updated with WSL_HOST = $WSL_IP"
