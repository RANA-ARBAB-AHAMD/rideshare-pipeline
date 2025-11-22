#!/bin/bash
set -e

echo "Initializing Airflow DB..."
airflow db migrate

echo "Creating Admin user..."
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || echo "Admin user already exists"
