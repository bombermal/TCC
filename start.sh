#!/bin/bash

# Get the hostname of the machine
hostname=$(hostname)
echo "Hostname: "$hostname
# Check if the hostname is "residenciabi-04"
if [ "$hostname" == "residenciabi-04" ]; then
    # Write "ENV=tre" to the file "Monitoring_VM\Prometheus-Grafana\.env"
    echo "ENV=tre" > "Monitoring_VM/Prometheus-Grafana/.env"
else
    # Write "ENV=home" to the file "Monitoring_VM\Prometheus-Grafana\.env"
    echo "ENV=home" > "Monitoring_VM/Prometheus-Grafana/.env"
fi

# Run the command "docker-compose up -d" for the file "docker-compose.yml" inside the path "Monitoring_VM\Prometheus-Grafana\"
docker-compose -f "Monitoring_VM/Prometheus-Grafana/docker-compose.yml" up -d
