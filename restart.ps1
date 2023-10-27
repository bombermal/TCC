docker-compose -f "Monitoring_VM/Prometheus-Grafana/docker-compose.yml" down

# Get the hostname of the machine
$hostname = hostname
Write-Host "Hostname: $hostname"

# Check if the hostname is "residenciabi-04"
if ($hostname -eq "residenciabi-04") {
    # Write "ENV=tre" to the file "Monitoring_VM\Prometheus-Grafana\.env"
    Set-Content -Path "Monitoring_VM/Prometheus-Grafana/.env" -Value "ENV=tre"
} else {
    # Write "ENV=home" to the file "Monitoring_VM\Prometheus-Grafana\.env"
    Set-Content -Path "Monitoring_VM/Prometheus-Grafana/.env" -Value "ENV=home"
}

# Run the command "docker-compose up -d" for the file "docker-compose.yml" inside the path "Monitoring_VM\Prometheus-Grafana\"
docker-compose -f "Monitoring_VM/Prometheus-Grafana/docker-compose.yml" up -d
