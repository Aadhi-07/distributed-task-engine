FROM grafana/grafana:10.4.0

# Copy datasource and dashboard provisioning configurations
COPY provisioning/ /etc/grafana/provisioning/
COPY dashboards/ /var/lib/grafana/dashboards/

EXPOSE 10000
