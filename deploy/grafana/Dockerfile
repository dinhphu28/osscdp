# Bake Grafana provisioning + dashboards into the image (no bind mount).
FROM grafana/grafana:11.2.0
COPY provisioning /etc/grafana/provisioning
COPY dashboards /var/lib/grafana/dashboards
