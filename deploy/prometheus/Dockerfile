# Bake the Prometheus config + rules into the image so no bind mount is needed
# (works on rootless Docker where /home bind mounts fail).
FROM prom/prometheus:v2.54.1
COPY prometheus.yml /etc/prometheus/prometheus.yml
COPY alerts.yml /etc/prometheus/alerts.yml
