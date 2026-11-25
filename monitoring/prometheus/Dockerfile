FROM prom/prometheus:v2.51.0

COPY prometheus.yml /etc/prometheus/prometheus.yml

EXPOSE 10000

ENTRYPOINT ["/bin/prometheus"]
CMD [ \
    "--config.file=/etc/prometheus/prometheus.yml", \
    "--web.listen-address=:10000", \
    "--storage.tsdb.retention.time=3d" \
]
