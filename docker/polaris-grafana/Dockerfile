
FROM grafana/grafana:11.2.0
COPY ./grafana/provisioning/dashboards /etc/grafana/provisioning/dashboards
COPY ./grafana/provisioning/datasources /etc/grafana/provisioning/datasources
COPY ./grafana/dashboards /etc/grafana/dashboards
COPY ./grafana/img/grafana_icon.svg /usr/share/grafana/public/img/grafana_icon.svg
COPY ./grafana/img /usr/share/grafana/public/img/lake
COPY ./data/grafana/grafana.ini /etc/grafana/grafana.ini

ENV GF_USERS_ALLOW_SIGN_UP=false
ENV GF_SERVER_SERVE_FROM_SUB_PATH=true
ENV GF_DASHBOARDS_JSON_ENABLED=true
ENV GF_LIVE_ALLOWED_ORIGINS='*'
ENV GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/etc/grafana/dashboards/Homepage.json
# RUN grafana cli plugins install grafana-piechart-panel
