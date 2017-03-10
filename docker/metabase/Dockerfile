FROM metabase/metabase:latest
EXPOSE 3000
RUN mkdir -p /var/opt/metabase
ENV MB_DB_FILE=/var/opt/metabase/dockercrawler.db
COPY dockercrawler.db.mv.db /var/opt/metabase/