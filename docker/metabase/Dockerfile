# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
FROM metabase/metabase:latest
EXPOSE 3000
RUN mkdir -p /var/opt/metabase
COPY dockercrawler.db.mv.db /var/opt/metabase/
VOLUME /var/opt/metabase
ENV MB_DB_FILE=/var/opt/metabase/dockercrawler.db