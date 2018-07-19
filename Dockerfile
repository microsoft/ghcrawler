# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

FROM node:8

LABEL maintainer="opensource@microsoft.com"\
  vendor="Microsoft"\
  com.microsoft.product="GHCrawler"\
  com.microsoft.url="https://hub.docker.com/r/microsoft/ghcrawler"\
  com.microsoft.vcs-url="https://github.com/Microsoft/ghcrawler"

EXPOSE 3000
EXPOSE 5858

RUN mkdir -p /opt/ghcrawler

# use changes to package.json to force Docker not to use the cache
# when we change our application's nodejs dependencies:
ENV NPM_CONFIG_LOGLEVEL=warn
ADD package.json /tmp/package.json
RUN cd /tmp && npm install --production
RUN cp -a /tmp/node_modules /opt/ghcrawler/

WORKDIR /opt/ghcrawler
ENV PATH="/opt/ghcrawler/bin:$PATH"
ADD . /opt/ghcrawler

CMD ["npm", "start"]