# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

FROM node:6

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

CMD ["ghcrawler"]