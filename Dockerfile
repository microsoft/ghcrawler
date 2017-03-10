FROM node:6.9.5

EXPOSE 3000
EXPOSE 5858

RUN mkdir -p /opt/ospo-ghcrawler

# use changes to package.json to force Docker not to use the cache
# when we change our application's nodejs dependencies:
RUN npm install -g nodemon
ADD package.json /tmp/package.json
RUN cd /tmp && npm install --production
RUN cp -a /tmp/node_modules /opt/ospo-ghcrawler/

WORKDIR /opt/ospo-ghcrawler
ENV PATH="/opt/ospo-ghcrawler/bin:$PATH"
ADD . /opt/ospo-ghcrawler

CMD ["npm", "start"]