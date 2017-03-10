![Version](https://img.shields.io/npm/v/ghcrawler.svg)
![License](https://img.shields.io/github/license/Microsoft/ghcrawler.svg)
![Downloads](https://img.shields.io/npm/dt/ghcrawler.svg)

# GHCrawler
A robust GitHub API crawler that walks a queue of GitHub entities transitively retrieving and storing their contents. GHCrawler is great for:

* Retreiving all GitHub entities related to an org, repo, or user
* Efficiently storing and the retrieved entities
* Keeping the stored data up to date when used in conjunction with a GitHub webhook to track events

GHCrawler focuses on successively retrieving and walking GitHub API resources supplied on a (set of) queues. Each resource is fetched, processed, plumbed
for more resources to fetch and ultimately stored. Discovered resources are themselves queued for further processing. The crawler is careful to not
repeatedly fetch the same resource. It makes heavy use of etags, Redis, client-side rate limiting, and GitHub token pooling and rotation to optimize use of your API tokens and not beat up the GitHub API.

The crawler can be configured to use a variety of different queuing (e.g., AMQP 1.0 and AMQP 0.9 compatible queues like Azure ServiceBus and Rabbit MQ, respectively) and storage technologies (e.g., Azure Blob and MongoDB). You can create your own infrastructure plugins to use different technologies.

# Running in-memory
The easiest way try our the crawler is to run it in memory. You can get up and running in a couple minutes.  This approach does not scale and is not persistent but it's dead simple.

1. Clone the [Microsoft/ghcrawler](https://github.com/Microsoft/ghcrawler.git) repo.
1. Run ```npm install``` in the clone repo directory to install the prerequisites.
1. Run the crawler using ```node bin/www.js```.

Once the service is up and running, you should see some crawler related messages in the console output every few seconds. You can control the crawler either using the ```cc``` command line tool or a brower-based dashboard both of which are described below. Note that since you are running in memory, if you kill the crawler process, all work will be lost. This mode is great for playing around with the crawler or testing.

# Running Crawler-In-A-Box (CIABatta)
If you want to persist the data gathered and create some insight dashboards in small to medium production system, you can run the crawler in Docker with Mongo, Rabbit, and Redis using the Crawler-in-a-box (CIABatta) approach. This setup also includes Metabase for building browser-based insgihts and gives you a browser-based control-panel for observing and controlling the crawler service.

***NOTE*** This is an evolving solution and the steps for running will be simplified published, ready-to-use images on Docker Hub. For now, follow these steps

1. Clone the [Microsoft/ghcrawler](https://github.com/Microsoft/ghcrawler.git) and [Microsoft/crawler-dashboard](https://github.com/Microsoft/crawler-dashboard.git) repos.
1. In a command prompt go to ```ghcrawler/docker``` and run ```docker-compose up```.

Once the containers are up and running, you should see some crawler related messages in the container's console output every few seconds. You can control the crawler either using the ```cc``` command line tool or a brower-based dashboard both of which are described below.

You can also hookup directly to the crawler infrastructure. By default the containers expose a number of endpoints at different ports on localhost. Note that if you have trouble starting the containers due to port conflicts, either shutdown your services using these ports or edit the docker/docker-compose.yml file to change the ports.

* Crawler Dashboard (4000) -- Open http://localhost:4000 in your browser to see what's happening and control some behaivors and configurations
* Crawler (3000) -- Direct access to the REST API for the crawler
* MongoDB (27017 and 28017) -- Direct access to the Mongo DB
* Redis (6379) -- Observe what's happening in Redis. Not much else for you to do here
* RabbitMQ (5672 and 15672) -- Hit http://localhost:15672 with a browser to see and maange the RabbitMQ queues
* Metabase (5000) -- Hit http://localhost:5000 to get live insights in your browser via Metabase

# Deploying native
For ultimate flexibility, the crawler and associated bits can be run directly on VMs or as an app service. This structure typically uses cloud-based infrastructure for queuing, storage and redis. For example, this project comes with adapters for Azure Service Bus queuing and Azure Blob storage. The APIs on these adpaters is very slim so it is easy to for you to implement (and contribute) more.

***Setting up this operating mode is a bit more involved and is not yet documented.***

# Controlling the crawler
Given a running crawler service (see above), you can control it using either a simple command line app or a browser-based dashboard.

## ```cc``` command line

The *crawler-cli* (aka ```cc```) can run interactively or as a single command processor and enables a number of basic operations.  For now the crawler-cli is not published as an npm. Instead, [clone its repo]((https://github.com/Microsoft/crawler-cli.git), run ```npm install``` and run the command line using

```
node bin/cc -i
```

The app's built-in help has general usage info and more details can be found in [the project's readme](https://github.com/Microsoft/crawler-cli/blob/develop/README.md). A typical command sequence shown in the snippet below starts ```cc``` in interactive mode, configures the crawler with a set of GitHub tokens, sets the org filtering and then queues and starts the processing of the org.

```
> node bin/cc -i
http://localhost:3000> tokens 43984b2344ca575d0f0e097efd97#public 972bbdfe098098fa9ce082309#admin
http://localhost:3000> orgs contoso-d
http://localhost:3000> queue contoso-d
http://localhost:3000> start 5
http://localhost:3000> exit
>
```

## Browser dashboard

The crawler dashboard gives you live feedback on what the crawler is doing as well as better control over the crawler's queues and configuration. Some configurations (e.g., Docker) include and start the dashboard for free. If you need to deploy the dashboard explicitly, clone the [Microsoft/crawler-dashboard](https://github.com/Microsoft/crawler-dashboard.git) repo and follow the instructions in [the README found there](https://github.com/Microsoft/crawler-dashboard/blob/develop/README.md).

Once the dashboard service is up and running, point your browser at the dashboard endpoing (http://localhost:4000 by default).

Note that the dashboard does not report queue message rates (top right graph) when used with the memory-based crawler service as that mechanism requires Redis to talk record activity.

# Known issues

It is clearly early days for the crawler so there are a number of things left to do. These will be collected in repo issues. Note that the remaining issue set has yet to be populated.

Broadly speaking there are several types of work:

* Documentation -- The crawler code itself is relatively straightforward but some of the architecture, control and extensibility points are not called out.
* Ease of use -- There are a number of places where running and manaing the crawler is just clumsy and error prone
* Completeness -- There are a few functional gaps in certain scenarios that need to be addressed.
* Docker configuration -- Several items in making the Docker configuration real
* Analysis and insights -- Metabase is supplied in the Docker configuration but relatively little has been done with analyzing the harvested data.


## Runtime

### Docker items
1. Data persistence
1. Create separate docker-compose for general usage vs development
  * Development should use local source code and enable Node debugging
  * Both should allow end to end crawling with a single command (e.g. crawl orgName githubToken)
1. Publish images for Crawler Dashboard and Crawler to Docker Hub

## Updating the default Metabase for Docker configuratoins:
The Metabase configured by default has some canned queries and a dashboard. If you want to clear that out and start fresh, do the following:

1. Ensure you're starting from a completely clean container (docker-compose down && docker-compose up).
1. Crawl a small org to populate Mongo so you have schema/sample data to work with.
1. Open the Metabase URL and configure the questions, dashboard, etc. you want
  1. REMEMBER: Any changes you make will be persisted
1. Copy the Metabase database by changing to the docker/metabase folder in the ospo-ghcrawler repository and running:
  ```docker cp docker_metabase_1:/var/opt/metabase/dockercrawler.db.mv.db .```

Production Docker deployment using Kubernetes or the like has been discussed but not yet planned. If you have a desire to do this, please open an issue or better yet a PR and lets see what can be done.

# Working with the code

### Build
`npm install`

### Unit test
`npm test`

### Integration test
`npm run integration`

### Run
`node ./bin/www.js`



# stale content
1. Start the service crawling by going to Crawler Dashboard at [http://localhost:4000](http://localhost:4000). On the righthand side, change the ```crawler/count``` to 1 and click ```Update``` button.


## Configuration
```
{
  "NODE_ENV": "localhost",
  "CRAWLER_MODE": "Standard",
  "CRAWLER_OPTIONS_PROVIDER": ["defaults" | "memory" | "redis"],
  "CRAWLER_INSIGHTS_KEY": "[SECRET]",
  "CRAWLER_ORGS_FILE": "../orgs",
  "CRAWLER_GITHUB_TOKENS": "[SECRET]",
  "CRAWLER_REDIS_URL": "peoplesvc-dev.redis.cache.windows.net",
  "CRAWLER_REDIS_ACCESS_KEY": "[SECRET]",
  "CRAWLER_REDIS_PORT": 6380,
  "CRAWLER_QUEUE_PROVIDER": "amqp10",
  "CRAWLER_AMQP10_URL": "amqps://RootManageSharedAccessKey:[SECRET]@ghcrawlerdev.servicebus.windows.net",
  "CRAWLER_QUEUE_PREFIX": "ghcrawlerdev",
  "CRAWLER_STORE_PROVIDER": "azure",
  "CRAWLER_STORAGE_NAME": "ghcrawlerdev",
  "CRAWLER_STORAGE_ACCOUNT": "ghcrawlerdev",
  "CRAWLER_STORAGE_KEY": "[SECRET]",
  "CRAWLER_DOCLOG_STORAGE_ACCOUNT": "ghcrawlerdev",
  "CRAWLER_DOCLOG_STORAGE_KEY": "[SECRET]"
}
```

# Contributing

The project team is more than happy to take contributions and suggestions.

To start working, run ```npm install``` in the repository folder to install the required dependencies. See the usage section for pointers on how to run.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.