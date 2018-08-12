![Version](https://img.shields.io/npm/v/ghcrawler.svg)
![License](https://img.shields.io/github/license/Microsoft/ghcrawler.svg)
![Downloads](https://img.shields.io/npm/dt/ghcrawler.svg)

# GHCrawler
GHCrawler is a robust GitHub API crawler that walks a queue of GitHub entities transitively retrieving and storing their contents. GHCrawler is primarily intended for people trying to track sets of orgs and repos. For example, the Microsoft Open Source Programs Office uses this to track 1000s of repos in which Microsoft is involved. In short, GHCrawler is great for:

* Retrieving all GitHub entities related to an org, repo, user, team, ...
* Efficiently storing and the retrieved entities
* Keeping the stored data up to date when used in conjunction with a GitHub webhook to track events

GHCrawler focuses on successively retrieving and walking GitHub API resources supplied on a (set of) queues. Each resource is fetched, processed, plumbed for more resources to fetch and ultimately stored. Discovered resources are themselves queued for further processing. The crawler is careful to not repeatedly fetch the same resource. It makes heavy use of etags, Redis, client-side rate limiting, and GitHub token pooling and rotation to optimize use of your API tokens and not beat up the GitHub API.

The crawler can be configured to use a variety of different queuing technologies (e.g., AMQP 1.0 and AMQP 0.9 compatible queues like Azure ServiceBus and Rabbit MQ, respectively), and storage systems (e.g., Azure Blob and MongoDB). You can create your own infrastructure plugins to use different technologies.


# Documentation
This page is essentially the Quick Start Guide for using the crawler. Detailed and complete documentation is maintained in

* This [project's wiki](https://github.com/Microsoft/ghcrawler/wiki) for documentation on the crawler itself
* The [Dashboard repo](https://github.com/Microsoft/ghcrawler-dashboard), for information on the browser-based crawler management dashboard
* The [Command line repo](https://github.com/Microsoft/ghcrawler-cli), for details of controlling the crawler from the command line

# Running in-memory
The easiest way try our the GHCrawler is to run it in memory. You can get up and running in a couple minutes.  This approach does not scale and is not persistent but it's dead simple.

1. Clone the [Microsoft/ghcrawler](https://github.com/Microsoft/ghcrawler.git) repo.
1. Run `npm install` in the clone repo directory to install the prerequisites.
1. Set the `CRAWLER_GITHUB_TOKENS` environment var to a semi-colon delimited list of [GitHub API tokens](https://developer.github.com/v3/#authentication) for rate-limiting and permissions.  For example, `set CRAWLER_GITHUB_TOKENS=432b345acd23`.
1. Run the crawler using `node bin/www.js`.

Once the service is up and running, you should see some crawler related messages in the console output every few seconds. You can control the crawler either using the `cc` command line tool or a browser-based dashboard, both of which are described below. Note that since you are running in memory, if you kill the crawler process, all work will be lost. This mode is great for playing around with the crawler or testing.

# Running Crawler-In-A-Box (CIABatta)
If you want to persist the data gathered and create some insights dashboards in small to medium production system, you can run GHCrawler in Docker with Mongo, Rabbit, and Redis infrastructure using the Crawler-in-a-box (CIABatta) approach. This setup also includes Metabase for building browser-based insights and gives you a browser-based control-panel for observing and controlling the crawler service.

***NOTE*** This is an evolving solution and the steps for running will be simplified published, ready-to-use images on Docker Hub. For now, follow these steps

1. Clone the [Microsoft/ghcrawler](https://github.com/Microsoft/ghcrawler.git) and [Microsoft/ghcrawler-dashboard](https://github.com/Microsoft/ghcrawler-dashboard.git) repos.
1. Set the `CRAWLER_GITHUB_TOKENS` environment var to a semi-colon delimited list of [GitHub API tokens](https://developer.github.com/v3/#authentication) for rate-limiting and permissions.  For example, `export CRAWLER_GITHUB_TOKENS=432b345acd23`.
1. In a command prompt go to `ghcrawler/docker` and run `docker-compose up`.

Once the containers are up and running, you should see some crawler related messages in the container's console output every few seconds. You can control the crawler either using the `cc` command line tool or the browser-based dashboard, both of which are described below.

Check out the [related GHCrawler wiki page](https://github.com/Microsoft/ghcrawler/wiki/Crawler-in-a-box) for more information on running in Docker.

# Deploying native
For ultimate flexibility, the crawler and associated bits can be run directly on VMs or as an app service. This structure typically uses cloud-based infrastructure for queuing, storage and redis. For example, this project comes with adapters for Azure Service Bus queuing and Azure Blob storage. The APIs on these adpaters is very slim so it is easy to for you to implement (and contribute) more.

***Setting up this operating mode is a bit more involved and is not fully documented.  The [wiki pages on Configuration](https://github.com/Microsoft/ghcrawler/wiki/Configuration) contain much of the raw info needed.***

# Event tracking
The crawler can hook and track GitHub events by listening webhooks.  To set this up,

1. Create a webhook on your GitHub orgs or repos and point it at the running crawler.  When events are on the webhook should point to
```
  https://<crawler machine>:3000/webhook
```
2. Set the crawler to handle webhook events by modifying the `queuing.events` property in the [Runtime configuration](https://github.com/Microsoft/ghcrawler/wiki/Configuration#runtime-configuration) or setting the `CRAWLER_EVENT_PROVIDER` [Infrastructure setting](https://github.com/Microsoft/ghcrawler/wiki/Configuration#infrastructure-settings) to have the value `webhook`.  In both cases changing the value requires a restart.  Note that you can turn off events by setting the value to `none`.

If you are using signature validation, you must set the [Infrastructure setting](https://github.com/Microsoft/ghcrawler/wiki/Configuration#infrastructure-settings) `CRAWLER_WEBHOOK_SECRET` to the value you configured into the GitHub webhook.

# Controlling the crawler
Given a running crawler service (see above), you can control it using either a simple command line app or a browser-based dashboard.

## `cc` command line

The *crawler-cli* (aka `cc`) can run interactively or as a single command processor and enables a number of basic operations.  For now the crawler-cli is not published as an npm. Instead, [clone its repo](https://github.com/Microsoft/crawler-cli.git), run `npm install` and run the command line using

```
node bin/cc -i [-s <server url>]
```

The app's built-in help has general usage info and more details can be found in [the project's readme](https://github.com/Microsoft/crawler-cli/blob/develop/README.md). A typical command sequence shown in the snippet below starts `cc` in interactive mode talking to the crawler on http://localhost:3000 (default if `-s` is not specified), configures the crawler with a public and an admin GitHub token, and then queues and starts the processing of the repo called `contoso-d/test`.

```
> node bin/cc -i
http://localhost:3000> tokens 43984b2dead7o4ene097efd97#public 972bbdfe09dead704en82309#admin
http://localhost:3000> queue contoso-d/test
http://localhost:3000> start
http://localhost:3000> exit
>
```

## Browser dashboard

The crawler dashboard gives you live feedback on what the crawler is doing as well as better control over the crawler's queues and configuration. Some configurations (e.g., Docker) include and start the dashboard for free. If you need to deploy the dashboard explicitly, clone the [Microsoft/ghcrawler-dashboard](https://github.com/Microsoft/ghcrawler-dashboard.git) repo and follow the instructions in [the README found there](https://github.com/Microsoft/ghcrawler-dashboard/blob/develop/README.md).

Once the dashboard service is up and running, point your browser at the dashboard endpoint (http://localhost:4000 by default). Detail information is included in [the dashboard README](https://github.com/Microsoft/ghcrawler-dashboard/blob/develop/README.md).

Note that the dashboard does not report queue message rates (top right graph) when used with the memory-based crawler service as that mechanism requires Redis to record activity.

# Working with the code

### Node version
`>=6`

### Build
  `npm install`

### Unit test
  `npm test`

### Integration test
  `npm run integration`

### Run
  `node ./bin/www.js`

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

# Known issues

It is clearly early days for the crawler so there are a number of things left to do. Most of the concrete work items are captured in repo issues. Broadly speaking there are several types of work:

* Documentation -- The crawler code itself is relatively straightforward but not all of the architecture, control and extensibility points are not called out.
* Completeness -- There are a few functional gaps in certain scenarios that need to be addressed.
* Docker configuration -- Several items in making the Docker configuration real
* Analysis and insights -- Metabase is supplied in the Docker configuration but relatively little has been done with analyzing the harvested data.
