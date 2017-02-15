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

# Usage

The crawler in this repo is not particularly runnable -- it has all the business logic but little of the infrastructure. It needs to be configured with:

1. Queuing infrastructure that can take and supply *requests* to process the response from an API URL. Typically RabbitMQ or Azure Service Bus.
1. A *store* used to store the processed documents. Typically a document store such as MongoDB or Azure blob.
1. A *token factory* to manage and hand out GitHub API tokens.
1. Various *rate limiters* to suit your particular scenario.
1. An *event webhook handler* if needed.

The good news is that the [OSPO-ghcrawler](https://github.com/Microsoft/ospo-ghcrawler) repo provides everything you need as well as various configurations and a factory for creating running systems. Head over there to actually get the crawler running.

# Contributing

The project team is more than happy to take contributions and suggestions.

To start working, run ```npm install``` in the repository folder to install the required dependencies. See the usage section for pointers on how to run.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
