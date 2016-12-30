![Version](https://img.shields.io/npm/v/ghcrawler.svg)
![License](https://img.shields.io/github/license/Microsoft/ghcrawler.svg)
![Downloads](https://img.shields.io/npm/dt/ghcrawler.svg)

# GHCrawler
A robust GitHub API crawler that walks a queue of GitHub entities transitively retrieving and storing their contents. GHCrawler is great for:

* Retreiving all GitHub entities related to an org, repo, or user
* Efficiently storing and the retrieved entities
* Keeping the stored data up to date when used in conjunction with a GitHub event tracker

GHCrawler focuses on successively retrieving and walking GitHub resources supplied on a (set of) queues.  Each resource is fetched, processed, plumbed
for more resources to fetch and ultimately stored. Discovered resources are themselves queued for further processing.  The crawler is careful to not
repeatedly fetch the same resource. It makes heavy use of etags and includes GitHub token pooling and rotation to optimize use of your API tokens.

# Usage

The crawler itself is not particularly runnable. It needs to be configured with:
1. Queuing infrastructure that can take and supply *requests* to process the response from an API URL.
1. A *fetcher* that queries APIs with the URL in a given request.
1. One or more *processors* that handle requests and the fetched API document.
1. A *store* used to store the processed documents.

The best way to get running with the crawler is to look at the [OSPO-ghcrawler](https://github.com/Microsoft/ospo-ghcrawler) repo.  It has integrations for several queuing and storage technologies as well as examples of how to configure and run a crawler.

# Contributing

The project team is more than happy to take contributions and suggestions.

To start working, run ```npm install``` in the repository folder to install the required dependencies.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.