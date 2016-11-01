![Version](https://img.shields.io/npm/v/ghcrawler.svg)
![License](https://img.shields.io/github/license/Microsoft/ghcrawler.svg)
![Downloads](https://img.shields.io/npm/dt/ghcrawler.svg)

# GHCrawler
A robust GitHub API crawler that walks a queue of GitHub entities transitively retrieving and storing their contents. GHCrawler is great for:

* Retreiving all GitHub entities related to an org, repo, or user
* Efficiently storing and the retrieved entities
* Keeping the stored data up to date when used in conjunction with a GitHub event tracker

GHCrawler focuses on successively retrieving and walking GitHub resources supplied on a queue.  Each resource is fetched, analyzed, stored and plumbed for more resources to fetch. Discovered resources are themselves queued for further processing.  The crawler is careful to not repeatedly fetch the same resource.

# Examples

Coming...

# Contributing

The project team is more than happy to take contributions and suggestions.

To start working, run ```npm install``` in the repository folder to install the required dependencies.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.