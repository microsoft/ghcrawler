const moment = require('moment');
const Q = require('q');

class Crawler {
  constructor(queue, store, requestor, config, logger) {
    this.seen = {};
    this.queue = queue;
    this.store = store;
    this.requestor = requestor;
    this.config = config;
    this.logger = logger;
  }

  start() {
    return this.queue.pop()
      .then(this._filterSeen.bind(this))
      .then(request => {
        if (request) {
          this.logger.log('info', `${request.url} [${request.type}]`);
        }
        return request;
      })
      .then(this._fetch.bind(this))
      .then(this._convertToDocument.bind(this))
      .then(this._processDocument.bind(this))
      .then(this._storeDocument.bind(this))
      .then(this._deleteFromQueue.bind(this))
      .then(this._markSeen.bind(this))
      .then(this._startNext.bind(this));
  }

  _startNext() {
    setTimeout(this.start.bind(this), 0);
  }

  _filterSeen(request) {
    if (!request || this.seen[request.url]) {
      return Q.resolve();
    }

    return Q.resolve(request);
  }

  _fetch(request) {
    if (!request) {
      return Q.resolve();
    }

    return this._getRequestor().getAll(request.url)
      .then(githubResponse => {
        request.response = githubResponse;
        return request;
      });
  }

  _convertToDocument(request) {
    if (!request) {
      return Q.resolve();
    }

    request.response._metadata = {
      type: request.type,
      url: request.url,
      fetchedAt: moment.utc().toISOString(),
      links: {}
    };

    return Q.resolve(request);
  }

  _processDocument(request) {
    if (!request) {
      return Q.resolve();
    }
    let document = null;
    switch (request.type) {
      case 'orgs': {
        document = this._processCollection(request.response, 'login', request.context);
        break;
      }
      case 'repo': {
        document = this._processRepo(request.response, request.context);
        break;
      }
      case 'login': {
        document = this._processLogin(request.response, request.context);
        break;
      }
      case 'repos': {
        document = this._processCollection(request.response, 'repo', request.context);
        break;
      }
      case 'issues': {
        document = this._processCollection(request.response, 'issue', request.context);
        break;
      }
      case 'issue': {
        document = this._processIssue(request.response, request.context);
        break;
      }
      case 'issue_comments': {
        document = this._processCollection(request.response, 'issue_comment', request.context);
        break;
      }
      case 'issue_comment': {
        document = this._processIssueComment(request.response, request.context);
        break;
      }
      case 'commits': {
        document = this._processCollection(request.response, 'commit', request.context);
        break;
      }
      case 'commit': {
        document = this._processCommit(request.response, request.context);
        break;
      }
    }
    request.document = document;
    return Q.resolve(request);
  }

  _storeDocument(request) {
    if (!request) {
      return Q.resolve();
    }

    if (!this.store || !request.document) {
          if (!err.repsonse || err.response && (err.response.statusCode >= 500 || err.response.statusCode === 403)) {
      return Q.resolve(request);
    }

    return this.store.upsert(request.document).then((upsert) => {
      request.upsert = upsert;
      return request;
    });
  }

  _deleteFromQueue(request) {
    if (!request) {
      return Q.resolve();
    }

    return this.queue.done(request).then(() => { return request; });
  }

  _markSeen(request) {
    if (!request) {
      return Q.resolve();
    }
    this.seen[request.url] = true;
    return Q.resolve(request);
  }

  _getRequestor() {
    return new this.requestor({
      headers: {
        authorization: this.config.githubToken
      }
    });
  }

  _processCollection(document, type, context) {
    document.forEach(item => {
      this.queue.push(type, item.url, context);
    });
    return null;
  }

  _processRepo(document) {
    document._metadata.links.self = { type: 'self', href: `urn:repo:${document.id}` };
    document._metadata.links.owner = { type: 'self', href: `urn:login:${document.owner.id}` };
    document._metadata.links.parent = { type: 'self', href: `urn:login:${document.owner.id}` };
    document._metadata.links.siblings = { type: 'siblings', href: `urn:login:${document.owner.id}:repos` };
    this.queue.push('login', document.owner.url);
    this.queue.push('issues', document.issues_url.replace('{/number}', ''), { repo: document.id });
    this.queue.push('commits', document.commits_url.replace('{/sha}', ''), { repo: document.id });
    return document;
  }

  _processCommit(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:commit:${document.sha}` };
    document._metadata.links.siblings = { type: 'siblings', href: `urn:repo:${context.repo}:commits` };
    document._metadata.links.repo = { type: 'self', href: `urn:repo:${context.repo}` };
    document._metadata.links.parent = document._metadata.links.parent;
    if (document.author) {
      document._metadata.links.author = { type: 'self', href: `urn:login:${document.author.id}` };
      this.queue.push('login', document.author.url);
    }
    if (document.committer) {
      document._metadata.links.committer = { type: 'self', href: `urn:login:${document.committer.id}` };
      this.queue.push('login', document.committer.url);
    }
    if (document.files) {
      document.files.forEach(file => {
        delete file.patch;
      });
    }
    return document;
  }

  _processLogin(document) {
    document._metadata.links.self = { type: 'self', href: `urn:login:${document.id}` };
    document._metadata.links.repos = { type: 'siblings', href: `urn:login:${document.id}:repos` };
    document._metadata.links.siblings = { type: 'siblings', href: 'urn:login' };
    this.queue.push('repos', document.repos_url);
    return document;
  }

  _processIssue(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:issue:${document.id}` };
    document._metadata.links.siblings = { type: 'siblings', href: `urn:repo:${context.repo}:issues` };
    document._metadata.links.assignees = { type: 'self', hrefs: document.assignees.map(assignee => { return `urn:login:${assignee.id}` }) };
    document._metadata.links.repo = { type: 'self', href: `urn:repo:${context.repo}` };
    document._metadata.links.parent = document._metadata.links.repo;
    document._metadata.links.user = { type: 'self', href: `urn:login:${document.user.id}` };
    this.queue.push('login', document.user.url);
    if (document.assignee) {
      document._metadata.links.assignee = { type: 'self', href: `urn:login:${document.assignee.id}` };
      this.queue.push('login', document.assignee.url);
    }
    if (document.closed_by) {
      document._metadata.links.closed_by = { type: 'self', href: `urn:login:${document.closed_by.id}` };
      this.queue.push('login', document.closed_by.url);
    }

    // milestone
    // pull request
    // events
    // labels
    this.queue.push('issue_comments', document.comments_url, { issue: document.id, repo: context.repo });
    return document;
  }

  _processIssueComment(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:issue_comment:${document.id}` };
    document._metadata.links.user = { type: 'self', href: `urn:login:${document.user.id}` };
    document._metadata.links.siblings = { type: 'siblings', href: `urn:repo:${context.repo}:issue:${context.issue}:comments` };
    this.queue.push('login', document.user.url);
    return document;
  }
}

module.exports = Crawler;
