const extend = require('extend');
const moment = require('moment');
const parse = require('parse-link-header');
const Q = require('q');
const URL = require('url');

const collections = {
  orgs: 'org', repos: 'repo', issues: 'issue', issue_comments: 'issue_comment', commits: 'commit', teams: 'team', users: 'user'
};

class Crawler {
  constructor(queue, priorityQueue, store, requestor, config, logger) {
    this.queue = queue;
    this.priorityQueue = priorityQueue;
    this.store = store;
    this.requestor = requestor;
    this.config = config;
    this.logger = logger;
  }

  start() {
    return this._pop(this.priorityQueue)
      .then(this._pop.bind(this, this.queue))
      .then(this._trackStart.bind(this))
      .then(this._filter.bind(this))
      .then(this._fetch.bind(this))
      .then(this._convertToDocument.bind(this))
      .then(this._processDocument.bind(this))
      .then(this._storeDocument.bind(this))
      .then(this._deleteFromQueue.bind(this))
      .then(this._logOutcome.bind(this))
      .then(this._startNext.bind(this))
      .catch(error => {
        this.logger.log('error', `${error.message}`);
      });
  }

  _pop(queue, request = null) {
    return request ? Q(request) : queue.pop();
  }

  _trackStart(request) {
    request.start = Date.now();
    return Q(request);
  }

  _startNext() {
    setTimeout(this.start.bind(this), 0);
  }

  _filter(request) {
    if (this._configFilter(request.type, request.url)) {
      this._markSkip(request, 'Filtered');
    }
    return Q.resolve(request);
  }

  _fetch(request) {
    if (request.skip) {
      return Q.resolve(request);
    }
    // rewrite the request type for collections remember the collection subType
    // Also setup 'page' as the document type to look up for etags etc.
    let fetchType = request.type;
    let subType = collections[request.type];
    if (subType) {
      request.type = 'collection';
      request.subType = subType;
      fetchType = 'page';
    }
    const self = this;
    return this.store.etag(fetchType, request.url).then(etag => {
      const options = etag ? { headers: { 'If-None-Match': etag } } : {};
      const start = Date.now();
      return self.requestor.get(request.url, options).then(githubResponse => {
        const status = githubResponse.statusCode;
        this._addMeta(request, { status: status, fetch: Date.now() - start });
        if (status !== 200 && status !== 304) {
          self._markSkip(request, 'Error', `Code: ${status} for: ${request.url}`);
          return request;
        }

        if (status === 304 && githubResponse.headers.etag === etag) {
          // We have the content for this element.  If it is immutable, skip.
          // Otherwise get it from the store and process.
          if (!request.force) {
            return self._markSkip(request, 'Unmodified');
          }
          return self.store.get(fetchType, request.url).then(document => {
            request.document = document;
            request.response = githubResponse;
            // Our store is up to date so don't '
            request.store = false;
            return request;
          });
        }
        request.document = githubResponse.body;
        request.response = githubResponse;
        return request;
      });
    }).catch(error => {
      // TODO can this request be requeued?
      return this._markSkip(request, 'Error', error.message);
    });
  }

  _convertToDocument(request) {
    if (request.skip) {
      return Q.resolve(request);
    }

    // If the doc is an array, wrap it in an object to make storage more consistent (Mongo can't store arrays directly)
    if (Array.isArray(request.document)) {
      request.document = { elements: request.document };
    }
    request.document._metadata = {
      type: request.type,
      url: request.url,
      etag: request.response.headers.etag,
      fetchedAt: moment.utc().toISOString(),
      links: {}
    };
    request.promises = [];
    return Q.resolve(request);
  }

  _processDocument(request) {
    if (request.skip) {
      return Q.resolve(request);
    }
    const handler = this[request.type];
    if (!handler) {
      this._markSkip(request, 'Error', `No handler found for request type: ${request.type}`);
      return request;
    }

    request.document = handler.call(this, request);
    return Q.resolve(request);
  }

  _storeDocument(request) {
    // See if we should skip storing the document.  Test request.store explicitly for false as it may just not be set.
    if (request.skip || !this.store || !request.document || request.store === false) {
      return Q.resolve(request);
    }

    return this.store.upsert(request.document).then((upsert) => {
      request.upsert = upsert;
      return request;
    });
  }

  _deleteFromQueue(request) {
    if (!request.message) {
      return Q.resolve(request);
    }
    return this.queue.done(request).then(() => { return request; });
  }

  _logOutcome(request) {
    const outcome = request.outcome ? request.outcome : 'Processed';
    const message = request.message;
    this._addMeta(request, { total: Date.now() - request.start });
    this.logger.log('info', `${outcome} ${request.type} [${request.url}] ${message || ''}`, request.meta);
    return request;
  }

  _addMeta(request, data) {
    request.meta = extend({}, request.meta, data);
    return request;
  }

  // ===============  Entity Processors  ============

  collection(request) {
    // if there are additional pages, queue them up to be processed.  Note that these go
    // on the high priority queue so they are loaded before they change much.
    const linkHeader = request.response.headers.link;
    if (linkHeader) {
      const links = parse(linkHeader);
      for (let i = 2; i <= links.last.page; i++) {
        const url = request.url + `?page=${i}&per_page=100`;
        const context = { qualifier: request.context.qualifier };
        this._queueBase(request, { type: 'page', url: url, subType: request.subType, page: i, force: request.force, context: context }, this.priorityQueue);
      }
    }

    // Rewrite the request and document to be a 'page' and then process.
    request.page = 1;
    request.document._metadata.type = 'page';
    return this.page(request);
  }

  page(request) {
    const document = request.document;
    const type = request.subType;
    const first = document.elements[0];
    const qualifier = request.context.qualifier;
    this._linkSelf(request, 'self', `${qualifier}:${type}:pages:${request.page}`);
    document.elements.forEach(item => {
      this._queueChild(request, type, item.url, qualifier);
    });
    return document;
  }

  org(request) {
    const document = request.document;
    this._addSelfLink(request, 'urn:');
    this._linkSiblings(request, 'repos', `urn:org:${document.id}:repos`);
    this._linkSiblings(request, 'siblings', 'urn:org');
    this._queueChildren(request, 'repos', document.repos_url);
    // TODO is this "logins"
    this._queueChildren(request, 'users', document.members_url.replace('{/member}', ''));
    return document;
  }

  user(request) {
    const document = request.document;
    this._addSelfLink(request, 'urn:');
    this._linkSiblings(request, 'repos', `urn:user:${document.id}:repos`);
    this._linkSiblings(request, 'siblings', 'urn:user');
    this._queueChildren(request, 'repos', document.repos_url);
    return document;
  }

  repo(request) {
    const document = request.document;
    this._addSelfLink(request, 'urn:');
    this._linkSelf(request, 'owner', `urn:login:${document.owner.id}`);
    this._linkSelf(request, 'parent', `urn:login:${document.owner.id}`);
    this._linkSiblings(request, 'siblings', `urn:login:${document.owner.id}:repos`);
    this._queueRoot(request, 'login', document.owner.url);
    this._queueChildren(request, 'issues', document.issues_url.replace('{/number}', ''), { repo: document.id });
    this._queueChildren(request, 'commits', document.commits_url.replace('{/sha}', ''), { repo: document.id });
    return document;
  }

  commit(request) {
    const document = request.document;
    const context = request.context;
    this._addSelfLink(request, null, 'sha');

    this._linkSelf(request, 'repo', `urn:repo:${context.repo}`);
    this._linkSiblings(request, 'siblings', `urn:repo:${context.repo}:commits`);
    // TODO not sure what the following line does
    // document._metadata.links.parent = document._metadata.links.parent;
    if (document.author) {
      this._linkSelf(request, 'author', `urn:login:${document.author.id}`);
      this._queueRoot(request, 'login', document.author.url);
    }
    if (document.committer) {
      this._linkSelf(request, 'committer', `urn:login:${document.committer.id}`);
      this._queueRoot(request, 'login', document.committer.url);
    }
    if (document.files) {
      document.files.forEach(file => {
        delete file.patch;
      });
    }
    return document;
  }

  login(request) {
    const document = request.document;
    this._addSelfLink(request, 'urn:');
    this._linkSelf(request, 'self', `urn:login:${document.id}`);
    // TODO should we do repos here and in the user/org?
    this._linkSiblings(request, 'repos', `urn:login:${document.id}:repos`);
    this._linkSiblings(request, 'siblings', 'urn:login');
    if (document.type === 'Organization') {
      this._queueRoot(request, 'org', `https://api.github.com/orgs/${document.login}`);
    } else if (document.type === 'User') {
      this._queueRoot(request, 'user', `https://api.github.com/users/${document.login}`);
    }
    this._queueChildren(request, 'repos', document.repos_url);
    return document;
  }

  issue(request) {
    const document = request.document;
    const context = request.context;
    this._addSelfLink(request);
    this._linkSelf(request, 'assignees', document.assignees.map(assignee => { return `urn:login:${assignee.id}`; }));
    this._linkSelf(request, 'repo', `urn:repo:${context.repo}`);
    this._linkSelf(request, 'parent', `urn:repo:${context.repo}`);
    this._linkSelf(request, 'user', `urn:login:${document.user.id}`);
    this._linkSiblings(request, 'siblings', `urn:repo:${context.repo}:issues`);
    this._queueRoot(request, 'login', document.user.url);
    if (document.assignee) {
      this._linkSelf(request, 'assignee', `urn:login:${document.assignee.id}`);
      this._queueRoot(request, 'login', document.assignee.url);
    }
    if (document.closed_by) {
      this._linkSelf(request, 'closed_by', `urn:login:${document.closed_by.id}`);
      this._queueRoot(request, 'login', document.closed_by.url);
    }

    // milestone
    // pull request
    // events
    // labels
    this._queueChildren(request, 'issue_comments', document.comments_url, { issue: document.id, repo: context.repo });
    return document;
  }

  issue_comment(request) {
    const document = request.document;
    const context = request.context;
    this._addSelfLink(request);
    this._linkSelf(request, 'user', `urn:login:${document.user.id}`);
    this._linkSiblings(request, 'siblings', `urn:repo:${context.repo}:issue:${context.issue}:comments`);
    this._queue(request, 'login', document.user.url);
    return document;
  }

  team(request) {
    const document = request.document;
    this._addSelfLink(request, `urn:org:${document.organization.id}`);
    this._linkSelf(request, 'org', `urn:org:${document.organization.id}`);
    this._linkSelf(request, 'login', `urn:login:${document.organization.id}`);
    this._linkSiblings(request, 'siblings', `urn:org:${document.organization.id}:teams`);
    this._queueChildren(request, 'team_members', document.members_url);
    this._queueChildren(request, 'team_repos', document.repositories_url);
    return document;
  }

  team_members(request) {
    const document = request.document;
    this._addSelfLink(request, `urn:org:${document.organization.id}`);
    return document;
  }

  team_repos(request) {
    this._addSelfLink(request, `urn:org:${document.organization.id}`);
    return document;
  }

  // ===============  Event Processors  ============
  CommitCommentEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(request);
    this._linkSelf(request, 'comment', `urn:repo:${context.repo}:comment:${payload.comment.id}`);
    // TODO siblings?
    this._queue(request, 'comment', payload.comment.url);
    return document;
  }

  CreateEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    return document;
  }

  DeleteEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    // TODO do something for interesting deletions e.g.,  where ref-type === 'repository'
    return document;
  }

  DeploymentEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    this._linkSelf(request, 'deployment', `urn:repo:${context.repo}:deployment:${payload.deployment.id}`);
    this._queue(request, 'deployment', payload.deployment.url);
    return document;
  }

  DeploymentStatusEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    this._linkSelf(request, 'deployment_status', `urn:repo:${context.repo}:deployment:${payload.deployment.id}:status:${payload.deployment_status.id}`);
    this._linkSelf(request, 'deployment', `urn:repo:${context.repo}:deployment:${payload.deployment.id}`);
    this._queue(request, 'deployment', payload.deployment.url);
    return document;
  }

  ForkEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    // TODO figure out what else to do
    return document;
  }

  GollumEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    return document;
  }

  IssueCommentEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    this._linkSelf(request, 'issue', `urn:repo:${context.repo}:issue:${payload.issue.id}`);
    this._linkSelf(request, 'comment', `urn:repo:${context.repo}:comment:${payload.comment.id}`);
    this._queue(request, 'comment', payload.comment.url);
    this._queue(request, 'issue', payload.issue.url);
    return document;
  }

  IssuesEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    this._linkSelf(request, 'issue', `urn:repo:${context.repo}:issue:${payload.issue.id}`);
    this._queue(request, 'issue', payload.issue.url);
    return document;
  }

  LabelEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    return document;
  }

  MemberEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    this._linkSelf(request, 'member', `urn:login:${payload.member.id}`);
    this._queueRoot(request, 'login', payload.member.url);
    return document;
  }

  MembershipEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    this._linkSelf(request, 'self', `urn:team:${payload.team.id}:membership_event:${document.id}`);
    this._linkSelf(request, 'member', `urn:login:${payload.member.id}`);
    this._linkSelf(request, 'team', `urn:team:${payload.team.id}`);
    this._linkSelf(request, 'org', `urn:org:${payload.organization.id}`);
    this._queueRoot(request, 'login', payload.member.url);
    this._queueRoot(request, 'org', payload.organization.url);
    this._queue(request, 'team', payload.team.url);
    return document;
  }

  MilestoneEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    this._linkSelf(request, 'milestone', `urn:repo:${context.repo}:milestone:${payload.milestone.id}`);
    this._queue(request, 'milestone', payload.milestone.url);
    return document;
  }

  PageBuildEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    this._linkSelf(request, 'page_build', `urn:repo:${context.repo}:page_builds:${payload.id}`);
    this._queue(request, 'page_build', payload.build.url);
    return document;
  }

  PublicEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    return document;
  }

  PullRequestEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    this._linkSelf(request, 'pull', `urn:repo:${context.repo}:pull:${payload.pull_request.id}`);
    this._queue(request, 'pull', payload.pull_request.url);
    return document;
  }

  PullRequestReviewEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    this._linkSelf(request, 'review', `urn:repo:${context.repo}:pull:${payload.pull_request.id}:review:${payload.review.id}`);
    this._linkSelf(request, 'pull', `urn:repo:${context.repo}:pull:${payload.pull_request.id}`);
    this._queue(request, 'pull_review', payload.pull_request.review_comment_url.replace('{/number}', `/${payload.review.id}`));
    this._queue(request, 'pull', payload.pull_request.url);
    return document;
  }

  PullRequestReviewCommentEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    this._linkSelf(request, 'comment', `urn:repo:${context.repo}:pull:${payload.pull_request.id}:comment:${payload.comment.id}`);
    this._linkSelf(request, 'pull', `urn:repo:${context.repo}:pull:${payload.pull_request.id}`);
    // TODO see if all the various comments can be the same type
    this._queue(request, 'pull_comment', payload.comment.url);
    this._queue(request, 'pull', payload.pull_request.url);
    return document;
  }

  PushEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = this._eventHelper(document);
    // TODO figure out what to do with the commits
    return document;
  }

  // ===============  Helpers  ============

  _addSelfLink(request, base = null, key = 'id') {
    let qualifier = base ? base : request.context.qualifier;
    qualifier = qualifier.endsWith(':') ? qualifier : qualifier + ':';
    this._linkSelf(request, 'self', `${qualifier}${request.type}:${request.document[key]}`);
  }

  _linkSelf(request, name, value) {
    const links = request.document._metadata.links;
    const key = Array.isArray(value) ? 'hrefs' : 'href';
    links[name] = { type: 'self' };
    links[name][key] = value;
  }

  _linkSiblings(request, name, href) {
    const links = request.document._metadata.links;
    links[name] = { type: 'siblings', href: href };
  }

  _queue(request, type, url, context, queue = null) {
    const newRequest = { type: type, url: url };
    newRequest.context = context;
    this._queueBase(request, newRequest, queue);
  }

  _queueRoot(request, type, url) {
    this._queueBase(request, { type: type, url: url });
  }

  _queueChild(request, type, url, qualifier) {
    const newRequest = { type: type, url: url };
    newRequest.context = request.context || {};
    newRequest.context.qualifier = qualifier;
    if (request.force) {
      newRequest.force = request.force;
    }
    this._queueBase(request, newRequest);
  }

  _queueChildren(request, type, url, context = null) {
    const newRequest = { type: type, url: url };
    const newContext = extend(request.context || {}, context);
    newRequest.context = newContext;
    newContext.qualifier = request.document._metadata.links.self.href;
    if (request.force) {
      newRequest.force = request.force;
    }
    this._queueBase(request, newRequest);
  }

  // TODO make a queue all and add promises (then) to the code below
  _queueBase(request, newRequest, queue = null) {
    if (this._configFilter(newRequest.type, newRequest.url)) {
      this.logger.log('info', `Skipped queuing ${newRequest.type} [${newRequest.url}]`);
      return;
    }
    queue = queue || this.queue;
    request.promises.push(queue.push(newRequest));
  }

  _configFilter(type, target) {
    if (!this.config.orgFilter) {
      return false;
    }
    if (type === 'repo' || type === 'repos' || type === 'org') {
      const parsed = URL.parse(target);
      const org = parsed.path.split('/')[2];
      return !this.config.orgFilter.has(org.toLowerCase());
    }
    return false;
  }

  _markSkip(request, outcome, message) {
    request.skip = true;
    request.outcome = request.outcome || outcome;
    request.message = request.message || message;
    return request;
  }

  _eventHelper(request, references) {
    const document = request.document;
    // TODO understand if the actor is typically the same as the creator or pusher in the payload
    const repo = document.repo ? document.repo.id : null;
    const urn = repo ? `urn:repo:${repo}` : `urn:org:${document.org.id}`;
    this._linkSelf(request, 'self', `${urn}:${request.type}:${document.id}`);
    this._linkSelf(request, 'actor', `urn:login:${document.actor.id}`);
    this._linkSelf(request, 'repo', `urn:repo:${document.repo.id}`);
    this._linkSelf(request, 'org', `urn:org:${document.org.id}`);
    this._queueRoot(request, 'login', document.actor.url);
    this._queueRoot(request, 'repo', document.repo.url);
    this._queueRoot(request, 'org', document.org.url);
    return document.payload;
  }

  _isCollectionRequest(request) {
    return collections.hasOwnProperty(request.type);
  }
}

module.exports = Crawler;