const extend = require('extend');
const moment = require('moment');
const Q = require('q');
const url = require('url');

const collections = {
  orgs: 'org', repos: 'repo', issues: 'issue', issue_comments: 'issue_comment', commits: 'commit', teams: 'team', users: 'user'
};

const immutable = new Set([
  'commit', 'CommitCommentEvent', 'CreateEvent', 'DeleteEvent', 'DeploymentEvent', 'DeploymentStatusEvent', 'DownloadEvent', 'FollowEvent', 'ForkEvent', 'ForkApplyEvent', 'GistEvent', 'GollumEvent', 'IssueCommentEvent', 'IssuesEvent', 'LabelEvent', 'MemberEvent', 'MembershipEvent', 'MilestoneEvent', 'PageBuildEvent', 'PublicEvent', 'PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent', 'PushEvent', 'ReleaseEvent', 'RepositoryEvent', 'StatusEvent', 'TeamAddEvent', 'WatchEvent']);

class Crawler {
  constructor(queue, store, requestor, config, logger) {
    this.queue = queue;
    this.store = store;
    this.requestor = requestor;
    this.config = config;
    this.logger = logger;
  }

  start() {
    return this.queue.pop()
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

    const result = this._isCollectionRequest(request) ? this._fetchCollection(request) : this._fetchSingle(request);
    return result.catch(error => {
      // TODO retryable vs non-retryable
      return this._markSkip(request, 'Error', error.message);
    });
  }

  _fetchSingle(request) {
    const self = this;
    return this.store.etag(request.type, request.url).then(etag => {
      const options = etag ? { headers: { 'If-None-Match': etag } } : {};
      return this.requestor.get(request.url, options)
        .then(githubResponse => {
          request.response = githubResponse.body;
          request.response._metadata = {
            etag: githubResponse.headers.etag
          };
          return request;
        },
        (error) => {
          // if the current doc is the same as what we've already seen then skip
          // TODO  Do we really want the requestor to return an error for the 304 case?  What about redirections?
          if (error && error.response && error.response.statusCode === 304) {
            // We have the content for this element.  If it is immutable, skip.
            //Otherwise get it from the store and process.
            if (immutable.has(request.type)) {
              return this._markSkip(request, 'Unmodified');
            }
            return this.store.get(request.type, request.url).then(document => {
              request.response = document;
              return request;
            });
          }
          // TODO rethrow here or change requestor to not error on 304
          return Q.reject(error);
        });
    });
  }

  _fetchCollection(request) {
    return this.requestor.getAll(request.url).then(
      githubResponse => {
        request.response = githubResponse;
        return request;
      },
      error => {
        if (error.response.statusCode === 409) {
          request.response = [];
          return request;
        }
        request.error = error;
      });
  }

  _convertToDocument(request) {
    if (request.skip) {
      return Q.resolve(request);
    }

    request.response._metadata = request.response._metadata || {};
    const newMetadata = {
      type: request.type,
      url: request.url,
      fetchedAt: moment.utc().toISOString(),
      links: {}
    };
    extend(request.response._metadata, newMetadata);
    return Q.resolve(request);
  }

  _processDocument(request) {
    if (request.skip) {
      return Q.resolve(request);
    }
    let document = null;
    if (collections.hasOwnProperty(request.type)) {
      document = this._processCollection(request.response, collections[request.type], request.context);
    } else {
      const handler = this[request.type];
      if (handler && typeof handler === 'function') {
        document = handler.call(this, request.response, request.context);
      } else {
        // TODO log something saying we did not know how to handle the type
      }
    }
    request.document = document;
    return Q.resolve(request);
  }

  _storeDocument(request) {
    if (request.skip || !this.store || !request.document) {
      return Q.resolve(request);
    }

    return this.store.upsert(request.document).then((upsert) => {
      request.upsert = upsert;
      return request;
    });
  }

  _queue(type, url, context) {
    if (this._configFilter(type, url)) {
      this.logger.log('info', `Skipped queuing ${type} [${url}]`);
    } else {
      this.queue.push(type, url, context);
    }
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
    this.logger.log('info', `${outcome} ${request.type} [${request.url}] ${message || ''}`);
    return request;
  }

  // ===============  Entity Processors  ============

  _processCollection(document, type, context) {
    document.forEach(item => {
      this._queue(type, item.url, context);
    });
    return null;
  }

  org(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:org:${document.id}` };
    document._metadata.links.repos = { type: 'siblings', href: `urn:org:${document.id}:repos` };
    document._metadata.links.siblings = { type: 'siblings', href: 'urn:org' };
    this._queue('repos', document.repos_url);
    // TODO is this "logins"
    this._queue('users', document.members_url.replace('{/member}', ''));
    return document;
  }

  user(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:user:${document.id}` };
    document._metadata.links.repos = { type: 'siblings', href: `urn:user:${document.id}:repos` };
    document._metadata.links.siblings = { type: 'siblings', href: 'urn:user' };
    this._queue('repos', document.repos_url);
    return document;
  }

  repo(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:repo:${document.id}` };
    document._metadata.links.owner = { type: 'self', href: `urn:login:${document.owner.id}` };
    document._metadata.links.parent = { type: 'self', href: `urn:login:${document.owner.id}` };
    document._metadata.links.siblings = { type: 'siblings', href: `urn:login:${document.owner.id}:repos` };
    this._queue('login', document.owner.url);
    this._queue('issues', document.issues_url.replace('{/number}', ''), { repo: document.id });
    this._queue('commits', document.commits_url.replace('{/sha}', ''), { repo: document.id });
    return document;
  }

  commit(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:commit:${document.sha}` };
    document._metadata.links.siblings = { type: 'siblings', href: `urn:repo:${context.repo}:commits` };
    document._metadata.links.repo = { type: 'self', href: `urn:repo:${context.repo}` };
    document._metadata.links.parent = document._metadata.links.parent;
    if (document.author) {
      document._metadata.links.author = { type: 'self', href: `urn:login:${document.author.id}` };
      this._queue('login', document.author.url);
    }
    if (document.committer) {
      document._metadata.links.committer = { type: 'self', href: `urn:login:${document.committer.id}` };
      this._queue('login', document.committer.url);
    }
    if (document.files) {
      document.files.forEach(file => {
        delete file.patch;
      });
    }
    return document;
  }

  login(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:login:${document.id}` };
    // TODO should we do repos here and in the user/org?
    document._metadata.links.repos = { type: 'siblings', href: `urn:login:${document.id}:repos` };
    document._metadata.links.siblings = { type: 'siblings', href: 'urn:login' };
    if (document.type === 'Organization') {
      this._queue('org', `https://api.github.com/orgs/${document.login}`);
    } else if (document.type === 'User') {
      this._queue('user', `https://api.github.com/users/${document.login}`);
    }
    this._queue('repos', document.repos_url);
    return document;
  }

  issue(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:issue:${document.id}` };
    document._metadata.links.siblings = { type: 'siblings', href: `urn:repo:${context.repo}:issues` };
    document._metadata.links.assignees = { type: 'self', hrefs: document.assignees.map(assignee => { return `urn:login:${assignee.id}` }) };
    document._metadata.links.repo = { type: 'self', href: `urn:repo:${context.repo}` };
    document._metadata.links.parent = document._metadata.links.repo;
    document._metadata.links.user = { type: 'self', href: `urn:login:${document.user.id}` };
    this._queue('login', document.user.url);
    if (document.assignee) {
      document._metadata.links.assignee = { type: 'self', href: `urn:login:${document.assignee.id}` };
      this._queue('login', document.assignee.url);
    }
    if (document.closed_by) {
      document._metadata.links.closed_by = { type: 'self', href: `urn:login:${document.closed_by.id}` };
      this._queue('login', document.closed_by.url);
    }

    // milestone
    // pull request
    // events
    // labels
    this._queue('issue_comments', document.comments_url, { issue: document.id, repo: context.repo });
    return document;
  }

  issue_comment(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:issue_comment:${document.id}` };
    document._metadata.links.user = { type: 'self', href: `urn:login:${document.user.id}` };
    document._metadata.links.siblings = { type: 'siblings', href: `urn:repo:${context.repo}:issue:${context.issue}:comments` };
    this._queue('login', document.user.url);
    return document;
  }

  team(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:org:${document.organization.id}:team:${document.id}` };
    document._metadata.links.org = { type: 'self', href: `urn:org:${document.organization.id}` };
    document._metadata.links.login = { type: 'self', href: `urn:login:${document.organization.id}` };
    document._metadata.links.siblings = { type: 'siblings', href: `urn:org:${document.organization.id}:teams` };
    this._queue('team_members', document.members_url);
    this._queue('team_repos', document.repositories_url);
    return document;
  }

  team_members(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:org:${document.organization.id}:team:${document.id}:members` };
    return document;
  }

  team_repos(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:org:${document.organization.id}:team:${document.id}:repos` };
    return document;
  }

  // ===============  Event Processors  ============
  CommitCommentEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:commit_comment_event:${document.id}` };
    document._metadata.links.comment = { type: 'self', href: `urn:repo:${context.repo}:comment:${payload.comment.id}` };
    // TODO siblings?
    this._queue('comment', payload.comment.url);
    return document;
  }

  CreateEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:create_event:${document.id}` };
    return document;
  }

  DeleteEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:delete_event:${document.id}` };
    // TODO do something for interesting deletions e.g.,  where ref-type === 'repository'
    return document;
  }

  DeploymentEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:deployment_event:${document.id}` };
    document._metadata.links.deployment = { type: 'self', href: `urn:repo:${context.repo}:deployment:${payload.deployment.id}` };
    this._queue('deployment', payload.deployment.url);
    return document;
  }

  DeploymentStatusEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:deployment_status_event:${document.id}` };
    document._metadata.links.deployment_status = { type: 'self', href: `urn:repo:${context.repo}:deployment:${payload.deployment.id}:status:${payload.deployment_status.id}` };
    document._metadata.links.deployment = { type: 'self', href: `urn:repo:${context.repo}:deployment:${payload.deployment.id}` };
    this._queue('deployment', payload.deployment.url);
    return document;
  }

  ForkEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:fork_event:${document.id}` };
    // TODO figure out what else to do
    return document;
  }

  GollumEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:gollum_event:${document.id}` };
    return document;
  }

  IssueCommentEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:issue_comment_event:${document.id}` };
    document._metadata.links.issue = { type: 'self', href: `urn:repo:${context.repo}:issue:${payload.issue.id}` };
    document._metadata.links.comment = { type: 'self', href: `urn:repo:${context.repo}:comment:${payload.comment.id}` };
    this._queue('comment', payload.comment.url);
    this._queue('issue', payload.issue.url);
    return document;
  }

  IssuesEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:issued_event:${document.id}` };
    document._metadata.links.issue = { type: 'self', href: `urn:repo:${context.repo}:issue:${payload.issue.id}` };
    this._queue('issue', payload.issue.url);
    return document;
  }

  LabelEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:label_event:${document.id}` };
    return document;
  }

  MemberEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:member_event:${document.id}` };
    document._metadata.links.member = { type: 'self', href: `urn:login:${payload.member.id}` };
    this._queue('login', payload.member.url);
    return document;
  }

  MembershipEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:team:${payload.team.id}:membership_event:${document.id}` };
    document._metadata.links.member = { type: 'self', href: `urn:login:${payload.member.id}` };
    document._metadata.links.team = { type: 'self', href: `urn:team:${payload.team.id}` };
    document._metadata.links.org = { type: 'self', href: `urn:org:${payload.organization.id}` };
    this._queue('login', payload.member.url);
    this._queue('org', payload.organization.url);
    this._queue('team', payload.team.url);
    return document;
  }

  MilestoneEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:milestone_event:${document.id}` };
    document._metadata.links.milestone = { type: 'self', href: `urn:repo:${context.repo}:milestone:${payload.milestone.id}` };
    this._queue('milestone', payload.milestone.url);
    return document;
  }

  PageBuildEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:page_build_event:${document.id}` };
    document._metadata.links.page_build = { type: 'self', href: `urn:repo:${context.repo}:page_builds:${payload.id}` };
    this._queue('page_build', payload.build.url);
    return document;
  }

  PublicEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:public_event:${document.id}` };
    return document;
  }

  PullRequestEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:pull_request_event:${document.id}` };
    document._metadata.links.pull = { type: 'self', href: `urn:repo:${context.repo}:pull:${payload.pull_request.id}` };
    this._queue('pull', payload.pull_request.url);
    return document;
  }

  PullRequestReviewEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:pull_request_review_event:${document.id}` };
    document._metadata.links.review = { type: 'self', href: `urn:repo:${context.repo}:pull:${payload.pull_request.id}:review:${payload.review.id}` };
    document._metadata.links.pull = { type: 'self', href: `urn:repo:${context.repo}:pull:${payload.pull_request.id}` };
    this._queue('pull_review', payload.pull_request.review_comment_url.replace('{/number}', `/${payload.review.id}`));
    this._queue('pull', payload.pull_request.url);
    return document;
  }

  PullRequestReviewCommentEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:pull_request_review_comment_event:${document.id}` };
    document._metadata.links.comment = { type: 'self', href: `urn:repo:${context.repo}:pull:${payload.pull_request.id}:comment:${payload.comment.id}` };
    document._metadata.links.pull = { type: 'self', href: `urn:repo:${context.repo}:pull:${payload.pull_request.id}` };
    // TODO see if all the various comments can be the same type
    this._queue('pull_comment', payload.comment.url);
    this._queue('pull', payload.pull_request.url);
    return document;
  }

  PushEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:push_event:${document.id}` };
    // TODO figure out what to do with the commits
    return document;
  }

  // ===============  Helpers  ============
  _configFilter(type, target) {
    if (!this.config.orgFilter) {
      return false;
    }
    if (type === 'repo' || type === 'repos' || type === 'org') {
      const parsed = url.parse(target);
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

  _eventHelper(document) {
    // TODO understand if the actor is typically the same as the creator or pusher in the payload
    document._metadata.links.actor = { type: 'self', href: `urn:login:${document.actor.id}` };
    document._metadata.links.repo = { type: 'self', href: `urn:repo:${document.repo.id}` };
    document._metadata.links.org = { type: 'self', href: `urn:org:${document.org.id}` };
    this._queue('login', document.actor.url);
    this._queue('repo', document.repo.url);
    this._queue('org', document.org.url);
    return document.payload;
  }

  _isCollectionRequest(request) {
    return collections.hasOwnProperty(request.type)
  }
}

module.exports = Crawler;