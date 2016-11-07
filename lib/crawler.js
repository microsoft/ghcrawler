const moment = require('moment');
const Q = require('q');
const url = require('url');

const documentCollections = new Set([
  'orgs', 'repos', 'issues', 'issue_comments', 'commits', 'teams'
]);

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
      .then(this._filter.bind(this))
      .then(this._fetch.bind(this))
      .then(this._convertToDocument.bind(this))
      .then(this._processDocument.bind(this))
      .then(this._storeDocument.bind(this))
      .then(this._deleteFromQueue.bind(this))
      .then(this._markSeen.bind(this))
      .then(this._logOutcome.bind(this))
      .then(this._startNext.bind(this));
  }

  _startNext() {
    setTimeout(this.start.bind(this), 0);
  }

  _filter(request) {
    if (this.seen[request.url]) {
      this._markSkip(request, 'Seen');
    } else if (this._configFilter(request.type, request.url)) {
      this._markSkip(request, 'Filtered');
    }
    return Q.resolve(request);
  }

  _fetch(request) {
    if (request.skip) {
      return Q.resolve(request);
    }

    const getCollection = documentCollections.has(request.type);
    const getFunction = getCollection ? this.requestor.getAll : this.requestor.get;
    return getFunction.call(this.requestor, request.url)
      .then(githubResponse => {
        request.response = getCollection ? githubResponse : githubResponse.body;
        return request;
      })
      .catch(error => {
        // TODO retryable vs non-retryable
        return this._markSkip(request, 'Error', error.message);
      });
  }

  _convertToDocument(request) {
    if (request.skip) {
      return Q.resolve(request);
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
    if (request.skip) {
      return Q.resolve(request);
    }
    let document = null;
    switch (request.type) {
      case 'orgs': {
        document = this._processCollection(request.response, 'org', request.context);
        break;
      }
      case 'org': {
        document = this._processOrg(request.response, request.context);
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
      case 'teams': {
        document = this._processCollection(request.response, 'team', request.context);
        break;
      }
      case 'team': {
        document = this._processTeam(request.response, request.context);
        break;
      }
      case 'team_members': {
        document = this._processTeamMembers(request.response, request.context);
        break;
      }
      case 'team_repos': {
        document = this._processTeamRepos(request.response, request.context);
        break;
      }
      default: {
        const handler = this[request.type];
        if (handler && typeof handler === 'function') {
          document = handler.call(this, request.response, request.context);
        }
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

  _markSeen(request) {
    // TODO retryable vs non-retryable and re-queue
    this.seen[request.url] = true;
    return Q.resolve(request);
  }

  _logOutcome(request) {
    const outcome = request.outcome ? request.outcome : 'Processed';
    const message = request.message;
    this.logger.log('info', `${outcome} ${request.type} [${request.url}] ${message || ''}`);
    return request;
  }

  _processCollection(document, type, context) {
    document.forEach(item => {
      this._queue(type, item.url, context);
    });
    return null;
  }

  _processOrg(document) {
    document._metadata.links.self = { type: 'self', href: `urn:org:${document.id}` };
    document._metadata.links.repos = { type: 'siblings', href: `urn:org:${document.id}:repos` };
    document._metadata.links.siblings = { type: 'siblings', href: 'urn:org' };
    this._queue('repos', document.repos_url);
    return document;
  }

  _processRepo(document) {
    document._metadata.links.self = { type: 'self', href: `urn:repo:${document.id}` };
    document._metadata.links.owner = { type: 'self', href: `urn:login:${document.owner.id}` };
    document._metadata.links.parent = { type: 'self', href: `urn:login:${document.owner.id}` };
    document._metadata.links.siblings = { type: 'siblings', href: `urn:login:${document.owner.id}:repos` };
    this._queue('login', document.owner.url);
    this._queue('issues', document.issues_url.replace('{/number}', ''), { repo: document.id });
    this._queue('commits', document.commits_url.replace('{/sha}', ''), { repo: document.id });
    return document;
  }

  _processCommit(document, context) {
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

  _processLogin(document) {
    document._metadata.links.self = { type: 'self', href: `urn:login:${document.id}` };
    return document;
  }

  _processIssue(document, context) {
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

  _processIssueComment(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:issue_comment:${document.id}` };
    document._metadata.links.user = { type: 'self', href: `urn:login:${document.user.id}` };
    document._metadata.links.siblings = { type: 'siblings', href: `urn:repo:${context.repo}:issue:${context.issue}:comments` };
    this._queue('login', document.user.url);
    return document;
  }

  _processTeam(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:org:${document.organization.id}:team:${document.id}` };
    document._metadata.links.org = { type: 'self', href: `urn:org:${document.organization.id}` };
    document._metadata.links.login = { type: 'self', href: `urn:login:${document.organization.id}` };
    document._metadata.links.siblings = { type: 'siblings', href: `urn:org:${document.organization.id}:teams` };
    this._queue('team_members', document.members_url);
    this._queue('team_repos', document.repositories_url);
    return document;
  }

  _processTeamMembers(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:org:${document.organization.id}:team:${document.id}:members` };
    return document;
  }

  _processTeamRepos(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:org:${document.organization.id}:team:${document.id}:repos` };
    return document;
  }

  // Event handlers
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

  _configFilter(type, target) {
    if (!this.config.orgFilter) {
      return false;
    }
    if (type === 'repo' || type === 'org') {
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
}

module.exports = Crawler;