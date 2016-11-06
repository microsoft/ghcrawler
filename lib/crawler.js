const moment = require('moment');
const Q = require('q');

class Crawler {
  constructor(queue, store, requestor, logger) {
    this.seen = {};
    this.queue = queue;
    this.store = store;
    this.requestor = requestor;
    this.logger = logger;
  }

  start() {
    return this.queue.pop()
      .then(this._filterSeen.bind(this))
      .then(this._fetch.bind(this))
      .then(this._convertToDocument.bind(this))
      .then(this._processDocument.bind(this))
      .then(this._storeDocument.bind(this))
      .then(this._deleteFromQueue.bind(this))
      .then(this._markSeen.bind(this))
      .then(request => {
        this.logger.log('info', `${request.skip ? (request.error ? 'Failed' : 'Skipped') : 'Processed'} ${request.url} [${request.type}]`);
        return request;
      })
      .then(this._startNext.bind(this));
  }

  _startNext() {
    setTimeout(this.start.bind(this), 0);
  }

  _filterSeen(request) {
    if (this.seen[request.url]) {
      request.skip = true;
    }
    return Q.resolve(request);
  }

  _fetch(request) {
    if (request.skip) {
      return Q.resolve(request);
    }

    return this.requestor.getAll(request.url)
      .then(githubResponse => {
        request.response = githubResponse;
        return request;
      })
      .catch(error => {
        // TODO retryable vs non-retryable
        request.skip = true;
        request.error = error;
        return request;
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
          document = handler.call(this, reqeust.response, request.context);
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

  _processTeam(document, context) {
    document._metadata.links.self = { type: 'self', href: `urn:org:${document.organization.id}:team:${document.id}` };
    document._metadata.links.org = { type: 'self', href: `urn:org:${document.organization.id}` };
    document._metadata.links.login = { type: 'self', href: `urn:login:${document.organization.id}` };
    document._metadata.links.siblings = { type: 'siblings', href: `urn:org:${document.organization.id}:teams` };
    this.queue.push('team_members', document.members_url);
    this.queue.push('team_repos', document.repositories_url);
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
    this.queue.push('comment', payload.comment.url);
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
    this.queue.push('deployment', payload.deployment.url);
    return document;
  }

  DeploymentStatusEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:deployment_status_event:${document.id}` };
    document._metadata.links.deployment_status = { type: 'self', href: `urn:repo:${context.repo}:deployment:${payload.deployment.id}:status:${payload.deployment_status.id}` };
    document._metadata.links.deployment = { type: 'self', href: `urn:repo:${context.repo}:deployment:${payload.deployment.id}` };
    this.queue.push('deployment', payload.deployment.url);
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
    this.queue.push('comment', payload.comment.url);
    this.queue.push('issue', payload.issue.url);
    return document;
  }

  IssuesEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:issued_event:${document.id}` };
    document._metadata.links.issue = { type: 'self', href: `urn:repo:${context.repo}:issue:${payload.issue.id}` };
    this.queue.push('issue', payload.issue.url);
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
    this.queue.push('login', payload.member.url);
    return document;
  }

  MembershipEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:team:${payload.team.id}:membership_event:${document.id}` };
    document._metadata.links.member = { type: 'self', href: `urn:login:${payload.member.id}` };
    document._metadata.links.team = { type: 'self', href: `urn:team:${payload.team.id}` };
    document._metadata.links.org = { type: 'self', href: `urn:org:${payload.organization.id}` };
    this.queue.push('login', payload.member.url);
    this.queue.push('org', payload.organization.url);
    this.queue.push('team', payload.team.url);
    return document;
  }

  MilestoneEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:milestone_event:${document.id}` };
    document._metadata.links.milestone = { type: 'self', href: `urn:repo:${context.repo}:milestone:${payload.milestone.id}` };
    this.queue.push('milestone', payload.milestone.url);
    return document;
  }

  PageBuildEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:page_build_event:${document.id}` };
    document._metadata.links.page_build = { type: 'self', href: `urn:repo:${context.repo}:page_builds:${payload.id}` };
    this.queue.push('page_build', payload.build.url);
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
    this.queue.push('pull', payload.pull_request.url);
    return document;
  }

  PullRequestReviewEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:pull_request_review_event:${document.id}` };
    document._metadata.links.review = { type: 'self', href: `urn:repo:${context.repo}:pull:${payload.pull_request.id}:review:${payload.review.id}` };
    document._metadata.links.pull = { type: 'self', href: `urn:repo:${context.repo}:pull:${payload.pull_request.id}` };
    this.queue.push('pull_review', payload.pull_request.review_comment_url.replace('{/number}', `/${payload.review.id}`));
    this.queue.push('pull', payload.pull_request.url);
    return document;
  }

  PullRequestReviewCommentEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:pull_request_review_comment_event:${document.id}` };
    document._metadata.links.comment = { type: 'self', href: `urn:repo:${context.repo}:pull:${payload.pull_request.id}:comment:${payload.comment.id}` };
    document._metadata.links.pull = { type: 'self', href: `urn:repo:${context.repo}:pull:${payload.pull_request.id}` };
    // TODO see if all the various comments can be the same type
    this.queue.push('pull_comment', payload.comment.url);
    this.queue.push('pull', payload.pull_request.url);
    return document;
  }

  PushEvent(document, context) {
    const payload = _eventHelper(document);
    document._metadata.links.self = { type: 'self', href: `urn:repo:${context.repo}:push_event:${document.id}` };
    // TODO figure out what to do with the commits
    return document;
  }


  _eventHelper(document) {
    // TODO understand if the actor is typically the same as the creator or pusher in the payload
    document._metadata.links.actor = { type: 'self', href: `urn:login:${document.actor.id}` };
    document._metadata.links.repo = { type: 'self', href: `urn:repo:${document.repo.id}` };
    document._metadata.links.org = { type: 'self', href: `urn:org:${document.org.id}` };
    this.queue.push('login', document.actor.url);
    this.queue.push('repo', document.repo.url);
    this.queue.push('org', document.org.url);
    return document.payload;
  }
}

module.exports = Crawler;