const parse = require('parse-link-header');
const queryString = require('query-string');
const Request = require('./request');
const URL = require('url');

// TODO open questions
// * relationship between login, user and org
// * convention for the pluralization of siblings links.  Some places it is "issues", others it is "org" and "user"
// * should we track followers on users, repos...  Perhaps if there are more than a certain threshold of people involved?

class Processor {
  constructor() {
    this.version = 3;
  }

  process(request) {
    const handler = this._getHandler(request);
    if (!handler) {
      request.markSkip('Warning', `No handler found for request type: ${request.type}`);
      return request.document;
    }

    if (request.isReprocessing()) {
      if (request.document._metadata.version === this.version) {
        request.markSkip('Up to date', `Already at version: ${this.version}`);
        return request.document;
      }
      if (request.document._metadata.version > this.version) {
        request.markSkip('Superceded', `Current version: ${request.document._metadata.version} > requested version: ${this.version}`);
        return request.document;
      }
    }
    const result = handler.call(this, request);
    result._metadata.version = this.version;
    return result;
  }

  collection(request) {
    // if there are additional pages, queue them up to be processed.  Note that these go
    // on the high soon queue so they are loaded before they change much.
    const linkHeader = (request.response && request.response.headers) ? request.response.headers.link : null;
    if (linkHeader) {
      const links = parse(linkHeader);
      const requests = [];
      for (let i = 2; i <= links.last.page; i++) {
        const url = request.url + `?page=${i}&per_page=100`;
        const newRequest = new Request(request.type, url);
        // Carry through this request's transitivity, fetch and qualifier for subsequent pages
        newRequest.fetch = request.fetch;
        newRequest.transitivity = request.transitivity;
        newRequest.context = { qualifier: request.context.qualifier };
        requests.push(newRequest);
      }
      // TODO this is a bit reachy.  need a better way to efficiently queue up
      // requests that we know are good.
      request.track(request.crawler.queues.push(requests, 'soon'));
    }

    // Process the rest of the request as a page.
    return this.page(1, request);
  }

  page(page, request) {
    const document = request.document;
    const qualifier = request.context.qualifier;
    request.linkSelf('self', `${qualifier}:${request.type}:page:${page}`);
    // If the context defines a relation, create a link in this page.  This can be used to
    // track that a page defines a relation between an entity and a set of entities.  For example,
    // a repo and its teams.  The teams are not exclusively "part of" the repo, they are just related.
    if (request.context.relation) {
      const relation = request.context.relation;
      const handler = this[relation];
      if (!handler) {
        request.markSkip('Warning', `No handler found for relation type: ${relation}`);
        return document;
      }
      handler.call(this, request);
    }
    const elementType = request.getCollectionType();
    document.elements.forEach(item => {
      request.queueCollectionElement(elementType, item.url, qualifier);
    });
    return document;
  }

  _getHandler(request, type = request.type) {
    const parsed = URL.parse(request.url, true);
    const page = parsed.query.page;
    if (page) {
      return this.page.bind(this, page);
    }
    const collectionType = request.getCollectionType();
    if (collectionType) {
      return this.collection;
    }
    return (this[type]);
  }

  org(request) {
    const document = request.document;
    request.addRootSelfLink();
    request.linkSiblings('repos', `urn:login:${document.id}:repos`);
    request.linkSiblings('siblings', 'urn:org');
    request.queueRoots('repos', document.repos_url);
    request.queueRoots('members', document.members_url.replace('{/member}', ''));
    return document;
  }

  user(request) {
    // TODO links to consider
    // * folowers
    // * following
    const document = request.document;
    request.addRootSelfLink();
    request.linkSiblings('repos', `urn:login:${document.id}:repos`);
    request.linkSiblings('siblings', 'urn:user');
    request.queueRoots('repos', document.repos_url);
    return document;
  }

  repo(request) {
    // TODO links to consider
    // * forks
    // * deployments
    // * labels
    const document = request.document;
    request.addRootSelfLink();
    request.linkSelf('owner', `urn:login:${document.owner.id}`);
    request.linkSelf('parent', `urn:login:${document.owner.id}`);
    request.linkSiblings('siblings', `urn:login:${document.owner.id}:repos`);
    request.queueRoot('login', document.owner.url);
    request.queueRoots('teams', document.teams_url, { relation: 'repo_teams_relation' });
    request.queueRoots('collaborators', document.collaborators_url.replace('{/collaborator}', ''), { relation: 'repo_collaborators_relation' });
    request.queueRoots('contributors', document.contributors_url, { relation: 'repo_contributors_relation' });
    request.queueRoots('subscribers', document.subscribers_url, { relation: 'repo_subscribers_relation' });
    request.queueChildren('issues', document.issues_url.replace('{/number}', ''), { repo: document.id });
    request.queueChildren('commits', document.commits_url.replace('{/sha}', ''), { repo: document.id });
    return document;
  }

  repo_teams_relation(request) {
    return this._processRelation(request, 'teams', 'repo', 'team');
  }

  repo_collaborators_relation(request) {
    return this._processRelation(request, 'collaborators', 'repo', 'user');
  }

  repo_contributors_relation(request) {
    return this._processRelation(request, 'contributors', 'repo', 'user');
  }

  repo_subscribers_relation(request) {
    return this._processRelation(request, 'contributors', 'repo', 'user');
  }

  _processRelation(request, name, originType, targetType) {
    const document = request.document;
    request.linkSelf(originType, `${request.context.qualifier}`);
    const urns = document.elements.map(element => `urn:${targetType}:${element.id}`);
    request.linkSelf(name, urns);
    return document;
  }

  commit(request) {
    const document = request.document;
    const context = request.context;
    request.addSelfLink('sha');

    request.linkSelf('repo', `urn:repo:${context.repo}`);
    request.linkSiblings('siblings', `urn:repo:${context.repo}:commits`);
    // TODO not sure what the following line does
    // document._metadata.links.parent = document._metadata.links.parent;
    if (document.author) {
      request.linkSelf('author', `urn:login:${document.author.id}`);
      request.queueRoot('user', document.author.url);
    }
    if (document.committer) {
      request.linkSelf('committer', `urn:login:${document.committer.id}`);
      request.queueRoot('user', document.committer.url);
    }
    if (document.files) {
      document.files.forEach(file => {
        delete file.patch;
      });
    }
    return document;
  }

  login(request) {
    // TODO sort out relationship of login to user and org.
    const document = request.document;
    request.addRootSelfLink();
    request.linkSelf('self', `urn:login:${document.id}`);
    // TODO should we do repos here and in the user/org?
    request.linkSiblings('repos', `urn:login:${document.id}:repos`);
    request.linkSiblings('siblings', 'urn:login');
    if (document.type === 'Organization') {
      request.queueRoot('org', `https://api.github.com/orgs/${document.login}`);
    } else if (document.type === 'User') {
      request.queueRoot('user', `https://api.github.com/users/${document.login}`);
    }
    return document;
  }

  issue(request) {
    // TODO links to consider
    // * milestone
    // * pull request -- all pull requests are issues. Should we queue it up twice?  add a link?
    // * events -- issue events
    // * labels
    // * reactions -- get this by using the following Accept header: application/vnd.github.squirrel-girl-preview
    const document = request.document;
    const context = request.context;
    request.addSelfLink();
    request.linkSelf('assignees', document.assignees.map(assignee => { return `urn:login:${assignee.id}`; }));
    request.linkSelf('repo', `urn:repo:${context.repo}`);
    request.linkSelf('parent', `urn:repo:${context.repo}`);
    request.linkSelf('user', `urn:login:${document.user.id}`);
    request.linkSiblings('siblings', `urn:repo:${context.repo}:issues`);
    request.queueRoot('login', document.user.url);
    request.queueRoot('repo', document.repository_url);
    if (document.assignee) {
      request.linkSelf('assignee', `urn:login:${document.assignee.id}`);
      request.queueRoot('login', document.assignee.url);
    }
    if (document.closed_by) {
      request.linkSelf('closed_by', `urn:login:${document.closed_by.id}`);
      request.queueRoot('login', document.closed_by.url);
    }

    request.queueChildren('issue_comments', document.comments_url, { issue: document.id, repo: context.repo });
    return document;
  }

  issue_comment(request) {
    // TODO links to consider
    // * reactions -- get this by using the following Accept header: application/vnd.github.squirrel-girl-preview
    const document = request.document;
    const context = request.context;
    request.addSelfLink();
    request.linkSelf('user', `urn:login:${document.user.id}`);
    request.linkSiblings('siblings', `urn:repo:${context.repo}:issue:${context.issue}:comments`);
    request.queueRoot('login', document.user.url);
    return document;
  }

  team(request) {
    const document = request.document;
    request.addSelfLink();
    request.linkSelf('org', `urn:org:${document.organization.id}`);
    request.linkSelf('login', `urn:login:${document.organization.id}`);
    request.linkSiblings('siblings', `urn:org:${document.organization.id}:teams`);
    request.queueRoots('team_members', document.members_url, { relation: 'team_members_relation' });
    request.queueRoots('team_repos', document.repositories_url, { relation: 'team_repos_relation' });
    return document;
  }

  team_members_relation(request) {
    return this._processRelation(request, 'members', 'team', 'user');
  }

  team_repos_relation(request) {
    return this._processRelation(request, 'repos', 'team', 'repo');
  }

  // ===============  Event Processors  ============
  CommitCommentEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(request);
    request.linkSelf('comment', `urn:repo:${context.repo}:comment:${payload.comment.id}`);
    // TODO siblings?
    request.queue('comment', payload.comment.url);
    return document;
  }

  CreateEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    return document;
  }

  DeleteEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    // TODO do something for interesting deletions e.g.,  where ref-type === 'repository'
    return document;
  }

  DeploymentEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkSelf('deployment', `urn:repo:${context.repo}:deployment:${payload.deployment.id}`);
    request.queue('deployment', payload.deployment.url);
    return document;
  }

  DeploymentStatusEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkSelf('deployment_status', `urn:repo:${context.repo}:deployment:${payload.deployment.id}:status:${payload.deployment_status.id}`);
    request.linkSelf('deployment', `urn:repo:${context.repo}:deployment:${payload.deployment.id}`);
    request.queue('deployment', payload.deployment.url);
    return document;
  }

  ForkEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    // TODO figure out what else to do
    return document;
  }

  GollumEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    return document;
  }

  IssueCommentEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkSelf('issue', `urn:repo:${context.repo}:issue:${payload.issue.id}`);
    request.linkSelf('comment', `urn:repo:${context.repo}:comment:${payload.comment.id}`);
    request.queue('comment', payload.comment.url);
    request.queue('issue', payload.issue.url);
    return document;
  }

  IssuesEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkSelf('issue', `urn:repo:${context.repo}:issue:${payload.issue.id}`);
    request.queue('issue', payload.issue.url);
    return document;
  }

  LabelEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    return document;
  }

  MemberEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkSelf('member', `urn:login:${payload.member.id}`);
    request.queueRoot('login', payload.member.url);
    return document;
  }

  MembershipEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkSelf('self', `urn:team:${payload.team.id}:membership_event:${document.id}`);
    request.linkSelf('member', `urn:login:${payload.member.id}`);
    request.linkSelf('team', `urn:team:${payload.team.id}`);
    request.linkSelf('org', `urn:org:${payload.organization.id}`);
    request.queueRoot('login', payload.member.url);
    request.queueRoot('org', payload.organization.url);
    request.queue('team', payload.team.url);
    return document;
  }

  MilestoneEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkSelf('milestone', `urn:repo:${context.repo}:milestone:${payload.milestone.id}`);
    request.queue('milestone', payload.milestone.url);
    return document;
  }

  PageBuildEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkSelf('page_build', `urn:repo:${context.repo}:page_builds:${payload.id}`);
    request.queue('page_build', payload.build.url);
    return document;
  }

  PublicEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    return document;
  }

  PullRequestEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkSelf('pull', `urn:repo:${context.repo}:pull:${payload.pull_request.id}`);
    request.queue('pull', payload.pull_request.url);
    return document;
  }

  PullRequestReviewEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkSelf('review', `urn:repo:${context.repo}:pull:${payload.pull_request.id}:review:${payload.review.id}`);
    request.linkSelf('pull', `urn:repo:${context.repo}:pull:${payload.pull_request.id}`);
    request.queue('pull_review', payload.pull_request.review_comment_url.replace('{/number}', `/${payload.review.id}`));
    request.queue('pull', payload.pull_request.url);
    return document;
  }

  PullRequestReviewCommentEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkSelf('comment', `urn:repo:${context.repo}:pull:${payload.pull_request.id}:comment:${payload.comment.id}`);
    request.linkSelf('pull', `urn:repo:${context.repo}:pull:${payload.pull_request.id}`);
    // TODO see if all the various comments can be the same type
    request.queue('pull_comment', payload.comment.url);
    request.queue('pull', payload.pull_request.url);
    return document;
  }

  PushEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    // TODO figure out what to do with the commits
    return document;
  }
}

module.exports = Processor;