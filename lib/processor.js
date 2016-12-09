const moment = require('moment');
const parse = require('parse-link-header');
const Request = require('./request');
const URL = require('url');
const uuid = require('node-uuid');

class Processor {
  constructor() {
    this.version = 4;
  }

  process(request) {
    const handler = this._getHandler(request);
    if (!handler) {
      request.markSkip('Skip', `No handler found for request type: ${request.type}`);
      return request.document;
    }

    if (!request.policy.shouldProcess(request, this.version)) {
      request.markSkip('Excluded', `Traversal policy excluded this resource`);
      return request.document;
    }

    const result = handler.call(this, request);
    result._metadata.version = this.version;
    result._metadata.processedAt = moment.utc().toISOString();
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
        const newRequest = new Request(request.type, url, request.context);
        newRequest.policy = request.policy;
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
    request.linkResource('self', `${qualifier}:${request.type}:page:${page}`);
    // If the context defines a relation, create a link in this page.  This can be used to
    // track that a page defines a relation between an entity and a set of entities.  For example,
    // a repo and its teams.  The teams are not exclusively "part of" the repo, they are just related.
    const relation = request.context.relation;
    let elementType = request.context.elementType;
    if (relation) {
      this._processRelation(request, relation);
      elementType = relation.type;
    }
    document.elements.forEach(item => {
      request.queueCollectionElement(elementType, item.url, qualifier);
    });
    return document;
  }

  org(request) {
    const document = request.document;
    request.addRootSelfLink();
    request.linkSiblings('urn:orgs');
    // TODO look at new API for membership differentiation
    // * hooks
    //
    this._addRoot(request, 'user', 'user', document.url.replace('/orgs/', '/users/'));
    this._addCollection(request, 'repos', "repo", null, `urn:user:${document.id}:repos`);
    this._addRelation(request, 'members', "user", document.members_url.replace('{/member}', ''));

    return document;
  }

  user(request) {
    // TODO links to consider
    // * followers
    // * following
    // * starred
    // * subscriptions
    // * orgs???
    const document = request.document;
    request.addRootSelfLink();
    request.linkSiblings('urn:users');

    this._addCollection(request, 'repos', "repo");
    return document;
  }

// reactions
// commit comment, issue, issue comment, pull request review comment



// pull request
// * issue
// * repo
// * review_comments
// * comments
// * statuses
// * merged by
// * < merge this with issue stuff>


  repo(request) {
    // TODO links to consider
    // * forks  *** not yet
    // * collaborators * 2 for all and outside
    // * deployments
    // * labels
    // * languages
    // * milestone
    // * pull request
    // * hooks
    // * releases
    // * invitations
    // * stargazers
    const document = request.document;
    request.addRootSelfLink();
    request.linkSiblings(`urn:user:${document.owner.id}:repos`);

    this._addRoot(request, 'owner', 'user');
    this._addRelation(request, 'teams', 'team');
    this._addRelation(request, 'collaborators', 'user', document.collaborators_url.replace('{/collaborator}', ''));
    this._addRelation(request, 'contributors', 'user');
    this._addRelation(request, 'subscribers', 'user');
    this._addCollection(request, 'issues', 'issue', document.issues_url.replace('{/number}', ''));
    this._addCollection(request, 'commits', 'commit', document.commits_url.replace('{/sha}', ''));
    return document;
  }

  commit(request) {
    const document = request.document;
    const context = request.context;
    request.addSelfLink('sha');
    request.linkSiblings(`${context.qualifier}:commits`);

    // * comments
    this._addRoot(request, 'repo', 'repo', document.url.replace(/\/commits\/.*/, ''), `${context.qualifier}`);
    // TODO some commits have author and committer properties, others have email info in a "commit" property
    // For the former, this code works.  For the latter, consider queuing an email lookup and storing a
    // email key here for the author/committer.
    this._addRoot(request, 'author', 'user');
    this._addRoot(request, 'committer', 'user');

    if (document.files) {
      document.files.forEach(file => {
        delete file.patch;
      });
    }
    return document;
  }

  issue(request) {
    // TODO links to consider
    // * milestone
    // * pull request -- all pull requests are issues. Should we queue it up twice?  add a link?
    // * labels
    // * reactions -- get this by using the following Accept header: application/vnd.github.squirrel-girl-preview
    const document = request.document;
    const context = request.context;
    request.addSelfLink();
    request.linkSiblings(`${context.qualifier}:issues`);

    const assignees = document.assignees.map(assignee => { return `urn:user:${assignee.id}`; });
    if (assignees.length > 0) {
      request.linkResource('assignees', assignees);
    }

    this._addRoot(request, 'user', 'user');
    this._addRoot(request, 'repo', 'repo', document.repository_url, context.qualifier);
    this._addRoot(request, 'assignee', 'user');
    this._addRoot(request, 'closed_by', 'user');
    if (document.comments) {
      this._addCollection(request, 'comments', 'issue_comment');
    }
    return document;
  }

  issue_comment(request) {
    // TODO links to consider
    // * reactions -- get this by using the following Accept header: application/vnd.github.squirrel-girl-preview
    const document = request.document;
    const context = request.context;
    request.addSelfLink();
    // TODO add link to the issue (i.e., the qualifier)
    request.linkSiblings(`${context.qualifier}:comments`);

    this._addRoot(request, 'user', 'user');
    return document;
  }

  team(request) {
    const document = request.document;
    request.addSelfLink();
    request.linkResource('org', `urn:org:${document.organization.id}`);
    request.linkSiblings(`urn:org:${document.organization.id}:teams`);

    // TODO queue the org??
    this._addRelation(request, 'members', 'user', document.members_url.replace('{/member}', ''));
    this._addRelation(request, 'repos', 'repo', document.repositories_url);
    return document;
  }

  // ===============  Event Processors  ============
  CommitCommentEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(request);
    request.linkResource('comment', `urn:repo:${context.repo}:comment:${payload.comment.id}`);
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
    request.linkResource('deployment', `urn:repo:${context.repo}:deployment:${payload.deployment.id}`);
    request.queue('deployment', payload.deployment.url);
    return document;
  }

  DeploymentStatusEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkResource('deployment_status', `urn:repo:${context.repo}:deployment:${payload.deployment.id}:status:${payload.deployment_status.id}`);
    request.linkResource('deployment', `urn:repo:${context.repo}:deployment:${payload.deployment.id}`);
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
    request.linkResource('issue', `urn:repo:${context.repo}:issue:${payload.issue.id}`);
    request.linkResource('comment', `urn:repo:${context.repo}:comment:${payload.comment.id}`);
    request.queue('comment', payload.comment.url);
    request.queue('issue', payload.issue.url);
    return document;
  }

  IssuesEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkResource('issue', `urn:repo:${context.repo}:issue:${payload.issue.id}`);
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
    request.linkResource('member', `urn:user:${payload.member.id}`);
    request.queueRoot('user', payload.member.url);
    return document;
  }

  MembershipEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkResource('self', `urn:team:${payload.team.id}:membership_event:${document.id}`);
    request.linkResource('member', `urn:user:${payload.member.id}`);
    request.linkResource('team', `urn:team:${payload.team.id}`);
    request.linkResource('org', `urn:org:${payload.organization.id}`);
    request.queueRoot('user', payload.member.url);
    request.queueRoot('org', payload.organization.url);
    request.queue('team', payload.team.url);
    return document;
  }

  MilestoneEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkResource('milestone', `urn:repo:${context.repo}:milestone:${payload.milestone.id}`);
    request.queue('milestone', payload.milestone.url);
    return document;
  }

  PageBuildEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkResource('page_build', `urn:repo:${context.repo}:page_builds:${payload.id}`);
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
    request.linkResource('pull', `urn:repo:${context.repo}:pull:${payload.pull_request.id}`);
    request.queue('pull', payload.pull_request.url);
    return document;
  }

  PullRequestReviewEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkResource('review', `urn:repo:${context.repo}:pull:${payload.pull_request.id}:review:${payload.review.id}`);
    request.linkResource('pull', `urn:repo:${context.repo}:pull:${payload.pull_request.id}`);
    request.queue('pull_review', payload.pull_request.review_comment_url.replace('{/number}', `/${payload.review.id}`));
    request.queue('pull', payload.pull_request.url);
    return document;
  }

  PullRequestReviewCommentEvent(request) {
    const document = request.document;
    const context = request.context;
    const payload = request.eventHelper(document);
    request.linkResource('comment', `urn:repo:${context.repo}:pull:${payload.pull_request.id}:comment:${payload.comment.id}`);
    request.linkResource('pull', `urn:repo:${context.repo}:pull:${payload.pull_request.id}`);
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

  _getHandler(request, type = request.type) {
    const parsed = URL.parse(request.url, true);
    const page = parsed.query.page;
    if (page) {
      return this.page.bind(this, page);
    }
    if (request.isCollectionType()) {
      return this.collection;
    }
    return (this[type]);
  }

  _addCollection(request, name, type, url = null, urn = null) {
    const qualifier = request.getQualifier();
    urn = urn || `${qualifier}:${name}`;
    url = url || request.document[`${name}_url`];

    request.linkCollection(name, urn);
    if (request.isRootType(type)) {
      return request.queueRoots(name, url, { elementType: type });
    }
    const newContext = { qualifier: qualifier, elementType: type };
    request.queueChildren(name, url, newContext);
  }

  _addRoot(request, name, type, url = null, urn = null) {
    const element = request.document[name];
    if (!element && !(urn && url)) {
      return;
    }

    urn = urn || `urn:${type}:${element.id}`;
    url = url || element.url;
    request.linkResource(name, urn);
    request.queueRoot(type, url);
  }
  /**
   * Relate this document to a collection of other documents of the given type.  For example,
   * a repo to its collaborators which are users.
   *
   * This creates a relationship between the current document being processed and the named
   * target resource of the given type. This results in a siblings link with the given name
   * and urn being added to this document and a relation request queued for the given url.
   * The document produced by processing that url will have matching siblings links (called 'siblings')
   */
  _addRelation(request, name, type, url = null, urn = null) {
    urn = urn || `${request.getQualifier()}:${name}`;
    url = url || request.document[`${name}_url`];

    // For relations we want to have a guid that uniquely identifies all of the pages for this
    // particular state of the relation.  Add the guid here for the relation link and brand
    // each page with it in its siblings link to get a coherent state.
    const guid = uuid.v4();
    request.linkRelation(name, `${urn}:pages:${guid}`);
    // Also add an 'knownPages' link to help deal with page clean up.
    request.linkCollection('knownPages', `${urn}:pages`);
    request.queueRoots(name, url, { relation: { origin: request.type, name: name, type: type, guid: guid } });
  }

  /**
   * Process a page resource for a relation.  Add links identifying this page as part of a
   * relation with the given info and enumerate links for the resources referenced from this page.
   * Note that currently relations can only point to root resources.
   */
  _processRelation(request, relation) {
    const document = request.document;
    const qualifier = request.context.qualifier;
    request.linkResource('origin', `${qualifier}`);
    request.linkResource(relation.origin, `${qualifier}`);
    request.linkSiblings(`${qualifier}:${relation.name}:pages`);
    request.linkCollection('unique', `${qualifier}:${relation.name}:pages:${relation.guid}`);
    const urns = document.elements.map(element => `urn:${relation.type}:${element.id}`);
    request.linkResource('resources', urns);
    return document;
  }
}

module.exports = Processor;