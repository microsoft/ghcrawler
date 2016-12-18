const moment = require('moment');
const parse = require('parse-link-header');
const Request = require('./request');
const Q = require('q');
const qlimit = require('qlimit');
const URL = require('url');
const uuid = require('node-uuid');

class GitHubProcessor {
  constructor(store) {
    this.store = store;
    this.version = 5;
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
    if (result) {
      result._metadata.version = this.version;
      result._metadata.processedAt = moment.utc().toISOString();
    }
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
    this._addRoot(request, 'user', 'user', document.url.replace('/orgs/', '/users/'), `urn:user:${document.id}`);
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

  repo(request) {
    // TODO links to consider
    // * forks  *** not yet
    // * deployments
    // * labels
    // * languages
    // * milestone
    // * hooks
    // * releases
    // * invitations
    // * stargazers
    const document = request.document;
    if (document.private) {
      request.context.repoType = 'private';
    }
    request.addRootSelfLink();
    request.linkSiblings(`urn:user:${document.owner.id}:repos`);
    // Pull requests are issues but with some additional bits.  Here just link to the collection and let the harvesting
    // of issues pull in (pun!) the pull request view of the world.
    request.linkCollection('pull_requests', `${document._metadata.links.self.href}:pull_requests`);

    this._addRoot(request, 'owner', 'user');
    if (document.organization) {
      this._addRoot(request, 'organization', 'org');
    }
    this._addRelation(request, 'teams', 'team');
    this._addRelation(request, 'collaborators', 'user', document.collaborators_url.replace('{/collaborator}', ''));
    // this._addRelation(request, 'outside_collaborators', 'user', document.collaborators_url.replace('{/collaborator}', '?affiliation=outside'));
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

    if (document.comments_url) {
      this._addCollection(request, 'commit_comments', 'commit_comment', document.comments_url, `${document._metadata.links.self.href}:commit_comments`);
    }
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

  commit_comment(request) {
    // TODO links to consider
    // * reactions -- get this by using the following Accept header: application/vnd.github.squirrel-girl-preview
    const document = request.document;
    const context = request.context;
    request.addSelfLink();
    request.linkResource('commit', context.qualifier);
    request.linkSiblings(`${context.qualifier}:commit_comments`);

    this._addRoot(request, 'user', 'user');
    return document;
  }

  pull_request(request) {
    const document = request.document;
    const context = request.context;
    request.addSelfLink();
    request.linkSiblings(`${context.qualifier}:pull_requests`);

    this._addRoot(request, 'user', 'user');
    this._addRoot(request, 'merged_by', 'user');
    this._addRoot(request, 'assignee', 'user');
    this._addRoot(request, 'head', 'repo', document.head.repo.url, `urn:repo:${document.head.repo.id}`);
    this._addRoot(request, 'base', 'repo', document.base.repo.url, `urn:repo:${document.base.repo.id}`);

    if (document._links.review_comments) {
      this._addCollection(request, 'review_comments', 'review_comment', document._links.review_comments.href);
    }
    if (document._links.statuses) {
      const sha = document._links.statuses.href.split('/').slice(-1)[0];
      this._addCollection(request, 'statuses', 'status', document._links.statuses.href, `${context.qualifier}:commit:${sha}:statuses`);
      // this._addResource(request, 'status', 'status', null, statusUrl, `${context.qualifier}:commits:${sha}:status`);
    }

    if (document._links.commits) {
      // TODO.  look at PR commit to see if it should be shared with the repo commit (use a relation if shared)
      this._addCollection(request, 'commits', 'commit', document._links.commits.href);
    }

    // link and queue the related issue.  Getting the issue will bring in the comments for this PR
    if (document._links.issue) {
      // construct and add a link to the relate issue's comments.
      request.linkCollection('issue_comments', `${context.qualifier}:issue:${document.id}:issue_comments`);
      this._addResource(request, 'issue', 'issue', document.id, document._links.issue.href, null, context.qualifier);
    }
    return document;
  }

  review_comment(request) {
    // TODO links to consider
    // * reactions -- get this by using the following Accept header: application/vnd.github.squirrel-girl-preview
    const document = request.document;
    const context = request.context;
    request.addSelfLink();
    request.linkResource('pull_request', context.qualifier);
    request.linkSiblings(`${context.qualifier}:review_comments`);

    this._addRoot(request, 'user', 'user');
    return document;
  }

  issue(request) {
    // TODO links to consider
    // * milestone
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
    if (document.comments_url) {
      this._addCollection(request, 'issue_comments', 'issue_comment', document.comments_url);
    }
    if (document.pull_request) {
      this._addResource(request, 'pull_request', 'pull_request', document.id, document.pull_request.url, null, context.qualifier);
    }
    // Add hrefs for all the labels but do not queue up the labels themselves. Chances of there being more than a
    // page of labels on an issue are near 0.  So we don't need a separate collection of  label pages so inline that here.
    if (document.labels) {
      const labelUrns = document.labels.map(label => `${context.qualifier}:label:${label.id}`);
      request.linkResource('labels', labelUrns);
    }
    return document;
  }

  issue_comment(request) {
    // TODO links to consider
    // * reactions -- get this by using the following Accept header: application/vnd.github.squirrel-girl-preview
    const document = request.document;
    const context = request.context;
    request.addSelfLink();
    request.linkResource('issue', context.qualifier);
    request.linkSiblings(`${context.qualifier}:issue_comments`);

    this._addRoot(request, 'user', 'user');
    return document;
  }

  team(request) {
    const document = request.document;
    request.addSelfLink();
    request.linkSiblings(`urn:org:${document.organization.id}:teams`);

    this._addRoot(request, 'organization', 'org');
    this._addRelation(request, 'members', 'user', document.members_url.replace('{/member}', ''));
    this._addRelation(request, 'repos', 'repo', document.repositories_url);
    return document;
  }

  deployment(request) {
    const document = request.document;
    const context = request.context;
    request.addSelfLink();
    request.linkSiblings(`${context.qualifier}:deployments`);
    request.linkResource('commit', `${context.qualifier}:commit:${document.sha}`);
    this._addRoot(request, 'creator', 'user');
    return document;
  }

  // ===============  Event Processors  ============

  // The events in a repo or org have changed.  Go get the latest events, discover any new
  // ones and queue them for processing.
  update_events(request) {
    const events = request.document.elements;
    // create a promise and track it right away so that this request does not finish
    // processing and exit before the event discovery and queueing completes.
    const processingPromise = this._findNew(events).then(newEvents => {
      // build a new request for each discovered event and include the event itself in the request
      const newRequests = newEvents.map(event => {
        // make sure the URL here is unique. Even though it will not actually be fetched (the content
        // is in the payload), it will need to be unique for the queue tagging/optimization
        const newRequest = new Request(event.type, `${request.url}/${event.id}`);
        newRequest.payload = event;
        return newRequest;
      });
      // queue up the discovered new events for immediate fetching
      request.queue(newRequests, 'immediate');
    });
    request.track(processingPromise);
    return null;
  }

  _findNew(events) {
    const self = this;
    return Q.all(events.map(qlimit(10)(event => {
      const url = event.repo ? `${event.repo.url}/events/${event.id}` : `${event.org.url}/events/${event.id}`;
      return self.store.etag('event', url).then(etag => {
        return etag ? null : event;
      });
    }))).then(events => {
      return events.filter(event => event);
    });
  }

  CommitCommentEvent(request) {
    let [document, repo, payload] = this._addEventBasics(request);
    let url = payload.repository.commits_url.replace('{/sha}', `/${payload.comment.commit_id}`);
    let urn = `urn:repo:${repo}:commit:${payload.comment.commit_id}`;
    this._addResource(request, 'commit', 'commit', null, url, urn, `urn:repo:${repo}`);

    url = payload.repository.comments_url.replace('{/number}', `/${payload.comment.id}`);
    const commentUrn = `${urn}:commit_comment:${payload.comment.id}`;
    this._addResource(request, 'commit_comment', 'commit_comment', null, url, commentUrn, urn);
    return document;
  }

  CreateEvent(request) {
    let [document] = this._addEventBasics(request);
    request.linkResource('repository', document._metadata.links.repo.href);
    return document;
  }

  DeleteEvent(request) {
    // TODO do something for interesting deletions e.g.,  where ref-type === 'repository'
    let [document] = this._addEventBasics(request);
    return document;
  }

  DeploymentEvent(request) {
    let [, repo] = this._addEventBasics(request);
    return this._addEventResource(request, repo, 'deployment');
  }

  DeploymentStatusEvent(request) {
    let [, repo] = this._addEventBasics(request);
    // TODO figure out how to do this more deeply nested structure
    // request.linkResource('deployment_status', `urn:repo:${repo}:deployment:${payload.deployment.id}:status:${payload.deployment_status.id}`);
    return this._addEventResource(request, repo, 'deployment');
  }

  ForkEvent(request) {
    // TODO figure out what else to do
    let [document] = this._addEventBasics(request);
    return document;
  }

  GollumEvent(request) {
    let [document] = this._addEventBasics(request);
    return document;
  }

  IssueCommentEvent(request) {
    let [, repo, payload] = this._addEventBasics(request);
    const qualifier = `urn:repo:${repo}:issue:${payload.issue.id}`;
    this._addEventResource(request, repo, 'comment', 'issue_comment', qualifier);
    return this._addEventResource(request, repo, 'issue');
  }

  IssuesEvent(request) {
    let [document, repo, payload] = this._addEventBasics(request);
    this._addEventResource(request, repo, 'issue');
    if (payload.assignee) {
      this._addEventResource(request, null, 'assignee', 'user');
    }
    if (payload.label) {
      this._addEventResource(request, repo, 'label');
    }
    return document;
  }

  LabelEvent(request) {
    let [document] = this._addEventBasics(request);
    return document;
  }

  MemberEvent(request) {
    this._addEventBasics(request);
    return this._addEventResource(request, null, 'member', 'user');
  }

  MembershipEvent(request) {
    this._addEventBasics(request);
    this._addEventResource(request, null, 'member', 'user');
    return this._addEventResource(request, null, 'team');
  }

  MilestoneEvent(request) {
    // TODO complete implementation and add Milestone handler
    // let [, repo] = this._addEventBasics(request);
    // return this._addEventResource(request, repo, 'milestone');
    let [document] = this._addEventBasics(request);
    return document;  }

  PageBuildEvent(request) {
    // TODO complete implementation and add page_build handler
    // let [document, repo, payload] = this._addEventBasics(request);
    // // This does not fit in to the standard model. In particular, the payload is less structured
    // request.linkResource('page_build', `urn:repo:${repo}:page_builds:${payload.id}`);
    // request.queue('page_build', payload.build.url);
    // return document;
    let [document] = this._addEventBasics(request);
    return document;
  }

  PublicEvent(request) {
    let [document] = this._addEventBasics(request);
    return document;
  }

  PullRequestEvent(request) {
    let [, repo] = this._addEventBasics(request);
    return this._addEventResource(request, repo, 'pull_request');
  }

  PullRequestReviewEvent(request) {
    let [, repo] = this._addEventBasics(request);
    return this._addEventResource(request, repo, 'pull_request');
  }

  PullRequestReviewCommentEvent(request) {
    let [, repo, payload] = this._addEventBasics(request);
    const qualifier = `urn:repo:${repo}:pull_request:${payload.pull_request.id}`;
    this._addEventResource(request, repo, 'comment', 'review_comment', qualifier);
    return this._addEventResource(request, repo, 'pull_request');
  }

  PushEvent(request) {
    let [document] = this._addEventBasics(request);
    // TODO figure out what to do with the commits
    return document;
  }

  ReleaseEvent(request) {
    // TODO complet implementation and add a Release handler
    // let [, repo] = this._addEventBasics(request);
    // return this._addEventResource(request, repo, 'release');
    let [document] = this._addEventBasics(request);
    return document;
  }

  RepositoryEvent(request) {
    this._addEventBasics(request);
    return this._addEventResource(request, null, 'repository', 'repo');
  }

  StatusEvent(request) {
    // TODO complete this by adding a status handler and linking/queuing the status here.
    let [document, , payload] = this._addEventBasics(request);
    request.linkResource('commit', `${document._metadata.links.repo.href}:commit:${payload.sha}`);
    return document;
  }

  TeamEvent(request) {
    let [, , payload] = this._addEventBasics(request, `urn:team:${request.document.payload.team.id}`);
    if (payload.repository) {
      this._addEventResource(request, null, 'repository', 'repo');
    }
    return this._addEventResource(request, null, 'team');
  }

  TeamAddEvent(request) {
    this._addEventBasics(request, `urn:team:${request.document.payload.team.id}`);
    this._addEventResource(request, null, 'repository', 'repo');
    return this._addEventResource(request, null, 'team');
  }

  WatchEvent(request) {
    let [document] = this._addEventBasics(request);
    request.linkResource('repository', document._metadata.links.repo.href);
    return document;
  }

  // ================ HELPERS ========================

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

  _addEventBasics(request, qualifier = null) {
    // TODO handle org event cases (no repo etc)
    const document = request.document;
    const repo = document.repo ? document.repo.id : null;
    qualifier = qualifier || (repo ? `urn:repo:${repo}` : `urn:org:${document.org.id}`);
    request.linkResource('self', `${qualifier}:${request.type}:${document.id}`);
    request.linkSiblings(`${qualifier}:${request.type}s`);

    // TODO understand if the actor is typically the same as the creator or pusher in the payload
    this._addRoot(request, 'actor', 'user');
    if (repo) {
      this._addRoot(request, 'repo', 'repo');
    }
    this._addRoot(request, 'org', 'org');

    return [document, repo, document.payload];
  }

  _addEventResource(request, repo, name, type = name, qualifier = null) {
    const payload = request.document.payload;
    // if the repo is given then use it. Otherwise, assume the type is a root and construct a urn
    qualifier = qualifier || (repo ? `urn:repo:${repo}` : `urn`);
    const target = payload[name];
    if (!target) {
      throw new Error(`Payload@${name} missing in ${request.toString()}`);
    }
    request.linkResource(name, qualifier + `:${type}:${payload[name].id}`);
    const newRequest = new Request(type, payload[name].url, { qualifier: qualifier });
    request.queue(newRequest);
    return request.document;
  }

  _addResource(request, name, type, id, url = null, urn = null, qualifier = null) {
    qualifier = qualifier || request.getQualifier();
    urn = urn || `${qualifier}:${name}:${id}`;
    url = url || request.document[`${name}_url`];

    request.linkResource(name, urn);
    request.queueChild(type, url, qualifier);
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
    // If there is no element then we must have both the url and urn as otherwise we don't know how to compute them
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
    // request.linkCollection('knownPages', `${urn}:pages`);
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

module.exports = GitHubProcessor;