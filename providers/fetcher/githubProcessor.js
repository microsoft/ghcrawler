// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const extend = require('extend');
const moment = require('moment');
const parse = require('parse-link-header');
const Q = require('q');
const qlimit = require('qlimit');
const Request = require('../../lib/request');
const TraversalPolicy = require('../../lib/traversalPolicy');
const URL = require('url');
const uuid = require('node-uuid');
const VisitorMap = require('../../lib/visitorMap');

class GitHubProcessor {
  constructor(store) {
    this.store = store;
    this.version = 13;
  }

  process(request) {
    const handler = this._getHandler(request);
    if (!handler) {
      request.markSkip('Skip', `No handler found for request type: ${request.type}`);
      return request.document;
    }

    const oldVersion = request.document._metadata.version;
    if (request.policy.shouldProcess(request, this.version)) {
      request.processMode = 'process';
    } else {
      // We are not going to process but may still need to traverse the doc to get to referenced docs that
      // do need processing. If so, mark the request for no saving (already have good content) and carry on.
      // Otherwise, skip the doc altogether.
      if (request.policy.shouldTraverse(request)) {
        request.processMode = 'traverse';
        request.markNoSave();
      } else {
        request.markSkip('Excluded', `Traversal policy excluded this resource`);
        return request.document;
      }
    }

    const result = handler.call(this, request);
    if (result) {
      result._metadata.version = this.version;
      result._metadata.processedAt = moment.utc().toISOString();
      if (result._metadata.version !== oldVersion) {
        request.markSave();
      }
    }
    if (!request.shouldSave()) {
      request.outcome = request.outcome || 'Traversed';
    }
    return result;
  }

  collection(request) {
    // if there are additional pages, queue them up to be processed.  Note that these go
    // on the 'soon' queue so they are loaded before they change much.
    const linkHeader = (request.response && request.response.headers) ? request.response.headers.link : null;
    if (linkHeader) {
      const links = parse(linkHeader);
      const requests = [];
      for (let i = 2; i <= links.last.page; i++) {
        const separator = request.url.includes('?') ? '&' : '?';
        const url = request.url + `${separator}page=${i}&per_page=100`;
        const newContext = extend(true, {}, request.context);
        const newRequest = new Request(request.type, url, newContext);
        // Carry this request's transitivity forward to the other pages.
        newRequest.policy = request.policy;
        requests.push(newRequest);
      }
      request.queueRequests(requests, 'soon');
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
    // Use the current request's policy as that is assumed to be precomputed for this page's elements. Don't bother queuing
    // if we are just going for the pages themselves (e.g., [] map step).
    const step = request.policy.getCurrentStep();
    if (Array.isArray(step) && step.length === 0) {
      return document;
    }
    // Queue up the page elements.  Use the same policy as this request as the page itself is more of an implementation
    // detail and should not be part of the user model of traversal.
    document.elements.forEach(item => {
      if (elementType) {
        const elementQualifier = this.isRootType(elementType) ? 'urn:' : qualifier;
        const newContext = { qualifier: elementQualifier, history: request.context.history };
        // review items are not "normal" and do not have a url property...
        // TODO consider queuing the reviews with their actual payload rather than as a url request.  consider effects on etags etc
        const url = elementType === 'review' ? `${item.pull_request_url}/reviews/${item.id}` : item.url;
        request.queue(elementType, url, request.policy, newContext);
      } else {
        // TODO if there is no elementType on a collection then assume it is events. Need to fix this up and
        // formalize the model of collections where the request carries the payload.
        const baseUrl = request.url.split("?")[0];
        const newContext = extend(true, {}, { history: request.context.history });
        const newRequest = new Request(item.type, `${baseUrl}/${item.id}`, newContext);
        newRequest.payload = { etag: 1, body: item };
        newRequest.policy = request.policy;
        request.queueRequests(newRequest);
      }
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
    this._addCollection(request, 'repos', 'repo', null, `urn:user:${document.id}:repos`);
    if (document.members_url) {
      this._addRelation(request, 'members', 'user', document.members_url.replace('{/member}', ''), `${this._getQualifier(request)}:org_members`);
    }
    const url = `${document.url.replace('/users/', '/orgs/')}/teams`;
    this._addRelation(request, 'teams', 'team', url, `${this._getQualifier(request)}:org_teams`);

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

    this._addCollection(request, 'repos', 'repo');
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

    if (document.organization) {
      this._addRoot(request, 'owner', 'user');
      this._addRoot(request, 'organization', 'org');
    } else {
      this._addRoot(request, 'owner', 'user');
    }

    this._addRelation(request, 'teams', 'team');
    // this._addRelation(request, 'collaborators', 'user', document.collaborators_url.replace('{/collaborator}', ''));
    this._addRelation(request, 'collaborators', 'user', document.collaborators_url.replace('{/collaborator}', '?affiliation=direct'));
    this._addRelation(request, 'contributors', 'user');
    if (document.subscribers_count) {
      this._addRelation(request, 'subscribers', 'user');
    }
    if (document.stargazers_count) {
      this._addRelation(request, 'stargazers', 'user');
    }
    this._addCollection(request, 'issues', 'issue', document.issues_url.replace('{/number}', '?state=all'));
    this._addCollection(request, 'commits', 'commit', document.commits_url.replace('{/sha}', ''));
    this._addCollection(request, 'events', null);

    return document;
  }

  pull_request_commit(request) {
    return this.commit(request, true);
  }

  commit(request, isPullRequestCommit = false) {
    const document = request.document;
    const context = request.context;
    let repoUrn = null;
    if (isPullRequestCommit) {
      repoUrn = `urn:repo:${context.qualifier.split(':')[2]}`;
      request.addSelfLink('sha');
      request.linkSiblings(`${context.qualifier}:pull_request_commits`);
      request.linkResource('pull_request', context.qualifier);
    } else if (context.qualifier.includes('PushEvent')) {
      repoUrn = `urn:repo:${context.qualifier.split(':')[2]}`;
      context.qualifier = `${repoUrn}:commit`;
      request.linkResource('self', `${context.qualifier}:${document.sha}`);
      request.linkSiblings(`${repoUrn}:commits`);
    } else {
      request.addSelfLink('sha');
      repoUrn = context.qualifier;
      request.linkSiblings(`${context.qualifier}:commits`);
    }
    this._addRoot(request, 'repo', 'repo', document.url.replace(/\/commits\/.*/, ''), repoUrn);

    // Most often there actually are no comments. Get the comments if we think there will be some and this resource is being processed (vs. traversed).
    // Note that if we are doing event processing, new comments will be added to the list dynamically so the only reason we need to refetch the
    // comment list in general is if we think we missed some events.
    const commitCommentType = isPullRequestCommit ? 'pull_request_commit_comment' : 'commit_comment';
    const commentsUrn = `${document._metadata.links.self.href}:${commitCommentType}s`;
    if (document.comments_url && (document.commit.comment_count > 0 && request.processMode === 'process')) {
      this._addCollection(request, `${commitCommentType}s`, commitCommentType, document.comments_url, commentsUrn);
    } else {
      // even if there are no comments to process, add a link to the comment collection for future use
      request.linkCollection(`${commitCommentType}s`, commentsUrn);
    }

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

  // It looks like there are only pull request review comments now. Pull request commit comments may not exist anymore.
  pull_request_commit_comment(request) {
    return this.commit_comment(request, true);
  }

  commit_comment(request, isPullRequest = false) {
    // TODO links to consider
    // * reactions -- get this by using the following Accept header: application/vnd.github.squirrel-girl-preview
    const document = request.document;
    const context = request.context;
    request.addSelfLink();
    const commitName = isPullRequest ? 'pull_request_commit' : 'commit';
    request.linkResource(commitName, context.qualifier);
    request.linkSiblings(`${context.qualifier}:${commitName}_comments`);

    this._addRoot(request, 'user', 'user');
    return document;
  }

  pull_request(request) {
    const document = request.document;
    const context = request.context;
    request.addSelfLink();
    request.linkResource('repo', `urn:repo:${document.base.repo.id}`);
    request.linkSiblings(`${context.qualifier}:pull_requests`);

    this._addRoot(request, 'user', 'user');
    this._addRoot(request, 'merged_by', 'user');
    this._addRoot(request, 'assignee', 'user');
    if (document.head.repo) {
      this._addRoot(request, 'head', 'repo', document.head.repo.url, `urn:repo:${document.head.repo.id}`);
    }
    this._addRoot(request, 'base', 'repo', document.base.repo.url, `urn:repo:${document.base.repo.id}`);

    this._addCollection(request, 'reviews', 'review', `${document._links.self.href}/reviews`);
    if (document._links.review_comments && document.comments) {
      this._addCollection(request, 'review_comments', 'review_comment', document._links.review_comments.href);
    }
    if (document._links.statuses) {
      const sha = document._links.statuses.href.split('/').slice(-1)[0];
      this._addCollection(request, 'statuses', 'status', document._links.statuses.href, `${context.qualifier}:commit:${sha}:statuses`);
      // this._addResource(request, 'status', 'status', null, statusUrl, `${context.qualifier}:commits:${sha}:status`);
    }

    if (document._links.commits && document.commits) {
      this._addCollection(request, 'pull_request_commits', 'pull_request_commit', document._links.commits.href);
    }

    // link and queue the related issue.  Getting the issue will bring in the comments for this PR
    if (document._links.issue) {
      // link to the relate issue and its comments.
      request.linkResource('issue', `${context.qualifier}:issue:${document.id}`);
      request.linkCollection('issue_comments', `${context.qualifier}:issue:${document.id}:issue_comments`);
    }
    return document;
  }

  review(request) {
    const document = request.document;
    const context = request.context;
    request.addSelfLink();
    request.linkResource('pull_request', context.qualifier);
    request.linkSiblings(`${context.qualifier}:reviews`);

    this._addRoot(request, 'user', 'user');
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
    if (document.comments_url && document.comments) {
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
    request.addRootSelfLink();
    request.linkSiblings(`urn:org:${document.organization.id}:teams`);

    this._addRoot(request, 'organization', 'org');
    this._addRelation(request, 'members', 'user', document.members_url.replace('{/member}', ''), `${this._getQualifier(request)}:team_members`);
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

  traffic(request) {
    const document = request.document;
    this._addResource(request, 'referrers', 'referrers', `urn:repo:${document.id}`, `${request.url}/traffic/popular/referrers`, null, `urn:repo:${document.id}`);
    this._addResource(request, 'paths', 'paths', `urn:repo:${document.id}`, `${request.url}/traffic/popular/paths`, null, `urn:repo:${document.id}`);
    this._addResource(request, 'views', 'views', `urn:repo:${document.id}`, `${request.url}/traffic/views`, null, `urn:repo:${document.id}`);
    this._addResource(request, 'clones', 'clones', `urn:repo:${document.id}`, `${request.url}/traffic/clones`, null, `urn:repo:${document.id}`);
    return null;
  }

  referrers(request) {
    return this._trafficChild(request);
  }

  views(request) {
    return this._trafficChild(request);
  }

  clones(request) {
    return this._trafficChild(request);
  }

  paths(request) {
    return this._trafficChild(request);
  }

  _trafficChild(request) {
    request.document.id = moment.utc(request.document._metadata.fetchedAt).format('YYYY_MM_DD');
    request.addSelfLink();
    request.linkResource('repo', request.context.qualifier);
    return request.document;
  }

  // ===============  Event Processors  ============

  // An event in a repo or org has been detected. Queue up a request to update the events for
  // the affected entity.  This event typically comes straight off the event feed and so is not
  // deduplicated.  By queuing it here, the hard work of getting the events etc is deduplicated.
  // This dramatically reduces the work as events in one repo tend to come in bursts.
  event_trigger(request) {
    request.markNoSave();
    if (this._isEventVisibleInTimeline(request.payload.type, request.payload.body.action)) {
      // If the request is visible in the timeline, toss the payload and queue an update_events request to query the timeline
      const url = request.url.substr(0, request.url.lastIndexOf('/'));
      const newRequest = new Request('update_events', url, request.context);
      request.queueRequests(newRequest, 'immediate');
    } else {
      // Otherwise, this is as good as it is going to get so queue the webhook event directly.
      const type = this._getTranslatedEventType(request.payload.type);
      const newContext = extend(true, {}, { history: request.context.history });
      const newRequest = new Request(type, request.url, newContext);
      newRequest.payload = request.payload;
      request.queueRequests(newRequest, 'immediate');
    }
    return null;
  }

  _isEventVisibleInTimeline(type, action) {
    if (['deployment', 'deployment_status', 'label', 'membership', 'milestone', 'organization',
      'page_build', 'repository', 'status', 'team', 'team_add', 'star'].includes(type)) {
      return false;
    }
    if (['issue_comment', 'pull_request_review_comment'].includes(type) && action === 'deleted') {
      return false;
    }
    if (type === 'member' && action === 'removed') {
      return false;
    }
    if (type === 'issues' && !['opened', 'closed', 'reopened'].includes(action)) {
      return false;
    }
    if (type === 'pull_request' && !['opened', 'closed', 'reopened'].includes(action)) {
      return false;
    }
    return true;
  }

  _getTranslatedEventType(type) {
    const eventTypes = {
      deployment: 'DeploymentEvent',
      deployment_status: 'DeploymentStatusEvent',
      issues: 'IssuesEvent',
      issue_comment: 'IssueCommentEvent',
      label: 'LabelEvent',
      member: 'MemberEvent',
      membership: 'MembershipEvent',
      milestone: 'MilestoneEvent',
      organization: 'OrganizationEvent',
      page_build: 'PageBuildEvent',
      pull_request: 'PullRequestEvent',
      pull_request_review_comment: 'PullRequestReviewCommentEvent',
      repository: 'RepositoryEvent',
      star: 'StarEvent',
      status: 'StatusEvent',
      team: 'TeamEvent',
      team_add: 'TeamAddEvent',
    };
    return eventTypes[type];
  }

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
        // Events are immutable (and we can't fetch them later) so set the etag to a constant
        const baseUrl = request.url.split("?")[0];
        const newContext = extend(true, {}, { history: request.context.history });
        const newRequest = new Request(event.type, `${baseUrl}/${event.id}`, newContext);
        newRequest.policy = TraversalPolicy.event(event.type);
        newRequest.payload = { etag: 1, body: event };
        return newRequest;
      });
      request.queueRequests(newRequests);
    });
    request.track(processingPromise);
    return null;
  }

  _findNew(events) {
    const self = this;
    return Q.all(events.map(qlimit(10)(event => {
      const url = event.repo ? `${event.repo.url}/events/${event.id}` : `${event.org.url}/events/${event.id}`;
      return self.store.etag(event.type, url).then(etag => {
        return etag ? null : event;
      });
    }))).then(events => {
      return events.filter(event => event);
    });
  }

  _unhandled_event(request) {
    // for unhandled events, just store the document for potential future processing.
    const doc = request.document;
    const urnId = doc.repo ? `repo:${doc.repo.id}` : `org:${doc.org.id}`;
    let [document] = this._addEventBasics(request, `urn:unhandled:${urnId}`);
    return document;
  }

  CommitCommentEvent(request) {
    let [document, repo, payload] = this._addEventBasics(request);
    let url = `${document.repo.url}/commits/${payload.comment.commit_id}`;
    let urn = `urn:repo:${repo}:commit:${payload.comment.commit_id}`;
    this._addResource(request, 'commit', 'commit', null, url, urn, `urn:repo:${repo}`);

    url = payload.comment.url;
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
    if (payload.action === 'deleted') {
      const context = { deletedAt: request.payload.fetchedAt };
      const policy = this._getNextDeletedPolicy();
      return this._addEventResource(request, repo, 'comment', 'issue_comment', qualifier, context, policy);
    }
    this._addEventResource(request, repo, 'comment', 'issue_comment', qualifier);
    return this._addEventResource(request, repo, 'issue');
  }

  IssuesEvent(request) {
    let [document, repo, payload] = this._addEventBasics(request);
    if (['deleted', 'transferred'].includes(payload.action)) {
      const context = { deletedAt: request.payload.fetchedAt, cacheKey: `urn:repo:${repo}:issue:${payload.issue.id}` };
      const policy = this._getNextDeletedPolicy();
      return this._addEventResource(request, repo, 'issue', 'issue', null, context, policy);
    }
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
    let [document, repo, payload] = this._addEventBasics(request);
    if (payload.action === 'added' || payload.action === 'removed') { // removed and not deleted as stated in documentation
      const relationPolicy = this._getNextRelationPolicy('repo', request);
      this._addEventResourceExplicit(request, 'repository', repo, (document.repo || document.repository).url, 'repo', null, {}, relationPolicy);
      if (payload.action === 'removed') {
        return document;
      }
    }
    return this._addEventResource(request, null, 'member', 'user');
  }

  MembershipEvent(request) {
    let [document, , payload] = this._addEventBasics(request);
    const userPolicy = TraversalPolicy.reload('user');
    this._addEventResource(request, null, 'member', 'user', null, {}, userPolicy);
    if (payload.team && payload.team.deleted) {
      return document;
    }
    const teamPolicy = TraversalPolicy.reload('team');
    return this._addEventResource(request, null, 'team', 'team', null, {}, teamPolicy);
  }

  MilestoneEvent(request) {
    // TODO complete implementation and add Milestone handler
    // let [, repo] = this._addEventBasics(request);
    // const context = (payload.action === 'deleted') ? { deletedAt: request.payload.fetchedAt } : {};
    // return this._addEventResource(request, repo, 'milestone', null, null, context);
    let [document] = this._addEventBasics(request);
    return document;
  }

  OrganizationEvent(request) {
    let [document, , payload] = this._addEventBasics(request);
    if (payload.action === 'renamed') {
      const policy = TraversalPolicy.reload('org');
      return this._addEventResource(request, null, 'organization', 'org', null, {}, policy);
    }
    if (payload.action === 'deleted') {
      const context = { deletedAt: request.payload.fetchedAt };
      const policy = this._getNextDeletedPolicy();
      return this._addEventResource(request, null, 'organization', 'org', null, context, policy);
    }
    return document;
  }

  OrgBlockEvent(request) {
    // TODO complete implementation and add organization handler
    let [document] = this._addEventBasics(request);
    return document;
  }

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
    let [, repo, payload] = this._addEventBasics(request);
    const newPolicy = request.getNextPolicy('issue');
    request.queue('issue', payload.pull_request.issue_url, newPolicy, { qualifier: `urn:repo:${repo}`, history: request.context.history });
    return this._addEventResource(request, repo, 'pull_request');
  }

  PullRequestReviewEvent(request) {
    let [, repo] = this._addEventBasics(request);
    return this._addEventResource(request, repo, 'pull_request');
  }

  PullRequestReviewCommentEvent(request) {
    if (request.document.payload && !request.document.payload.pull_request) {
      // this is a legacy event so tack on the event and queue up a fetch of the pull request to be processed
      // together so the stored event can have the right pull request link (id is missing in the old events).
      const context = { originalDocument: request.document };
      request.queue('LegacyPullRequestReviewCommentEvent', request.document.payload.comment.pull_request_url, request.policy, context);
      return null;
    }
    let [document, repo, payload] = this._addEventBasics(request);
    // Sometimes payload.pull_request is null instead of an object.
    if (!payload.pull_request) {
      return document;
    }
    const qualifier = `urn:repo:${repo}:pull_request:${payload.pull_request.id}`;
    if (payload.action === 'deleted') {
      const context = { deletedAt: request.payload.fetchedAt };
      const policy = this._getNextDeletedPolicy();
      return this._addEventResource(request, repo, 'comment', 'review_comment', qualifier, context, policy);
    }
    this._addEventResource(request, repo, 'comment', 'review_comment', qualifier);
    return this._addEventResource(request, repo, 'pull_request');
  }

  LegacyPullRequestReviewCommentEvent(request) {
    const pullRequestDocument = request.document;
    request.document = request.context.originalDocument;
    // Tweak the old event to look like a new one, do the normal processing and revert before storing.
    request.type = 'PullRequestReviewCommentEvent';
    request.document.payload.pull_request = {
      url: pullRequestDocument.url,
      id: pullRequestDocument.id
    };
    const result = this.PullRequestReviewCommentEvent(request);
    delete request.document.payload.pull_request;
    request.type = 'LegacyPullRequestReviewCommentEvent';
    return result;
  }

  // PushEvent is issued when commit/s are pushed to either default or other branches.
  // It causes a superset of commits from the default branch to be stored.
  // In some cases PushEvent is not visible in the timeline when new commits are pushed,
  // for example when a new branch containing commits is published, so some commits from
  // other branches are expected to be missing.
  PushEvent(request) {
    let [document, repo, payload] = this._addEventBasics(request);
    const qualifier = document._metadata.links.self.href;
    request.linkCollection('commits', `${qualifier}:commits`);
    const commits = payload.commits || [];
    const newRequests = commits.map(commit => {
      const newContext = extend(true, {}, { history: request.context.history, qualifier: qualifier });
      const newRequest = new Request('commit', commit.url, newContext);
      newRequest.policy = request.getNextPolicy('commits');
      return newRequest.policy ? newRequest : null;
    });
    request.queueRequests(newRequests.filter(request => request));
    return document;
  }

  ReleaseEvent(request) {
    // TODO complete implementation and add a Release handler
    // let [, repo] = this._addEventBasics(request);
    // return this._addEventResource(request, repo, 'release');
    let [document] = this._addEventBasics(request);
    return document;
  }

  RepositoryEvent(request) {
    let [, , payload] = this._addEventBasics(request, null, ['actor', 'org']);
    if (['created', 'transferred'].includes(payload.action)) {
      const policy = TraversalPolicy.reload('repo');
      return this._addEventResource(request, null, 'repository', 'repo', null, {}, policy);
    }
    if (payload.action === 'deleted') {
      const context = { deletedAt: request.payload.fetchedAt };
      const policy = this._getNextDeletedPolicy();
      return this._addEventResource(request, null, 'repository', 'repo', null, context, policy);
    }
    return this._addEventResource(request, null, 'repository', 'repo');
  }

  StarEvent(request) {
    let [document] = this._addEventBasics(request);
    request.linkResource('repository', document._metadata.links.repo.href);
    return document;
  }

  StatusEvent(request) {
    // TODO complete this by adding a status handler and linking/queuing the status here.
    let [document, , payload] = this._addEventBasics(request);
    request.linkResource('commit', `${document._metadata.links.repo.href}:commit:${payload.sha}`);
    return document;
  }

  TeamEvent(request) {
    let [, , payload] = this._addEventBasics(request, `urn:team:${(request.document.payload || request.document).team.id}`);
    if (payload.action === 'added_to_repository' || payload.action === 'removed_from_repository') {
      const relationPolicy = this._getNextRelationPolicy('repo', request);
      return this._addEventResource(request, null, 'repository', 'repo', null, {}, relationPolicy);
    }
    if (payload.repository) {
      this._addEventResource(request, null, 'repository', 'repo');
    }
    if (payload.action === 'created') {
      const policy = TraversalPolicy.reload('team');
      return this._addEventResource(request, null, 'team', 'team', null, {}, policy);
    }
    if (payload.action === 'deleted') {
      const context = { deletedAt: request.payload.fetchedAt };
      const policy = this._getNextDeletedPolicy();
      return this._addEventResource(request, null, 'team', 'team', null, context, policy);
    }
    return this._addEventResource(request, null, 'team');
  }

  TeamAddEvent(request) {
    this._addEventBasics(request, `urn:team:${(request.document.payload || request.document).team.id}`);
    this._addEventResource(request, null, 'repository', 'repo');
    return this._addEventResource(request, null, 'team');
  }

  WatchEvent(request) {
    let [document] = this._addEventBasics(request);
    request.linkResource('repository', document._metadata.links.repo.href);
    return document;
  }

  // ================ HELPERS ========================

  isCollectionType(request) {
    const collections = new Set([
      'collaborators', 'commit_comments', 'commits', 'contributors', 'events', 'issues', 'issue_comments', 'members', 'orgs', 'pull_request_commit_comments', 'pull_request_commits', 'repos', 'reviews', 'review_comments', 'subscribers', 'stargazers', 'statuses', 'teams'
    ]);
    return collections.has(request.type);
  }

  isRootType(type) {
    const roots = new Set(['orgs', 'org', 'repos', 'repo', 'teams', 'team', 'user', 'members']);
    return roots.has(type);
  }

  _getQualifier(request) {
    return this.isRootType(request.type) ? request.getRootQualifier() : request.getChildQualifier();
  }

  _getHandler(request, type = request.type) {
    const parsed = URL.parse(request.url, true);
    const page = parsed.query.page;
    // TODO / check is a temporary measure to work around a queuing bug. Remove once queue is cleared
    if (page && !parsed.query.per_page.includes('/')) {
      return this.page.bind(this, page);
    }
    if (this.isCollectionType(request)) {
      return this.collection;
    }
    const handler = this[type];
    if (handler) {
      return handler;
    }
    if (type.endsWith('Event')) {
      return this._unhandled_event;
    }
    return null;
  }

  _addEventBasics(request, qualifier = null, queueList = ['actor', 'repo', 'org']) {
    // TODO handle org event cases (no repo etc)
    const document = request.document;
    let repo = document.repo ? document.repo.id : null;
    if (!repo) {
      repo = document.repository ? document.repository.id : null;
    }
    let orgId = document.org ? document.org.id : null;
    if (!orgId) {
      orgId = document.organization ? document.organization.id : null;
    }
    qualifier = qualifier || (repo ? `urn:repo:${repo}` : `urn:org:${orgId}`);
    const id = document.id || request.payload.deliveryId;
    request.linkResource('self', `${qualifier}:${request.type}:${id}`);
    request.linkSiblings(`${qualifier}:${request.type}s`);

    // TODO understand if the actor is typically the same as the creator or pusher in the payload
    this._addRoot(request, 'actor', 'user', null, null, queueList.includes('actor'));
    if (repo) {
      this._addRoot(request, 'repo', 'repo', null, null, queueList.includes('repo'));
    }
    this._addRoot(request, 'org', 'org', null, null, queueList.includes('org'));
    return [document, repo, document.payload || document];
  }

  _addEventResource(request, repo, name, type = name, qualifier = null, context = {}, policy = null) {
    const payload = request.document.payload || request.document;
    const target = payload[name];
    if (!target) {
      throw new Error(`payload[${name}] missing in ${request.toString()}`);
    }
    // if the repo is given then use it. Otherwise, assume the type is a root and construct a urn
    qualifier = qualifier || (repo ? `urn:repo:${repo}` : 'urn:');
    context.qualifier = qualifier;
    const separator = qualifier.endsWith(':') ? '' : ':';
    request.linkResource(name, `${qualifier}${separator}${type}:${payload[name].id}`);
    const newContext = extend(true, {}, context, { history: request.context.history, qualifier: qualifier });
    const newRequest = new Request(type, payload[name].url, newContext);
    newRequest.policy = policy || request.getNextPolicy(name);
    if (newRequest.policy) {
      request.queueRequests(newRequest);
    }
    return request.document;
  }

  _addEventResourceExplicit(request, name, id, url, type = name, qualifier = null, context = {}, policy = null) {
    qualifier = qualifier || 'urn:';
    context.qualifier = qualifier;
    const separator = qualifier.endsWith(':') ? '' : ':';
    request.linkResource(name, `${qualifier}${separator}${type}:${id}`);
    const newContext = extend(true, {}, context, { history: request.context.history, qualifier: qualifier });
    const newRequest = new Request(type, url, newContext);
    newRequest.policy = policy || request.getNextPolicy(name);
    if (newRequest.policy) {
      request.queueRequests(newRequest);
    }
    return request.document;
  }

  _getNextRelationPolicy(name, request) {
    const map = VisitorMap.getMap(`relationOnly/${request.type}`);
    if (!map) {
      return null;
    }
    const policy = request.policy.getNextPolicy(name, map);
    policy.fetch = 'originStorage';
    return policy;
  }

  _getNextDeletedPolicy() {
    return new TraversalPolicy('storageOnly', 'always', VisitorMap.getMap('self'));
  }

  _addResource(request, name, type, id, url = null, urn = null, qualifier = null) {
    qualifier = qualifier || this._getQualifier(request);
    urn = urn || `${qualifier}:${name}:${id}`;
    url = url || request.document[`${name}_url`];

    request.linkResource(name, urn);
    const newPolicy = request.getNextPolicy(name);
    request.queue(type, url, newPolicy, { qualifier: qualifier });
  }

  _addCollection(request, name, type, url = null, urn = null) {
    const qualifier = this._getQualifier(request);
    urn = urn || `${qualifier}:${name}`;
    url = url || request.document[`${name}_url`];

    request.linkCollection(name, urn);
    const newPolicy = request.getNextPolicy(name);
    const newContext = { qualifier: request.document._metadata.links.self.href, elementType: type };
    request.queue(name, url, newPolicy, newContext);
  }

  _addRoot(request, name, type, url = null, urn = null, queue = true) {
    let element = request.document[name];
    if (!element) {
      if (name === 'actor') {
        element = request.document.sender;
      }
      if (name === 'repo') {
        element = request.document.repository;
      }
      if (name === 'org') {
        element = request.document.organization;
      }
    }
    // If there is no element then we must have both the url and urn as otherwise we don't know how to compute them
    if ((!element || Object.getOwnPropertyNames(element).length === 0) && !(urn && url)) {
      return;
    }

    urn = urn || `urn:${type}:${element.id}`;
    url = url || element.url;
    request.linkResource(name, urn);
    const newPolicy = request.getNextPolicy(name);
    if (queue) {
      request.queue(type, url, newPolicy);
    }
  }

  /**
   * Relate this document to a collection of other documents of the given type.  For example,
   * a repo to its collaborators which are users.
   *
   * This creates a relation between the current document being processed and the named
   * target resource of the given type. This results in a siblings link with the given name
   * and urn being added to this document and a relation request queued for the given url.
   * The document produced by processing that url will have matching siblings links (called 'siblings')
   *
   * Relations are always references.
   */
  _addRelation(request, name, type, url = null, urn = null) {
    const qualifier = this._getQualifier(request);
    urn = urn || `${qualifier}:${name}`;
    url = url || request.document[`${name}_url`];

    // For relations we want to have a guid that uniquely identifies all of the pages for this
    // particular state of the relation.  Add the guid here for the relation link and brand
    // each page with it in its siblings link to get a coherent state.
    const guid = uuid.v4();
    request.linkRelation(name, `${urn}:pages:${guid}`);
    // Also add an 'knownPages' link to help deal with page clean up.
    // request.linkCollection('knownPages', `${urn}:pages`);
    const context = { qualifier: qualifier, relation: { origin: request.type, qualifier: urn, type: type, guid: guid } };
    const newPolicy = request.getNextPolicy(name);
    request.queue(name, url, newPolicy, context, false);
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
    request.linkSiblings(`${relation.qualifier}:pages`);
    request.linkCollection('unique', `${relation.qualifier}:pages:${relation.guid}`);
    const urns = document.elements.map(element => `urn:${relation.type}:${element['id']}`);
    request.linkResource('resources', urns);
    return document;
  }
}

module.exports = GitHubProcessor;