const extend = require('extend');

/**
  Requests describe a resource to capture and process as well as the context for that processing.

  Transitivity
   * none - Only process this exact resource
   * normal - Process this resource if not previously seen and do normal processing on non-roots and roots
   * forceNone - Process this resource and force processing on non-roots and no processing of roots
   * forceNormal - Force processing of children plus normal processing of roots
   * forceForce - Force processing of children and roots.  Decays to forceNormal on roots
  Basically, once you are forcing, force transitivity for all children, but still allow control over transitivity
  when traversing to a root.  When traversing with forceForce, queued roots end up as forceNormal.  Similarly,
  when traversing with forceNormal, queued roots end up as normal.

  Fetch behavior
    * none - Only use existing content.  Skip this resource if we don't already have it
    * normal - Use existing content if we have it and it matches.  Otherwise, get content from original source
    * force - Ignore exiting content and get contenf from original source
*/

class Request {
  constructor(type, url, context = null) {
    this.type = type;
    this.url = url;
    this.transitivity = 'normal';
    this.fetch = 'normal';
    this.context = context || {};
    this.promises = [];
  }

  track(promises) {
    if (!promises) {
      return;
    }
    if (Array.isArray(promises)) {
      Array.prototype.push.apply(this.promises, promises);
    } else {
      this.promises.push(promises);
    }
  }

  addMeta(data) {
    this.meta = extend({}, this.meta, data);
    return this;
  }

  addRootSelfLink() {
    this.addSelfLink('id', 'urn:');
  }

  addSelfLink(key = 'id', base = null) {
    let qualifier = base ? base : this.context.qualifier;
    if (!qualifier || (typeof qualifier !== 'string')) {
      throw new Error('Need something on which to base the self link URN');
    }
    qualifier = qualifier.endsWith(':') ? qualifier : qualifier + ':';
    this.linkSelf('self', `${qualifier}${this.type}:${this.document[key]}`);
  }

  linkSelf(name, value) {
    const links = this.document._metadata.links;
    const key = Array.isArray(value) ? 'hrefs' : 'href';
    links[name] = { type: 'self' };
    links[name][key] = value;
  }

  linkSiblings(name, href) {
    const links = this.document._metadata.links;
    links[name] = { type: 'siblings', href: href };
  }

  queue(type, url, context) {
    const newRequest = new Request(type, url);
    newRequest.context = context;
    newRequest.fetch = this.fetch;
    this.track(this.crawler.queue(newRequest));
  }

  queueRoot(type, url) {
    const transitivity = this._getRootTransitivity();
    if (!transitivity) {
      return;
    }
    const newRequest = new Request(type, url);
    newRequest.context = { qualifier: 'urn:' };
    // set the new request's transitivity to the next value
    newRequest.transitivity = transitivity;
    newRequest.fetch = this.fetch;
    this.track(this.crawler.queue(newRequest));
  }

  queueRoots(type, url, context = null) {
    const transitivity = this._getRootTransitivity();
    if (!transitivity) {
      return;
    }
    const newRequest = new Request(type, url);
    const newContext = extend({}, this.context, context);
    newContext.qualifier = this.document._metadata.links.self.href;
    newRequest.context = newContext;
    // carry over this requests transitivity as we are queuing a collection
    newRequest.transitivity = this.transitivity;
    newRequest.fetch = this.fetch;
    this.track(this.crawler.queue(newRequest));
  }

  queueCollectionElement(type, url, qualifier) {
    if (this.isRootType(type)) {
      return this.queueRoot(type, url);
    }
    return this.queueChild(type, url, qualifier);
  }

  queueChild(type, url, qualifier) {
    const transitivity = this._getChildTransitivity();
    if (!transitivity) {
      return;
    }
    const newRequest = new Request(type, url);
    newRequest.context = this.context || {};
    newRequest.context.qualifier = qualifier;
    newRequest.transitivity = transitivity;
    newRequest.fetch = this.fetch;
    this.track(this.crawler.queue(newRequest));
  }

  queueChildren(type, url, context = null) {
    const transitivity = this._getChildTransitivity();
    if (!transitivity) {
      return;
    }
    const newRequest = new Request(type, url);
    const newContext = extend({}, this.context, context);
    newContext.qualifier = this.document._metadata.links.self.href;
    newRequest.context = newContext;
    // carry over this requests transitivity as we are queuing a collection
    newRequest.transitivity = this.transitivity;
    newRequest.fetch = this.fetch;
    this.track(this.crawler.queue(newRequest));
  }

  _getRootTransitivity() {
    return { normal: 'normal', forceNormal: 'normal', forceForce: 'forceNormal' }[this.transitivity];
  }

  _getChildTransitivity() {
    return { normal: 'normal', forceNone: 'forceNone', forceNormal: 'forceNormal', forceForce: 'forceNormal' }[this.transitivity];
  }

  isReprocessing() {
    return this.fetch === 'none';
  }

  isForced() {
    return this.transitivity.startsWith('force');
  }

  isForcedFetch() {
    return this.fetch === 'force';
  }

  markSkip(outcome, message) {
    if (this.shouldSkip()) {
      return this;
    }
    this.processControl = 'skip';
    this.outcome = this.outcome || outcome;
    this.message = this.message || message;
    return this;
  }

  markRequeue(outcome, message) {
    if (this.shouldRequeue()) {
      return this;
    }
    this.processControl = 'requeue';
    this.outcome = this.outcome || outcome;
    this.message = this.message || message;
    return this;
  }

  shouldSkip() {
    return this.processControl === 'skip' || this.processControl === 'requeue';
  }

  delayUntil(time) {
    if (!this.nextRequestTime || this.nextRequestTime < time) {
      this.nextRequestTime = time;
    }
  }

  delay(milliseconds = 2000) {
    this.delayUntil(Date.now() + milliseconds);
  }

  shouldRequeue() {
    return this.processControl === 'requeue';
  }

  eventHelper(references) {
    const document = this.document;
    // TODO understand if the actor is typically the same as the creator or pusher in the payload
    const repo = document.repo ? document.repo.id : null;
    const urn = repo ? `urn:repo:${repo}` : `urn:org:${document.org.id}`;
    this.linkSelf('self', `${urn}:${this.type}:${document.id}`);
    this.linkSelf('actor', `urn:login:${document.actor.id}`);
    this.linkSelf('repo', `urn:repo:${document.repo.id}`);
    this.linkSelf('org', `urn:org:${document.org.id}`);
    this.queueRoot('login', document.actor.url);
    this.queueRoot('repo', document.repo.url);
    this.queueRoot('org', document.org.url);
    return document.payload;
  }

  getCollectionType() {
    const collections = {
      orgs: 'org', repos: 'repo', issues: 'issue', issue_comments: 'issue_comment', commits: 'commit', teams: 'team', members: 'user', team_members: 'user', team_repos: 'repo', collaborators: 'user', contributors: 'user', subscribers: 'user'
    };
    return collections[this.type];
  }

  isRootType(type) {
    const roots = new Set(['orgs', 'org', 'repos', 'repo', 'teams', 'team', 'user', 'members']);
    return roots.has(type);
  }
}

module.exports = Request;