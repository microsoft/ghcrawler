const extend = require('extend');
const Policy = require('./traversalPolicy');

/**
 * Requests describe a resource to capture and process as well as the context for that processing.
 */
class Request {
  constructor(type, url, context = null) {
    this.type = type;
    this.url = url;
    this.policy = Policy.default();
    this.context = context || {};
    this.promises = [];
  }

  static adopt(object) {
    if (object.__proto__ !== Request.prototype) {
      object.__proto__ = Request.prototype;
    }
    if (object.policy && object.policy.__proto__ !== Policy.prototype) {
      object.policy.__proto__ = Policy.prototype;
    }
    return object;
  }

  track(promises) {
    if (!promises) {
      return this;
    }
    if (Array.isArray(promises)) {
      Array.prototype.push.apply(this.promises, promises);
    } else {
      this.promises.push(promises);
    }
    return this;
  }

  addMeta(data) {
    this.meta = extend({}, this.meta, data);
    return this;
  }

  addRootSelfLink() {
    this.linkResource('self', this.getRootQualifier());
  }

  addSelfLink(key = 'id') {
    this.linkResource('self', this.getChildQualifier(key));
  }

  getQualifier() {
    return this.isRootType(this.type) ? this.getRootQualifier() : this.getChildQualifier();
  }

  getRootQualifier() {
    return `urn:${this.type}:${this.document.id}`;
  }

  getChildQualifier(key = 'id') {
    let qualifier = this.context.qualifier;
    if (!qualifier || (typeof qualifier !== 'string')) {
      throw new Error('Need something on which to base the self link URN');
    }
    qualifier = qualifier.endsWith(':') ? qualifier : qualifier + ':';
    return `${qualifier}${this.type}:${this.document[key]}`;
  }

  linkResource(name, value) {
    const links = this.document._metadata.links;
    const key = Array.isArray(value) ? 'hrefs' : 'href';
    links[name] = {};
    links[name][key] = value;
    links[name].type = 'resource';
  }

  linkSiblings(href) {
    const links = this.document._metadata.links;
    links.siblings = { href: href, type: 'collection' };
  }

  linkCollection(name, href) {
    const links = this.document._metadata.links;
    links[name] = { href: href, type: 'collection' };
  }

  linkRelation(name, href) {
    const links = this.document._metadata.links;
    links[name] = { href: href, type: 'relation' };
  }

  queue(type, url, context) {
    const newRequest = new Request(type, url);
    newRequest.context = context;
    newRequest.fetch = this.fetch;
    this.track(this.crawler.queue(newRequest));
  }

  queueRoot(type, url) {
    const policy = this.policy.createPolicyForRoot();
    if (!policy) {
      return;
    }
    const newRequest = new Request(type, url);
    newRequest.context = { qualifier: 'urn:' };
    newRequest.policy = policy;
    // relations are not transitive so ensure any relation is stripped off
    delete newRequest.context.relation;
    this.track(this.crawler.queue(newRequest));
  }

  queueRoots(type, url, context = null) {
    const policy = this.policy.createPolicyForRoot();
    if (!policy) {
      return;
    }
    const newRequest = new Request(type, url);
    const newContext = extend({}, this.context, context);
    newContext.qualifier = this.document._metadata.links.self.href;
    newRequest.context = newContext;
    // We are queuing a collection so carry this request's policy over.  A new policy may
    // apply to the elements in the collection
    newRequest.policy = this.policy;
    this.track(this.crawler.queue(newRequest));
  }

  queueCollectionElement(type, url, qualifier) {
    if (this.isRootType(type)) {
      return this.queueRoot(type, url);
    }
    return this.queueChild(type, url, qualifier);
  }

  queueChild(type, url, qualifier) {
    const policy = this.policy.createPolicyForChild();
    if (!policy) {
      return;
    }
    const newRequest = new Request(type, url);
    newRequest.context = this.context || {};
    newRequest.context.qualifier = qualifier;
    newRequest.policy = policy;
    // relations are not transitive so ensure any relation is stripped off
    delete newRequest.context.relation;
    this.track(this.crawler.queue(newRequest));
  }

  queueChildren(type, url, context = null) {
    const policy = this.policy.createPolicyForChild();
    if (!policy) {
      return;
    }
    const newRequest = new Request(type, url);
    const newContext = extend({}, this.context, context);
    newContext.qualifier = this.document._metadata.links.self.href;
    newRequest.context = newContext;
    // We are queuing a collection so carry this request's policy over.  A new policy may
    // apply to the elements in the collection
    newRequest.policy = this.policy;
    this.track(this.crawler.queue(newRequest));
  }

  markSkip(outcome, message) {
    // Ensure we don't miss out on any errors.  If there is already an error, log it.
    if (outcome === 'Error' && this.outcome === 'Error') {
      this._logCurrentError();
    }
    // Keep the first outcome unless the new one is an error
    if (this.shouldSkip() && outcome !== 'Error') {
      return this;
    }
    this.processControl = 'skip';
    this.outcome = this.outcome || outcome;
    this.message = this.message || message;
    return this;
  }

  markRequeue(outcome, message) {
    // Ensure we don't miss out on any errors.  If there is already an error, log it.
    if (outcome === 'Error' && this.outcome === 'Error') {
      this._logCurrentError();
    }
    // Keep the first outcome unless the new one is an error
    if (this.shouldRequeue() && outcome !== 'Error') {
      return this;
    }
    this.processControl = 'requeue';
    this.outcome = this.outcome || outcome;
    this.message = this.message || message;
    return this;
  }

  _logCurrentError() {
    if (this._crawler && this.outcome === 'Error') {
      this.crawler._logOutcome(this);
    }
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
    this.linkResource('self', `${urn}:${this.type}:${document.id}`);
    this.linkResource('actor', `urn:user:${document.actor.id}`);
    this.linkResource('repo', `urn:repo:${document.repo.id}`);
    this.linkResource('org', `urn:org:${document.org.id}`);
    this.queueRoot('user', document.actor.url);
    this.queueRoot('repo', document.repo.url);
    this.queueRoot('org', document.org.url);
    return document.payload;
  }

  toString() {
    return `${this.type}@${this.url}`;
  }

  toUniqueString() {
    return `${this.type}@${this.url}:${this.policy.getShortForm()}`;
  }

  isCollectionType() {
    const collections = new Set([
      'orgs', 'repos', 'issues', 'comments', 'commits', 'teams', 'members', 'collaborators', 'contributors', 'subscribers'
    ]);
    return collections.has(this.type);
  }

  isRootType(type) {
    const roots = new Set(['orgs', 'org', 'repos', 'repo', 'teams', 'team', 'user', 'members', 'traffic']);
    return roots.has(type);
  }
}

module.exports = Request;