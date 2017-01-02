// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

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
    this.promises;
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

  getTrackedPromises() {
    return this.promises || [];
  }

  track(promises) {
    if (!promises) {
      return this;
    }
    this.promises = this.promises || [];
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

  linkResource(name, urn) {
    const links = this.document._metadata.links;
    const key = Array.isArray(urn) ? 'hrefs' : 'href';
    links[name] = {};
    links[name][key] = urn;
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

  queue(requests, name = null) {
    this.track(this.crawler.queue(requests, name));
  }

  queueRoot(type, url) {
    const policy = this.policy.createPolicyForRoot();
    if (!policy) {
      return;
    }
    const newRequest = new Request(type, url, { qualifier: 'urn:' });
    newRequest.policy = policy;
    // relations are not transitive so ensure any relation is stripped off
    delete newRequest.context.relation;
    this.queue(newRequest);
  }

  queueRoots(type, url, context = null) {
    const policy = this.policy.createPolicyForRoot();
    if (!policy) {
      return;
    }
    const newRequest = new Request(type, url, extend({}, this.context, context));
    newRequest.context.qualifier = this.document._metadata.links.self.href;
    // We are queuing a collection so carry this request's policy over.  A new policy may
    // apply to the elements in the collection
    newRequest.policy = this.policy;
    this.queue(newRequest);
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
    const newRequest = new Request(type, url, extend({}, this.context || {}));
    newRequest.context.qualifier = qualifier;
    newRequest.policy = policy;
    // relations are not transitive so ensure any relation is stripped off
    delete newRequest.context.relation;
    this.queue(newRequest);
  }

  queueChildren(type, url, context = null) {
    const policy = this.policy.createPolicyForChild();
    if (!policy) {
      return;
    }
    const newRequest = new Request(type, url, extend({}, this.context, context));
    newRequest.context.qualifier = this.document._metadata.links.self.href;
    // We are queuing a collection so carry this request's policy over.  A new policy may
    // apply to the elements in the collection
    newRequest.policy = this.policy;
    this.queue(newRequest);
  }

  markSkip(outcome, message) {
    // if we are already skipping/requeuing, keep the original as the official outcome but log this new one so its not missed
    if (this.shouldSkip()) {
      this._log('verbose', `Redundant skip: ${outcome}, ${message}`, this.meta);
      return this;
    }
    this.processControl = 'skip';
    this.outcome = this.outcome || outcome;
    this.message = this.message || message;
    return this;
  }

  markRequeue(outcome, message) {
    // if we are already skipping/requeuing, keep the original as the official outcome but log this new one so its not missed
    if (this.shouldSkip()) {
      this._log('verbose', `Redundant requeue: ${outcome}, ${message}`, this.meta);
      return this;
    }
    this.processControl = 'requeue';
    this.outcome = this.outcome || outcome;
    this.message = this.message || message;
    return this;
  }

  markNoSave() {
    this.save = false;
  }

  shouldSave() {
    return (this.save !== false) && this.document && this.contentOrigin !== 'cacheOfOrigin';
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

  shouldFetchExisting() {
    return this.context.relation || this.policy.shouldFetchExisting(this);
  }

  toString() {
    return `${this.type}@${this.url}`;
  }

  toUniqueString() {
    return `${this.type}@${this.url}:${this.policy.getShortForm()}`;
  }

  isCollectionType() {
    const collections = new Set([
      'collaborators', 'commit_comments', 'commits', 'contributors', 'events', 'issues', 'issue_comments', 'members', 'orgs', 'repos', 'review_comments', 'subscribers', 'statuses', 'teams'
    ]);
    return collections.has(this.type);
  }

  isRootType(type) {
    const roots = new Set(['orgs', 'org', 'repos', 'repo', 'teams', 'team', 'user', 'members']);
    return roots.has(type);
  }

  _log(level, message, meta = null) {
    if (this.crawler) {
      this.crawler.logger.log(level, message, meta);
    }
  }
}

module.exports = Request;