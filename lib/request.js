// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Policy = require('./traversalPolicy');
const TraversalPolicy = require('./traversalPolicy');

/**
 * Requests describe a resource to capture and process as well as the context for that processing.
 */
class Request {
  constructor(type, url, context = null, relationship = 'contains') {
    this.type = type;
    this.url = url;
    this.context = context || {};
    this.relationship = relationship;
    this.policy = Policy.default();
  }

  static adopt(object) {
    if (object.__proto__ !== Request.prototype) {
      object.__proto__ = Request.prototype;
    }
    object.policy = Request._getExpandedPolicy(object.policy);
    if (object.policy && object.policy.__proto__ !== Policy.prototype) {
      object.policy.__proto__ = Policy.prototype;
    }
    this.relationship = this.relationship || 'contains';
    return object;
  }

  static _getExpandedPolicy(policyOrSpec) {
    return typeof policyOrSpec === 'string' ? Policy.getPolicy(policyOrSpec) : policyOrSpec;
  }

  // Setup some internal context and open this request for handling.
  open() {
    this.context = this.context || {};
    this._addHistory();
    this._expandPolicy();
    return this;
  }

  _expandPolicy() {
    if (typeof this.policy === 'string') {
      const policy = TraversalPolicy.getPolicy(this.policy);
      if (!policy) {
        return this._queueDead(this, `Unknown request policy: ${this.policy}`);
      }
      this.policy = policy;
    }
  }

  _addHistory() {
    this.context.history = this.context.history || [];
    this.context.history.push(this.toString());
  }

  hasSeen(request) {
    const history = this.context.history || [];
    return history.includes(request.toString());
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
    this.meta = Object.assign({}, this.meta, data);
    return this;
  }

  addRootSelfLink() {
    this.linkResource('self', this.getRootQualifier());
  }

  addSelfLink(key = 'id') {
    this.linkResource('self', this.getChildQualifier(key));
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

  queueRequests(requests, name = null) {
    this.track(this.crawler.queue(requests, name));
  }

  queue(relationship, type, url, context = null, pruneRelation = true, policy = null) {
    policy = policy || this.policy.getNextPolicy(this, relationship);
    if (!policy) {
      return;
    }
    context = Object.assign({}, this.context, context);
    context.qualifier = context.qualifier || 'urn:';
    const newRequest = new Request(type, url, context, relationship);
    newRequest.policy = policy;
    // relations are not transitive so ensure any relation is stripped off
    if (pruneRelation) {
      delete newRequest.context.relation;
    }
    this.queueRequests(newRequest);
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

  markSave() {
    this.save = true;
  }

  markNoSave() {
    this.save = false;
  }

  shouldSave() {
    return this.document && (this.save === true || (this.save !== false && this.contentOrigin !== 'cacheOfOrigin'));
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

  createRequeuable() {
    // Create a new request data structure that has just the things we should queue
    const queuable = new Request(this.type, this.url, this.context, this.relationship);
    queuable.attemptCount = this.attemptCount;
    queuable.policy = this.policy;
    if (this.payload) {
      queuable.payload = this.payload;
    }
    return queuable;
  }

  toString() {
    return `${this.type}@${this.url}`;
  }

  toUniqueString() {
    return `${this.type}@${this.url}:${Request._getExpandedPolicy(this.policy).getShortForm()}`;
  }

  _log(level, message, meta = null) {
    if (this.crawler) {
      this.crawler.logger.log(level, message, meta);
    }
  }
}

module.exports = Request;