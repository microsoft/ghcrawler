// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Policy = require('./traversalPolicy');

/**
 * Requests describe a resource to capture and process as well as the context for that processing.
 */
class Request {
  constructor(type, url, context = null) {
    this.type = type;
    this.url = url;
    this.context = context || {};
    this.policy = Policy.default(type);
  }

  static adopt(object) {
    if (object.__proto__ !== Request.prototype) {
      object.__proto__ = Request.prototype;
    }
    if (object.policy) {
      object.policy = Request._getResolvedPolicy(object);
      Policy.adopt(object.policy);
    } else {
      Policy.default(this.type);
    }
    return object;
  }

  static _getResolvedPolicy(request) {
    let policyOrSpec = request.policy;
    if (typeof policyOrSpec !== 'string') {
      return policyOrSpec;
    }
    policyOrSpec = policyOrSpec.includes(':') ? policyOrSpec : `${policyOrSpec}:${request.type}`;
    return Policy.getPolicy(policyOrSpec);
  }

  // Setup some internal context and open this request for handling.
  open(crawler) {
    this.crawler = crawler;
    this.start = Date.now();
    this.context = this.context || {};
    this._addHistory();
    const root = this.context.history.length <= 1 ? 'self' : this.context.history[0];
    this.addMeta({ root: root });
    this._resolvePolicy();
    return this;
  }

  _resolvePolicy() {
    if (!this.policy) {
      return this.markDead('Bogus', 'No policy');
    }
    if (typeof this.policy === 'string') {
      // if the policy spec does not include a map, default to using the type of this request as the map name
      const spec = this.policy.includes(':') ? this.policy : `${this.policy}:${this.type}`;
      const policy = Policy.getPolicy(spec);
      if (!policy) {
        return this.markDead('Bogus', 'Unable to resolve policy');
      }
      this.policy = policy;
    }
  }

  _addHistory(message = null) {
    this.context.history = this.context.history || [];
    this.context.history.push((message || this).toString());
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

  getNextPolicy(name) {
    return this.policy.getNextPolicy(name);
  }

  queueRequests(requests, name = null) {
    requests = Array.isArray(requests) ? requests : [requests];
    const toQueue = requests.filter(request => !this.hasSeen(request));
    this.track(this.crawler.queue(toQueue, name));
  }

  queue(type, url, policy, context = null, pruneRelation = true) {
    if (!policy) {
      return;
    }
    context = Object.assign({}, this.context, context);
    context.qualifier = context.qualifier || 'urn:';
    const newRequest = new Request(type, url, context);
    newRequest.policy = policy;
    // relations are not transitive so ensure any relation is stripped off
    if (pruneRelation) {
      delete newRequest.context.relation;
    }
    this.queueRequests(newRequest);
  }

  markDead(outcome, message) {
    this.track(this.crawler.storeDeadletter(this, message));
    return this.markSkip(outcome, message);
  }

  markSkip(outcome, message) {
    return this._cutShort(outcome, message, 'skip');
  }

  markRequeue(outcome, message) {
    this._addHistory(` Requeued: ${outcome} ${message}`);
    return this._cutShort(outcome, message, 'requeue');
  }

  markDefer(outcome, message) {
    this.crawler.queues.defer(this);
    return this._cutShort(outcome, message, 'defer');
  }

  _cutShort(outcome, message, reason) {
    // if we are already skipping/requeuing, keep the original as the official outcome but log this new one so its not missed
    if (this.shouldSkip()) {
      this._log('verbose', `Redundant ${reason}: ${outcome}, ${message}`, this.meta);
      return this;
    }
    this.processControl = reason;
    // overwrite previous outcomes if this is an error and the current is not.
    if (outcome === 'Error' && this.outcome !== 'Error') {
      this.outcome = outcome;
      this.message = message;
    } else {
      this.outcome = this.outcome || outcome;
      this.message = this.message || message;
    }
    return this;
  }

  markSave() {
    this.save = true;
    return this;
  }

  markNoSave() {
    this.save = false;
    return this;
  }

  shouldSave() {
    return this.document && (this.save === true || (this.save !== false && this.contentOrigin !== 'cacheOfOrigin'));
  }

  shouldSkip() {
    return this.processControl === 'skip' || this.processControl === 'requeue' || this.processControl === 'defer';
  }

  isDeferred() {
    return this.processControl === 'defer';
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
    const queuable = new Request(this.type, this.url, this.context);
    queuable.attemptCount = this.attemptCount;
    queuable.policy = this.policy;
    if (this.payload) {
      queuable.payload = this.payload;
    }
    return queuable;
  }

  toString() {
    return `${this.type}@${this._trimUrl(this.url)}`;
  }

  toUniqueString() {
    const policyName = this.policy ? Request._getResolvedPolicy(this).getShortForm() : 'NN';
    return `${this.type}@${this.url}:${policyName}`;
  }

  _trimUrl(url) {
    return url ? url.replace('https://api.github.com', '') : '';
  }

  _log(level, message, meta = null) {
    if (this.crawler) {
      this.crawler.logger.log(level, message, meta);
    }
  }
}

module.exports = Request;