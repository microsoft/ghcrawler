const extend = require('extend');

class Request {
  constructor(type, url) {
    this.type = type;
    this.url = url;
  }

  addMeta(data) {
    this.meta = extend({}, this.meta, data);
    return this;
  }

  addSelfLink(base = null, key = 'id') {
    let qualifier = base ? base : this.context.qualifier;
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

  queue(type, url, context, queue = null) {
    const newRequest = new Request(type, url);
    newRequest.context = context;
    this.crawler.queue(this, newRequest, queue);
  }

  queueRoot(type, url) {
    this.crawler.queue(this, new Request(type, url));
  }

  queueChild(type, url, qualifier) {
    const newRequest = new Request(type, url);
    newRequest.context = this.context || {};
    newRequest.context.qualifier = qualifier;
    if (this.force) {
      newRequest.force = this.force;
    }
    this.crawler.queue(this, newRequest);
  }

  queueChildren(type, url, context = null) {
    const newRequest = new Request(type, url);
    const newContext = extend(this.context || {}, context);
    newRequest.context = newContext;
    newContext.qualifier = this.document._metadata.links.self.href;
    if (this.force) {
      newRequest.force = this.force;
    }
    this.crawler.queue(this, newRequest);
  }

  markSkip(outcome, message) {
    this.processControl = 'skip';
    this.outcome = this.outcome || outcome;
    this.message = this.message || message;
    return this;
  }

  markRequeue(outcome, message) {
    this.processControl = 'requeue';
    this.outcome = this.outcome || outcome;
    this.message = this.message || message;
    return this;
  }

  shouldSkip() {
    return this.processControl === 'skip' || this.processControl === 'requeue';
  }

  markDelay() {
    this.flowControl = 'delay';
    return this;
  }

  shouldDelay() {
    return this.flowControl === 'delay';
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
      orgs: 'org', repos: 'repo', issues: 'issue', issue_comments: 'issue_comment', commits: 'commit', teams: 'team', users: 'user'
    };
    return collections[this.type];
  }
}

module.exports = Request;