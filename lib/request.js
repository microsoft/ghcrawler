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

  addRootSelfLink() {
    this.addSelfLink('id', 'urn:');
  }

  addSelfLink(key = 'id', base = null) {
    let qualifier = base ? base : this.context.qualifier;
    if (!qualifier || (typeof qualifier !== 'string' )) {
      console.log('bummer');
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

  queue(type, url, context, queue = null) {
    const newRequest = new Request(type, url);
    newRequest.context = context;
    this.crawler.queue(this, newRequest, queue);
  }

  queueRoot(type, url, force = false) {
    const newRequest = new Request(type, url);
    newRequest.context = { qualifier: 'urn:' };
    newRequest.force = force;
    this.crawler.queue(this, newRequest);
  }

  queueRoots(type, url, force = false) {
    const newRequest = new Request(type, url);
    const newContext = {};
    newContext.qualifier = this.document._metadata.links.self.href;
    newRequest.context = newContext;
    newRequest.force = force;
    this.crawler.queue(this, newRequest);
  }

  queueChild(type, url, qualifier) {
    const newRequest = new Request(type, url);
    newRequest.context = this.context || {};
    newRequest.context.qualifier = qualifier;
    newRequest.force = this.force;
    this.crawler.queue(this, newRequest);
  }

  queueChildren(type, url, context = null) {
    const newRequest = new Request(type, url);
    const newContext = extend(this.context || {}, context);
    newContext.qualifier = this.document._metadata.links.self.href;
    newRequest.context = newContext;
    newRequest.force = this.force;
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