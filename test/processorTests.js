const assert = require('chai').assert;
const chai = require('chai');
const expect = require('chai').expect;
const Processor = require('../lib/processor.js');
const Request = require('../lib/request.js');
const sinon = require('sinon');
const TraversalPolicy = require('../lib/traversalPolicy');

describe('Processor reprocessing', () => {
  it('will skip if at same version', () => {
    const processor = new Processor();
    const request = new Request('user', 'http://test.com/users/user1');
    request.policy.freshness = 'version';
    request.document = { _metadata: { version: processor.version } };
    sinon.stub(processor, 'user', () => { });
    processor.process(request);
    expect(request.shouldSkip()).to.be.true;
    expect(processor.user.callCount).to.be.equal(0);
  });

  it('will skip and warn if at greater version', () => {
    const processor = new Processor();
    const request = new Request('user', 'http://test.com/users/user1');
    request.policy.freshness = 'version';
    request.document = { _metadata: { version: processor.version + 1 } };
    sinon.stub(processor, 'user', () => { });
    processor.process(request);
    expect(request.shouldSkip()).to.be.true;
    expect(request.outcome).to.be.equal('Excluded');
    expect(processor.user.callCount).to.be.equal(0);
  });

  it('will process and update if at lesser version', () => {
    const processor = new Processor();
    const request = new Request('user', 'http://test.com/users/user1');
    request.fetch = 'none';
    request.document = { _metadata: { version: processor.version - 1 } };
    sinon.stub(processor, 'user', () => { return request.document; });
    const document = processor.process(request);
    expect(request.shouldSkip()).to.be.false;
    expect(processor.user.callCount).to.be.equal(1);
    expect(document._metadata.version).to.be.equal(processor.version);
  });
});

describe('Collection processing', () => {
  it('should queue collection pages as deepShallow and elements as deepShallow', () => {
    const request = new Request('issues', 'http://test.com/issues', { elementType: 'issue' });
    request.policy.transitivity = 'deepShallow';
    request.response = {
      headers: { link: createLinkHeader(request.url, null, 2, 2) }
    };
    request.document = { _metadata: { links: {} }, elements: [{ type: 'issue', url: 'http://child1' }] };
    request.crawler = { queue: () => { }, queues: { push: () => { } } };
    sinon.spy(request.crawler, 'queue');
    const push = sinon.spy(request.crawler.queues, 'push');
    const processor = new Processor();

    processor.process(request);

    expect(request.crawler.queues.push.callCount).to.be.equal(1);
    expect(push.getCall(0).args[1]).to.be.equal('soon');
    const newPages = request.crawler.queues.push.getCall(0).args[0];
    expect(newPages.length).to.be.equal(1);
    expect(newPages[0].policy.transitivity).to.be.equal('deepShallow');
    expect(newPages[0].url).to.be.equal('http://test.com/issues?page=2&per_page=100');
    expect(newPages[0].type).to.be.equal('issues');

    expect(request.crawler.queue.callCount).to.be.equal(1);
    const newRequest = request.crawler.queue.getCall(0).args[0];
    expect(newRequest.policy.transitivity).to.be.equal('deepShallow');
    expect(newRequest.url).to.be.equal('http://child1');
    expect(newRequest.type).to.be.equal('issue');
  });

  it('should queue deepShallow root collections as deepShallow and elements as shallow', () => {
    const request = new Request('orgs', 'http://test.com/orgs', { elementType: 'org' });
    request.policy.transitivity = 'deepShallow';
    request.response = {
      headers: { link: createLinkHeader(request.url, null, 2, 2) }
    };
    request.document = { _metadata: { links: {} }, elements: [{ type: 'org', url: 'http://child1' }] };
    request.crawler = { queue: () => { }, queues: { push: () => { } } };
    sinon.spy(request.crawler, 'queue');
    const push = sinon.spy(request.crawler.queues, 'push');
    const processor = new Processor();

    processor.process(request);

    expect(push.callCount).to.be.equal(1);
    expect(push.getCall(0).args[1]).to.be.equal('soon');

    const newPages = push.getCall(0).args[0];
    expect(newPages.length).to.be.equal(1);
    expect(newPages[0].policy.transitivity).to.be.equal('deepShallow');
    expect(newPages[0].url).to.be.equal('http://test.com/orgs?page=2&per_page=100');
    expect(newPages[0].type).to.be.equal('orgs');

    expect(request.crawler.queue.callCount).to.be.equal(1);
    const newRequest = request.crawler.queue.getCall(0).args[0];
    expect(newRequest.policy.transitivity).to.be.equal('shallow');
    expect(newRequest.url).to.be.equal('http://child1');
    expect(newRequest.type).to.be.equal('org');
  });

  it('should queue forceForce root collection pages as forceForce and elements as forceNormal', () => {
    const request = new Request('orgs', 'http://test.com/orgs', { elementType: 'org' });
    request.policy = TraversalPolicy.update();
    request.response = {
      headers: { link: createLinkHeader(request.url, null, 2, 2) }
    };
    request.document = { _metadata: { links: {} }, elements: [{ type: 'org', url: 'http://child1' }] };
    request.crawler = { queue: () => { }, queues: { push: () => { } } };
    sinon.spy(request.crawler, 'queue');
    const push = sinon.spy(request.crawler.queues, 'push');
    const processor = new Processor();

    processor.process(request);

    expect(push.callCount).to.be.equal(1);
    expect(push.getCall(0).args[1]).to.be.equal('soon');
    const newPages = push.getCall(0).args[0];
    expect(newPages.length).to.be.equal(1);
    expect(newPages[0].policy.transitivity).to.be.equal('deepDeep');
    expect(newPages[0].url).to.be.equal('http://test.com/orgs?page=2&per_page=100');
    expect(newPages[0].type).to.be.equal('orgs');

    expect(request.crawler.queue.callCount).to.be.equal(1);
    const newRequest = request.crawler.queue.getCall(0).args[0];
    expect(newRequest.policy.transitivity).to.be.equal('deepShallow');
    expect(newRequest.url).to.be.equal('http://child1');
    expect(newRequest.type).to.be.equal('org');
  });

  it('should queue forceForce page elements with forceNormal transitivity', () => {
    const request = new Request('orgs', 'http://test.com/orgs?page=2&per_page=100', { elementType: 'org' });
    request.policy = TraversalPolicy.update();
    request.document = { _metadata: { links: {} }, elements: [{ url: 'http://child1' }] };
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    const processor = new Processor();

    processor.page(2, request);
    expect(request.crawler.queue.callCount).to.be.equal(1);
    const newRequest = request.crawler.queue.getCall(0).args[0];
    expect(newRequest.policy.transitivity).to.be.equal('deepShallow');
    expect(newRequest.url).to.be.equal('http://child1');
    expect(newRequest.type).to.be.equal('org');
  });
});

describe('URN building', () => {
  it('should create urn for team members', () => {
    const request = new Request('repo', 'http://test.com/foo');
    request.document = { _metadata: { links: {} }, id: 42, owner: { url: 'http://test.com/test' }, teams_url: 'http://test.com/teams', issues_url: 'http://test.com/issues', commits_url: 'http://test.com/commits', collaborators_url: 'http://test.com/collaborators' };
    request.crawler = { queue: () => { }, queues: { pushPriority: () => { } } };
    sinon.spy(request.crawler, 'queue');
    sinon.spy(request.crawler.queues, 'pushPriority');
    const processor = new Processor();

    processor.repo(request);
    expect(request.crawler.queue.callCount).to.be.at.least(4);
    const teamsRequest = request.crawler.queue.getCall(1).args[0];
    expect(teamsRequest.context.qualifier).to.be.equal('urn:repo:42');
    expect(!!teamsRequest.context.relation.guid).to.be.true;
    delete teamsRequest.context.relation.guid;
    expect(teamsRequest.context.relation).to.be.deep.equal({ origin: 'repo', name: 'teams', type: 'team' });

    request.crawler.queue.reset();
    teamsRequest.type = 'teams';
    teamsRequest.document = { _metadata: { links: {} }, elements: [{ id: 13, url: 'http://team1' }] };
    teamsRequest.crawler = request.crawler;
    const teamsPage = processor.process(teamsRequest);
    const links = teamsPage._metadata.links;
    expect(links.resources.type).to.be.equal('resource');
    expect(links.resources.hrefs.length).to.be.equal(1);
    expect(links.resources.hrefs[0]).to.be.equal('urn:team:13');
    expect(links.repo.type).to.be.equal('resource');
    expect(links.repo.href).to.be.equal('urn:repo:42');
    expect(links.origin.type).to.be.equal('resource');
    expect(links.origin.href).to.be.equal('urn:repo:42');

    const teamRequest = request.crawler.queue.getCall(0).args[0];
    expect(teamRequest.type).to.be.equal('team');
    expect(teamRequest.context.qualifier).to.be.equal('urn:');

    request.crawler.queue.reset();
    teamRequest.document = { _metadata: { links: {} }, id: 54, organization: { id: 87 }, members_url: "http://team1/members", repositories_url: "http://team1/repos" };
    teamRequest.crawler = request.crawler;
    processor.team(teamRequest);
    const membersRequest = request.crawler.queue.getCall(0).args[0];
    expect(membersRequest.url).to.be.equal('http://team1/members');
    expect(membersRequest.context.qualifier).to.be.equal('urn:team:54');
    expect(!!membersRequest.context.relation.guid).to.be.true;
    delete membersRequest.context.relation.guid;
    expect(membersRequest.context.relation).to.be.deep.equal({ name: 'members', origin: 'team', type: 'user' });
    const reposRequest = request.crawler.queue.getCall(1).args[0];
    expect(reposRequest.url).to.be.equal('http://team1/repos');
    expect(reposRequest.context.qualifier).to.be.equal('urn:team:54');
    expect(!!reposRequest.context.relation.guid).to.be.true;
    delete reposRequest.context.relation.guid;
    expect(reposRequest.context.relation).to.be.deep.equal({ name: 'repos', origin: 'team', type: 'repo' });
  });
});

describe('Pull Request processing', () => {
  it('should link and queue correctly', () => {
    const request = new Request('pull_request', 'http://foo/pull');
    request.context = { qualifier: 'urn:repo:12' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push(request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 13,
      assignee: { id: 1, url: 'http://user.1' },
      milestone: { id: 26 },
      head: { repo: { id: 45, url: 'http://repo.45' } },
      base: { repo: { id: 17, url: 'http://repo.17' } },
      _links: {
        issue: { href: 'http://issue.13' },
        review_comments: { href: 'http://review_comments' },
        commits: { href: 'http://commits' },
        statuses: { href: 'http://statuses/funkySHA' }
      },
      user: { id: 7, url: 'http://user.7' },
      merged_by: { id: 15, url: 'http://user.15' }
    };
    const processor = new Processor();
    const document = processor.pull_request(request);
    expect(queue.length).to.be.equal(9);

    expectSelfLink(document, 'pull_request', 13, 12);
    expectSiblingsLink(document, 'pull_requests', 12);

    expectRootAdded(queue, document, 'user', 'user', 7, 'resource');
    expectRootAdded(queue, document, 'merged_by', 'user', 15, 'resource');
    expectRootAdded(queue, document, 'assignee', 'user', 1, 'resource');
    expectRootAdded(queue, document, 'head', 'repo', 45, 'resource');
    expectRootAdded(queue, document, 'base', 'repo', 17, 'resource');

    expectCollectionAdded(queue, document, 'review_comments', 'review_comments', 13, 12, 'pull_request');
    expectCollectionAdded(queue, document, 'commits', 'commits', 13, 12, 'pull_request');

    expectLinkUrn(document, 'statuses', 'urn:repo:12:commits:funkySHA:statuses', 'collection');
    expect(queue.some(r => r.type === 'statuses' && r.url === 'http://statuses/funkySHA')).to.be.true;

    expectResourceAdded(queue, document, 'issue', 'issue', document.id, 12);
    expectLinkUrn(document, 'comments', 'urn:repo:12:issues:13:comments', 'collection');
  });
});

function expectLinkUrn(document, name, urn, linkType) {
  expect(document._metadata.links[name].href).to.be.equal(urn);
  expect(document._metadata.links[name].type).to.be.equal(linkType);
}

function expectRootLink(document, name, type, id, linkType) {
  expectLinkUrn(document, name, `urn:${type}:${id}`, linkType);
}

function expectRootAdded(queue, document, name, type, id, linkType) {
  expectRootLink(document, name, type, id, linkType);
  expect(queue.some(r => r.type === type && r.url === `http://${type}.${id}`)).to.be.true;
}

function expectChildLink(document, name, type, id, linkType, repoId) {
  expectLinkUrn(document, name, `urn:repo:${repoId}:${type}:${id}`, linkType);
}

function expectResourceAdded(queue, document, name, type, id, repoId) {
  expectChildLink(document, name, type, id, 'resource', repoId);
  expect(queue.some(r => r.type === type && r.url === `http://${type}.${id}`)).to.be.true;
}

function expectCollectionAdded(queue, document, name, type, id, repoId, parent) {
  expectLinkUrn(document, name, `urn:repo:${repoId}:${parent}:${id}:${type}`, 'collection');
  expect(queue.some(r => r.type === type && r.url === `http://${name}`)).to.be.true;
}

function expectSelfLink(document, type, id, repoId) {
  expectLinkUrn(document, 'self', `urn:repo:${repoId}:${type}:${id}`, 'resource');
}

function expectSiblingsLink(document, type, repoId) {
  expectLinkUrn(document, 'siblings', `urn:repo:${repoId}:${type}`, 'collection');
}

function createLinkHeader(target, previous, next, last) {
  const separator = target.includes('?') ? '&' : '?';
  const firstLink = null; //`<${urlHost}/${target}${separator}page=1>; rel="first"`;
  const prevLink = previous ? `<${target}${separator}page=${previous}>; rel="prev"` : null;
  const nextLink = next ? `<${target}${separator}page=${next}>; rel="next"` : null;
  const lastLink = last ? `<${target}${separator}page=${last}>; rel="last"` : null;
  return [firstLink, prevLink, nextLink, lastLink].filter(value => { return value !== null; }).join(',');
}