const assert = require('chai').assert;
const chai = require('chai');
const expect = require('chai').expect;
const Processor = require('../lib/processor.js');
const Request = require('../lib/request.js');
const sinon = require('sinon');

describe('Processor reprocessing', () => {
  it('will skip if at same version', () => {
    const processor = new Processor();
    const request = new Request('user', 'http://test.com/users/user1');
    request.fetch = 'none';
    request.document = { _metadata: { version: processor.version } };
    sinon.stub(processor, 'user', () => { });
    processor.process(request);
    expect(request.shouldSkip()).to.be.true;
    expect(processor.user.callCount).to.be.equal(0);
  });

  it('will skip and warn if at greater version', () => {
    const processor = new Processor();
    const request = new Request('user', 'http://test.com/users/user1');
    request.fetch = 'none';
    request.document = { _metadata: { version: processor.version + 1 } };
    sinon.stub(processor, 'user', () => { });
    processor.process(request);
    expect(request.shouldSkip()).to.be.true;
    expect(request.outcome).to.be.equal('Warn');
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
  it('should queue forceNormal normal collection pages as forceNormal and elements as forceNormal', () => {
    const request = new Request('issues', 'http://test.com/issues');
    request.transitivity = 'forceNormal';
    request.subType = 'issue';
    request.response = {
      headers: { link: createLinkHeader(request.url, null, 2, 2) }
    };
    request.document = { _metadata: { links: {} }, elements: [{ type: 'issue', url: 'http://child1' }] };
    request.crawler = { queue: () => { }, queues: { pushPriority: () => { } } };
    sinon.spy(request.crawler, 'queue');
    sinon.spy(request.crawler.queues, 'pushPriority');
    const processor = new Processor();

    processor.collection(request);

    expect(request.crawler.queues.pushPriority.callCount).to.be.equal(1);
    const newPages = request.crawler.queues.pushPriority.getCall(0).args[0];
    expect(newPages.length).to.be.equal(1);
    expect(newPages[0].transitivity).to.be.equal('forceNormal');
    expect(newPages[0].url).to.be.equal('http://test.com/issues?page=2&per_page=100');
    expect(newPages[0].type).to.be.equal('page');
    expect(newPages[0].subType).to.be.equal('issue');

    expect(request.crawler.queue.callCount).to.be.equal(1);
    const newRequest = request.crawler.queue.getCall(0).args[0];
    expect(newRequest.transitivity).to.be.equal('forceNormal');
    expect(newRequest.url).to.be.equal('http://child1');
    expect(newRequest.type).to.be.equal('issue');
  });

  it('should queue forceNormal root collection pages as forceNormal and elements as normal', () => {
    const request = new Request('collection', 'http://test.com/orgs');
    request.transitivity = 'forceNormal';
    request.subType = 'org';
    request.response = {
      headers: { link: createLinkHeader(request.url, null, 2, 2) }
    };
    request.document = { _metadata: { links: {} }, elements: [{ type: 'org', url: 'http://child1' }] };
    request.crawler = { queue: () => { }, queues: { pushPriority: () => { } } };
    sinon.spy(request.crawler, 'queue');
    sinon.spy(request.crawler.queues, 'pushPriority');
    const processor = new Processor();

    processor.collection(request);

    expect(request.crawler.queues.pushPriority.callCount).to.be.equal(1);
    const newPages = request.crawler.queues.pushPriority.getCall(0).args[0];
    expect(newPages.length).to.be.equal(1);
    expect(newPages[0].transitivity).to.be.equal('forceNormal');
    expect(newPages[0].url).to.be.equal('http://test.com/orgs?page=2&per_page=100');
    expect(newPages[0].type).to.be.equal('page');
    expect(newPages[0].subType).to.be.equal('org');

    expect(request.crawler.queue.callCount).to.be.equal(1);
    const newRequest = request.crawler.queue.getCall(0).args[0];
    expect(newRequest.transitivity).to.be.equal('normal');
    expect(newRequest.url).to.be.equal('http://child1');
    expect(newRequest.type).to.be.equal('org');
  });

  it('should queue forceForce root collection pages as forceForce and elements as forceNormal', () => {
    const request = new Request('collection', 'http://test.com/orgs');
    request.transitivity = 'forceForce';
    request.subType = 'org';
    request.response = {
      headers: { link: createLinkHeader(request.url, null, 2, 2) }
    };
    request.document = { _metadata: { links: {} }, elements: [{ type: 'org', url: 'http://child1' }] };
    request.crawler = { queue: () => { }, queues: { pushPriority: () => { } } };
    sinon.spy(request.crawler, 'queue');
    sinon.spy(request.crawler.queues, 'pushPriority');
    const processor = new Processor();

    processor.collection(request);

    expect(request.crawler.queues.pushPriority.callCount).to.be.equal(1);
    const newPages = request.crawler.queues.pushPriority.getCall(0).args[0];
    expect(newPages.length).to.be.equal(1);
    expect(newPages[0].transitivity).to.be.equal('forceForce');
    expect(newPages[0].url).to.be.equal('http://test.com/orgs?page=2&per_page=100');
    expect(newPages[0].type).to.be.equal('page');
    expect(newPages[0].subType).to.be.equal('org');

    expect(request.crawler.queue.callCount).to.be.equal(1);
    const newRequest = request.crawler.queue.getCall(0).args[0];
    expect(newRequest.transitivity).to.be.equal('forceNormal');
    expect(newRequest.url).to.be.equal('http://child1');
    expect(newRequest.type).to.be.equal('org');
  });

  it('should queue forceForce page elements with forceNormal transitivity', () => {
    const request = new Request('page', 'http://test.com/orgs?page=2&per_page=100');
    request.transitivity = 'forceForce';
    request.subType = 'org';
    request.document = { _metadata: { links: {} }, elements: [{ url: 'http://child1' }] };
    request.crawler = { queue: () => { } };
    sinon.spy(request.crawler, 'queue');
    const processor = new Processor();

    processor.page(request);
    expect(request.crawler.queue.callCount).to.be.equal(1);
    const newRequest = request.crawler.queue.getCall(0).args[0];
    expect(newRequest.transitivity).to.be.equal('forceNormal');
    expect(newRequest.url).to.be.equal('http://child1');
    expect(newRequest.type).to.be.equal('org');
  });

});


function createLinkHeader(target, previous, next, last) {
  separator = target.includes('?') ? '&' : '?';
  const firstLink = null; //`<${urlHost}/${target}${separator}page=1>; rel="first"`;
  const prevLink = previous ? `<${target}${separator}page=${previous}>; rel="prev"` : null;
  const nextLink = next ? `<${target}${separator}page=${next}>; rel="next"` : null;
  const lastLink = last ? `<${target}${separator}page=${last}>; rel="last"` : null;
  return [firstLink, prevLink, nextLink, lastLink].filter(value => { return value !== null; }).join(',');
}