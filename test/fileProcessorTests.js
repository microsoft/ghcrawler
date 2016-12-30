const assert = require('chai').assert;
const chai = require('chai');
const expect = require('chai').expect;
const FileProcessor = require('../lib/fileProcessor');
const Q = require('q');
const Request = require('../lib/request.js');
const sinon = require('sinon');
const TraversalPolicy = require('../lib/traversalPolicy');
const moment = require('moment');

const testHelpers = require('./processorTestHelpers');
const expectLinks = testHelpers.expectLinks;
const expectQueued = testHelpers.expectQueued;

describe('File Processing', () => {
  it('will process matching files included in commit', () => {
    const request = new Request('commit', 'http://foo/commit');
    request.context = { qualifier: 'urn:repo:12' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push(request) }) };
    request.document = {
      _metadata: { links: {} },
      sha: '6dcb09b5b5',
      url: 'http://repo/12/commits/6dcb09b5b5',
      comments_url: 'http://comments',
      author: { id: 7, url: 'http://user/7' },
      committer: { id: 15, url: 'http://user/15' },
      files: [
        {
          filename: 'testfile.txt',
          contents_url: 'https://url1'
        },
        {
          filename: 'msmetadata.yaml',
          contents_url:'https://url2'
        }
      ]
    };
    const processor = new FileProcessor(null, ['msmetadata.yaml']);
    const document = processor.commit(request);

    const links = {
      self: { href: 'urn:repo:12:commit:6dcb09b5b5', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const queued = [
      { type: 'file', url: 'https://url2' },
    ];
    expectQueued(queue, queued);
  });

  it('will process files that are watched', () => {
    const request = new Request('files', 'http://foo/repoowner/reponame');
    request.context = { qualifier: 'urn:repo:12' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push(request) }) };
    request.document = {
      _metadata: { links: {} },
      id:123456789,
      url: 'http://repo/12'
    };
    const processor = new FileProcessor(null, ['msmetadata.yaml', 'otherfile.json']);
    const document = processor.files(request);

    expect(document).to.be.null;

    const queued = [
      { type: 'file', url: 'http://foo/repoowner/reponame/contents/msmetadata.yaml' },
      { type: 'file', url: 'http://foo/repoowner/reponame/contents/otherfile.json' }
    ];
    expectQueued(queue, queued);
  });

  it('will process file', () => {
    const request = new Request('file', 'http://foo/file');
    request.context = { qualifier: 'urn:repo:12:commit:6dcb09b5b5' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push(request) }) };
    request.document = {
      _metadata: { links: {}, fetchedAt: '1970-01-01' },
      sha: '6dcb09b5b5',
      name: 'msmetadata.yaml'
    };
    const processor = new FileProcessor(null, ['msmetadata.yaml']);
    const document = processor.file(request);
    const links = {
      self: { href: 'urn:repo:12:commit:6dcb09b5b5:file:msmetadata.yaml', type: 'resource' },
      siblings: { href: 'urn:repo:12:commit:6dcb09b5b5:file', type: 'collection' }
    }
    expectLinks(document._metadata.links, links);
  });
})