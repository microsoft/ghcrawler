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
    const membersRequest = request.crawler.queue.getCall(1).args[0];
    expect(membersRequest.url).to.be.equal('http://team1/members');
    expect(membersRequest.context.qualifier).to.be.equal('urn:team:54');
    expect(!!membersRequest.context.relation.guid).to.be.true;
    delete membersRequest.context.relation.guid;
    expect(membersRequest.context.relation).to.be.deep.equal({ name: 'members', origin: 'team', type: 'user' });
    const reposRequest = request.crawler.queue.getCall(2).args[0];
    expect(reposRequest.url).to.be.equal('http://team1/repos');
    expect(reposRequest.context.qualifier).to.be.equal('urn:team:54');
    expect(!!reposRequest.context.relation.guid).to.be.true;
    delete reposRequest.context.relation.guid;
    expect(reposRequest.context.relation).to.be.deep.equal({ name: 'repos', origin: 'team', type: 'repo' });
  });
});

describe('Org processing', () => {
  it('should link and queue correctly', () => {
    const request = new Request('org', 'http://org/9');
    request.context = {  };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push(request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 9,
      url: 'http://orgs/9',
      repos_url: 'http://repos',
      members_url: 'http://members{/member}'
    };

    const processor = new Processor();
    const document = processor.org(request);

    const links = {
      self: { href: 'urn:org:9', type: 'resource' },
      siblings: { href: 'urn:orgs', type: 'collection' },
      user: { href: 'urn:user:9', type: 'resource' },
      repos: { href: 'urn:user:9:repos', type: 'collection' },
      members: { href: 'urn:org:9:members:pages:*', type: 'relation' },
    }
    expectLinks(document._metadata.links, links);

    const queued = [
      { type: 'user', url: 'http://users/9' },
      { type: 'repos', url: 'http://repos' },
      { type: 'members', url: 'http://members' }
    ];
    expectQueued(queue, queued);
  });
});

describe('User processing', () => {
  it('should link and queue correctly', () => {
    const request = new Request('user', 'http://user/9');
    request.context = {  };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push(request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 9,
      repos_url: 'http://repos',
    };

    const processor = new Processor();
    const document = processor.user(request);

    const links = {
      self: { href: 'urn:user:9', type: 'resource' },
      siblings: { href: 'urn:users', type: 'collection' },
      repos: { href: 'urn:user:9:repos', type: 'collection' }
    }
    expectLinks(document._metadata.links, links);

    const queued = [
      { type: 'repos', url: 'http://repos' }
    ];
    expectQueued(queue, queued);
  });
});

describe('Repo processing', () => {
  it('should link and queue correctly', () => {
    const request = new Request('repo', 'http://foo/repo/12');
    request.context = {  };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push(request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 12,
      owner: { id: 45, url: 'http://user/45' },
      collaborators_url: 'http://collaborators{/collaborator}',
      commits_url: 'http://commits{/sha}',
      contributors_url: 'http://contributors',
      issues_url: 'http://issues{/number}',
      pulls_url: 'http://pulls{/number}',
      subscribers_url: 'http://subscribers',
      teams_url: 'http://teams',
      organization: { id: 24, url: 'http://org/24' },
    };

    const processor = new Processor();
    const document = processor.repo(request);

    const links = {
      self: { href: 'urn:repo:12', type: 'resource' },
      siblings: { href: 'urn:user:45:repos', type: 'collection' },
      owner: { href: 'urn:user:45', type: 'resource' },
      organization: { href: 'urn:org:24', type: 'resource' },
      pull_requests: { href: 'urn:repo:12:pull_requests', type: 'collection' },
      teams: { href: 'urn:repo:12:teams:pages:*', type: 'relation' },
      collaborators: { href: 'urn:repo:12:collaborators:pages:*', type: 'relation' },
      contributors: { href: 'urn:repo:12:contributors:pages:*', type: 'relation' },
      subscribers: { href: 'urn:repo:12:subscribers:pages:*', type: 'relation' },
      commits: { href: 'urn:repo:12:commits', type: 'collection' },
      issues: { href: 'urn:repo:12:issues', type: 'collection' },
    }
    expectLinks(document._metadata.links, links);

    const queued = [
      { type: 'user', url: 'http://user/45' },
      { type: 'org', url: 'http://org/24' },
      { type: 'teams', url: 'http://teams' },
      { type: 'collaborators', url: 'http://collaborators' },
      { type: 'contributors', url: 'http://contributors' },
      { type: 'subscribers', url: 'http://subscribers' },
      { type: 'issues', url: 'http://issues' },
      { type: 'commits', url: 'http://commits' },
    ];
    expectQueued(queue, queued);
  });
});

describe('Commit processing', () => {
  it('should link and queue correctly', () => {
    const request = new Request('commit', 'http://foo/commit');
    request.context = { qualifier: 'urn:repo:12' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push(request) }) };
    request.document = {
      _metadata: { links: {} },
      sha: '6dcb09b5b5',
      url: 'http://repo/12/commits/6dcb09b5b5',
      author: { id: 7, url: 'http://user/7' },
      committer: { id: 15, url: 'http://user/15' }
    };
    const processor = new Processor();
    const document = processor.commit(request);

    const links = {
      self: { href: 'urn:repo:12:commit:6dcb09b5b5', type: 'resource' },
      siblings: { href: 'urn:repo:12:commits', type: 'collection' },
      author: { href: 'urn:user:7', type: 'resource' },
      committer: { href: 'urn:user:15', type: 'resource' },
      repo: { href: 'urn:repo:12', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const queued = [
      { type: 'user', url: 'http://user/7' },
      { type: 'user', url: 'http://user/15' },
      { type: 'repo', url: 'http://repo/12' }
    ];
    expectQueued(queue, queued);
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
      assignee: { id: 1, url: 'http://user/1' },
      milestone: { id: 26 },
      head: { repo: { id: 45, url: 'http://repo/45' } },
      base: { repo: { id: 17, url: 'http://repo/17' } },
      _links: {
        issue: { href: 'http://issue/13' },
        review_comments: { href: 'http://review_comments' },
        commits: { href: 'http://commits' },
        statuses: { href: 'http://statuses/funkySHA' }
      },
      user: { id: 7, url: 'http://user/7' },
      merged_by: { id: 15, url: 'http://user/15' }
    };
    const processor = new Processor();
    const document = processor.pull_request(request);

    const links = {
      self: { href: 'urn:repo:12:pull_request:13', type: 'resource' },
      siblings: { href: 'urn:repo:12:pull_requests', type: 'collection' },
      user: { href: 'urn:user:7', type: 'resource' },
      merged_by: { href: 'urn:user:15', type: 'resource' },
      assignee: { href: 'urn:user:1', type: 'resource' },
      head: { href: 'urn:repo:45', type: 'resource' },
      base: { href: 'urn:repo:17', type: 'resource' },
      review_comments: { href: 'urn:repo:12:pull_request:13:review_comments', type: 'collection' },
      commits: { href: 'urn:repo:12:pull_request:13:commits', type: 'collection' },
      statuses: { href: 'urn:repo:12:commit:funkySHA:statuses', type: 'collection' },
      issue: { href: 'urn:repo:12:issue:13', type: 'resource' },
      issue_comments: { href: 'urn:repo:12:issue:13:issue_comments', type: 'collection' },
    }
    expectLinks(document._metadata.links, links);

    const queued = [
      { type: 'user', url: 'http://user/7' },
      { type: 'user', url: 'http://user/15' },
      { type: 'user', url: 'http://user/1' },
      { type: 'repo', url: 'http://repo/45' },
      { type: 'repo', url: 'http://repo/17' },
      { type: 'review_comments', url: 'http://review_comments' },
      { type: 'commits', url: 'http://commits' },
      { type: 'statuses', url: 'http://statuses/funkySHA' },
      { type: 'issue', url: 'http://issue/13' }
    ];
    expectQueued(queue, queued);
  });
});

describe('Pull request/review comment processing', () => {
  it('should link and queue correctly', () => {
    const request = new Request('review_comment', 'http://repo/pull_request/comment');
    request.context = { qualifier: 'urn:repo:12:pull_request:27' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push(request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 37,
      user: { id: 7, url: 'http://user/7' }
    };
    const processor = new Processor();
    const document = processor.review_comment(request);

    const links = {
      self: { href: 'urn:repo:12:pull_request:27:review_comment:37', type: 'resource' },
      siblings: { href: 'urn:repo:12:pull_request:27:review_comments', type: 'collection' },
      user: { href: 'urn:user:7', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const queued = [
      { type: 'user', url: 'http://user/7' },
    ];
    expectQueued(queue, queued);
  });
});

describe('Issue processing', () => {
  it('should link and queue correctly', () => {
    const request = new Request('issue', 'http://repo/issue');
    request.context = { qualifier: 'urn:repo:12' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push(request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 27,
      assignee: { id: 1, url: 'http://user/1' },
      assignees: [{ id: 50 }, { id: 51 }],
      milestone: { id: 26 },
      repo: { id: 45, url: 'http://repo/45' },
      comments_url: 'http://issue/27/comments',
      pull_request: { url: 'http://pull_request/27' },
      user: { id: 7, url: 'http://user/7' },
      closed_by: { id: 15, url: 'http://user/15' }
    };
    const processor = new Processor();
    const document = processor.issue(request);

    const links = {
      self: { href: 'urn:repo:12:issue:27', type: 'resource' },
      siblings: { href: 'urn:repo:12:issues', type: 'collection' },
      user: { href: 'urn:user:7', type: 'resource' },
      closed_by: { href: 'urn:user:15', type: 'resource' },
      assignee: { href: 'urn:user:1', type: 'resource' },
      repo: { href: 'urn:repo:12', type: 'resource' },
      assignees: { hrefs: ['urn:user:50', 'urn:user:51'], type: 'resource' },
      issue_comments: { href: 'urn:repo:12:issue:27:issue_comments', type: 'collection' },
      pull_request: { href: 'urn:repo:12:pull_request:27', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const queued = [
      { type: 'user', url: 'http://user/7' },
      { type: 'user', url: 'http://user/15' },
      { type: 'user', url: 'http://user/1' },
      { type: 'repo', url: 'http://repo/45' },
      { type: 'issue_comments', url: 'http://issue/27/comments' },
      { type: 'pull_request', url: 'http://pull_request/27' }
    ];
    expectQueued(queue, queued);
  });
});

describe('Issue comment processing', () => {
  it('should link and queue correctly', () => {
    const request = new Request('issue_comment', 'http://repo/issue/comment');
    request.context = { qualifier: 'urn:repo:12:issue:27' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push(request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 37,
      user: { id: 7, url: 'http://user/7' }
    };
    const processor = new Processor();
    const document = processor.issue_comment(request);

    const links = {
      self: { href: 'urn:repo:12:issue:27:issue_comment:37', type: 'resource' },
      siblings: { href: 'urn:repo:12:issue:27:issue_comments', type: 'collection' },
      user: { href: 'urn:user:7', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const queued = [
      { type: 'user', url: 'http://user/7' },
    ];
    expectQueued(queue, queued);
  });
});

describe('Team processing', () => {
  it('should link and queue correctly', () => {
    const request = new Request('team', 'http://team/66');
    request.context = { qualifier: 'urn' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push(request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 66,
      members_url: 'http://teams/66/members{/member}',
      repositories_url: 'http://teams/66/repos',
      organization: { id: 9, url: 'http://orgs/9'}
    };
    const processor = new Processor();
    const document = processor.team(request);

    const links = {
      self: { href: 'urn:team:66', type: 'resource' },
      siblings: { href: 'urn:org:9:teams', type: 'collection' },
      organization: { href: 'urn:org:9', type: 'resource' },
      members: { href: 'urn:team:66:members:pages:*', type: 'relation' },
      repos: { href: 'urn:team:66:repos:pages:*', type: 'relation' },
    }
    expectLinks(document._metadata.links, links);

    const queued = [
      { type: 'org', url: 'http://orgs/9' },
      { type: 'repos', url: 'http://teams/66/repos' },
      { type: 'members', url: 'http://teams/66/members' }
    ];
    expectQueued(queue, queued);
  });
});

function expectLinks(actual, expected) {
  expect(Object.getOwnPropertyNames(actual).length).to.be.equal(Object.getOwnPropertyNames(expected).length);
  Object.getOwnPropertyNames(actual).forEach(name => {
    const expectedElement = expected[name];
    const actualElement = actual[name];
    expect(actualElement.type).to.be.equal(expectedElement.type);
    if (expectedElement.hrefs) {
      expect(actualElement.hrefs).to.be.deep.equal(expectedElement.hrefs);
    } else if (expectedElement.href.endsWith('*')) {
      expect(actualElement.href.startsWith(expectedElement.href.slice(0, -1))).to.be.true;
    } else {
      expect(actualElement.href).to.be.equal(expectedElement.href);
    }
  });
}

function expectQueued(actual, expected) {
  expect(actual.length).to.be.equal(expected.length);
  actual.forEach(element => {
    expect(expected.some(r => r.type === element.type && r.url === element.url)).to.be.true;
  })
}






function expectLinkUrn(document, name, urn, linkType, startsWith = false) {
  if (startsWith) {
    expect(document._metadata.links[name].href.startsWith(urn)).to.be.true;
  } else {
    expect(document._metadata.links[name].href).to.be.equal(urn);
  }
  expect(document._metadata.links[name].type).to.be.equal(linkType);
}

function expectRootLink(document, name, type, id, linkType) {
  expectLinkUrn(document, name, `urn:${type}:${id}`, linkType);
}

function expectRootAdded(queue, document, name, type, id, linkType) {
  expectRootLink(document, name, type, id, linkType);
  expect(queue.some(r => r.type === type && r.url === `http://${type}/${id}`)).to.be.true;
}

function expectChildLink(document, name, type, id, linkType, repoId) {
  expectLinkUrn(document, name, `urn:repo:${repoId}:${type}:${id}`, linkType);
}

function expectResourceAdded(queue, document, name, type, id, repoId) {
  expectChildLink(document, name, type, id, 'resource', repoId);
  expect(queue.some(r => r.type === type && r.url === `http://${type}/${id}`)).to.be.true;
}

function expectRelationAdded(queue, document, name, type, repoId) {
  expectLinkUrn(document, name, `urn:repo:${repoId}:${name}:pages:`, 'relation', true);
  expect(queue.some(r => r.type === type && r.url === `http://${name}`)).to.be.true;
}

function expectCollectionAdded(queue, document, name, type, id, repoId = null, parent = null) {
  if (!repoId) {
    urn = `urn:repo:${repoId}:${parent}:${id}:${type}`;
  }
  expectLinkUrn(document, name, `urn:repo:${repoId}:${parent}:${id}:${type}`, 'collection');
  expect(queue.some(r => r.type === type && r.url === `http://${name}`)).to.be.true;
}

function expectSelfLink(document, type, id, repoId) {
  expectLinkUrn(document, 'self', `urn:repo:${repoId}:${type}:${id}`, 'resource');
}

function expectRootSelfLink(document, type, id) {
  expectLinkUrn(document, 'self', `urn:${type}:${id}`, 'resource');
}

function expectSiblingsLink(document, type, repoId) {
  expectLinkUrn(document, 'siblings', `urn:repo:${repoId}:${type}`, 'collection');
}

function expectRootSiblingsLink(document, parentType, parentId, name) {
  expectLinkUrn(document, 'siblings', `urn:${parentType}:${parentId}:${name}`, 'collection');
}

function createLinkHeader(target, previous, next, last) {
  const separator = target.includes('?') ? '&' : '?';
  const firstLink = null; //`<${urlHost}/${target}${separator}page=1>; rel="first"`;
  const prevLink = previous ? `<${target}${separator}page=${previous}>; rel="prev"` : null;
  const nextLink = next ? `<${target}${separator}page=${next}>; rel="next"` : null;
  const lastLink = last ? `<${target}${separator}page=${last}>; rel="last"` : null;
  return [firstLink, prevLink, nextLink, lastLink].filter(value => { return value !== null; }).join(',');
}