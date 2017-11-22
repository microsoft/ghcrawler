// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const expect = require('chai').expect;
const GitHubProcessor = require('../../providers/fetcher/githubProcessor.js');
const Q = require('q');
const Request = require('../../lib/request.js');
const sinon = require('sinon');
const TraversalPolicy = require('../../lib/traversalPolicy');

describe('GitHubProcessor reprocessing', () => {
  it('will skip if at same version', () => {
    const processor = new GitHubProcessor();
    const request = createRequest('user', 'http://test.com/users/user1');
    request.policy = TraversalPolicy.reprocess('user');
    request.document = { _metadata: { version: processor.version } };
    sinon.stub(processor, 'user', () => { });
    processor.process(request);
    expect(request.processMode === 'traverse').to.be.true;
    expect(processor.user.callCount).to.be.equal(1);
  });

  it('will skip and warn if at greater version', () => {
    const processor = new GitHubProcessor();
    const request = createRequest('user', 'http://test.com/users/user1');
    request.policy = TraversalPolicy.reprocess('user');
    request.document = { _metadata: { version: processor.version + 1 } };
    sinon.stub(processor, 'user', () => { });
    processor.process(request);
    expect(request.processMode === 'traverse').to.be.true;
    expect(request.outcome).to.be.equal('Traversed');
    expect(processor.user.callCount).to.be.equal(1);
  });

  it('will process and update if at lesser version', () => {
    const processor = new GitHubProcessor();
    const request = createRequest('user', 'http://test.com/users/user1');
    request.policy = TraversalPolicy.reprocess('user');
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
  it('should queue collection pages as broad and elements as broad', () => {
    const request = createRequest('issues', 'http://test.com/issues', { elementType: 'issue' });
    request.policy = TraversalPolicy.refresh('repo@issues');
    request.policy.freshness = 'always';
    request.response = {
      headers: { link: createLinkHeader(request.url, null, 2, 2) }
    };
    request.document = { _metadata: { links: {} }, elements: [{ type: 'issue', url: 'http://child1' }] };
    request.crawler = { queue: () => { } };
    const queue = sinon.spy(request.crawler, 'queue');
    const processor = new GitHubProcessor();

    processor.process(request);

    expect(queue.callCount).to.be.equal(2);
    expect(queue.getCall(0).args[1]).to.be.equal('soon');
    const newPages = queue.getCall(0).args[0];
    expect(newPages.length).to.be.equal(1);
    expect(newPages[0].url).to.be.equal('http://test.com/issues?page=2&per_page=100');
    expect(newPages[0].type).to.be.equal('issues');

    let newRequest = queue.getCall(1).args[0];
    expect(newRequest.length).to.be.equal(1);
    newRequest = newRequest[0];
    expect(newRequest.url).to.be.equal('http://child1');
    expect(newRequest.type).to.be.equal('issue');
  });
});

describe('URN building', () => {
  it('should create urn for team members', () => {
    const request = createRequest('repo', 'http://test.com/foo');
    request.policy = TraversalPolicy.refresh('repo');
    request.policy.freshness = 'always';
    request.document = { _metadata: { links: {} }, id: 42, owner: { url: 'http://test.com/test' }, teams_url: 'http://test.com/teams', issues_url: 'http://test.com/issues', commits_url: 'http://test.com/commits', collaborators_url: 'http://test.com/collaborators' };
    request.crawler = { queue: () => { }, queues: { pushPriority: () => { } } };
    const queue = sinon.spy(request.crawler, 'queue');
    // sinon.spy(request.crawler.queues, 'pushPriority');
    const processor = new GitHubProcessor();

    processor.repo(request);
    expect(queue.callCount).to.be.at.least(4);
    const teamsRequest = queue.getCall(1).args[0][0];
    expect(teamsRequest.context.qualifier).to.be.equal('urn:repo:42');
    expect(!!teamsRequest.context.relation.guid).to.be.true;
    delete teamsRequest.context.relation.guid;
    expect(teamsRequest.context.relation).to.be.deep.equal({ origin: 'repo', qualifier: 'urn:repo:42:teams', type: 'team' });

    queue.reset();
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

    expect(queue.callCount).to.be.equal(1);
    const teamRequest = queue.getCall(0).args[0][0];
    expect(teamRequest.type).to.be.equal('team');
    expect(teamRequest.context.qualifier).to.be.equal('urn:');

    // queue.reset();
    // teamRequest.document = { _metadata: { links: {} }, id: 54, organization: { id: 87 }, members_url: "http://team1/members", repositories_url: "http://team1/repos" };
    // teamRequest.crawler = request.crawler;
    // processor.team(teamRequest);
    // const membersRequest = queue.getCall(1).args[0][0];
    // expect(membersRequest.url).to.be.equal('http://team1/members');
    // expect(membersRequest.context.qualifier).to.be.equal('urn:team:54');
    // expect(!!membersRequest.context.relation.guid).to.be.true;
    // delete membersRequest.context.relation.guid;
    // expect(membersRequest.context.relation).to.be.deep.equal({ qualifier: 'urn:team:54:team_members', origin: 'team', type: 'user' });
    // const reposRequest = queue.getCall(2).args[0][0];
    // expect(reposRequest.url).to.be.equal('http://team1/repos');
    // expect(reposRequest.context.qualifier).to.be.equal('urn:team:54');
    // expect(!!reposRequest.context.relation.guid).to.be.true;
    // delete reposRequest.context.relation.guid;
    // expect(reposRequest.context.relation).to.be.deep.equal({ qualifier: 'urn:team:54:repos', origin: 'team', type: 'repo' });
  });
});

describe('Org processing', () => {
  it('should link and queue correctly', () => {
    const request = createRequest('org', 'http://org/9');
    request.context = {};
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 9,
      url: 'http://orgs/9',
      repos_url: 'http://repos',
      members_url: 'http://members{/member}'
    };

    const processor = new GitHubProcessor();
    const document = processor.org(request);

    const links = {
      self: { href: 'urn:org:9', type: 'resource' },
      siblings: { href: 'urn:orgs', type: 'collection' },
      user: { href: 'urn:user:9', type: 'resource' },
      repos: { href: 'urn:user:9:repos', type: 'collection' },
      members: { href: 'urn:org:9:org_members:pages:*', type: 'relation' },
      teams: { href: 'urn:org:9:org_teams:pages:*', type: 'relation' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://users/9', path: '/user' },
      { type: 'repos', url: 'http://repos', qualifier: 'urn:org:9', path: '/repos' },
      { type: 'members', url: 'http://members', qualifier: 'urn:org:9', path: '/members' },
      { type: 'teams', url: 'http://orgs/9/teams', qualifier: 'urn:org:9', path: '/teams' }
    ];
    expectQueued(queue, expected);
  });
});

describe('User processing', () => {
  it('should link and queue correctly', () => {
    const request = createRequest('user', 'http://user/9');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 9,
      repos_url: 'http://repos',
    };

    const processor = new GitHubProcessor();
    const document = processor.user(request);

    const links = {
      self: { href: 'urn:user:9', type: 'resource' },
      siblings: { href: 'urn:users', type: 'collection' },
      repos: { href: 'urn:user:9:repos', type: 'collection' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'repos', url: 'http://repos', qualifier: 'urn:user:9', path: '/repos' },
    ];
    expectQueued(queue, expected);
  });
});

describe('Repo processing', () => {
  it('should link and queue correctly', () => {
    const request = createRequest('repo', 'http://foo/repo/12');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 12,
      owner: { id: 45, url: 'http://user/45' },
      collaborators_url: 'http://collaborators{/collaborator}',
      commits_url: 'http://commits{/sha}',
      contributors_url: 'http://contributors',
      events_url: 'http://events',
      issues_url: 'http://issues{/number}',
      pulls_url: 'http://pulls{/number}',
      stargazers_count: 2,
      stargazers_url: 'http://stargazers',
      subscribers_count: 1,
      subscribers_url: 'http://subscribers',
      teams_url: 'http://teams',
      organization: { id: 24, url: 'http://org/24' },
    };

    const processor = new GitHubProcessor();
    const document = processor.repo(request);

    const links = {
      self: { href: 'urn:repo:12', type: 'resource' },
      siblings: { href: 'urn:user:45:repos', type: 'collection' },
      owner: { href: 'urn:user:45', type: 'resource' },
      organization: { href: 'urn:org:24', type: 'resource' },
      events: { href: 'urn:repo:12:events', type: 'collection' },
      pull_requests: { href: 'urn:repo:12:pull_requests', type: 'collection' },
      teams: { href: 'urn:repo:12:teams:pages:*', type: 'relation' },
      collaborators: { href: 'urn:repo:12:collaborators:pages:*', type: 'relation' },
      contributors: { href: 'urn:repo:12:contributors:pages:*', type: 'relation' },
      stargazers: { href: 'urn:repo:12:stargazers:pages:*', type: 'relation' },
      subscribers: { href: 'urn:repo:12:subscribers:pages:*', type: 'relation' },
      commits: { href: 'urn:repo:12:commits', type: 'collection' },
      issues: { href: 'urn:repo:12:issues', type: 'collection' },
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/45', path: '/owner' },
      { type: 'org', url: 'http://org/24', path: '/organization' },
      { type: 'teams', url: 'http://teams', qualifier: 'urn:repo:12', path: '/teams', relation: { origin: 'repo', qualifier: 'urn:repo:12:teams', type: 'team' } },
      { type: 'collaborators', url: 'http://collaborators?affiliation=direct', qualifier: 'urn:repo:12', path: '/collaborators', relation: { origin: 'repo', qualifier: 'urn:repo:12:collaborators', type: 'user' } },
      { type: 'contributors', url: 'http://contributors', qualifier: 'urn:repo:12', path: '/contributors', relation: { origin: 'repo', qualifier: 'urn:repo:12:contributors', type: 'user' } },
      { type: 'stargazers', url: 'http://stargazers', qualifier: 'urn:repo:12', path: '/stargazers', relation: { origin: 'repo', qualifier: 'urn:repo:12:stargazers', type: 'user' } },
      { type: 'subscribers', url: 'http://subscribers', qualifier: 'urn:repo:12', path: '/subscribers', relation: { origin: 'repo', qualifier: 'urn:repo:12:subscribers', type: 'user' } },
      { type: 'issues', url: 'http://issues?state=all', qualifier: 'urn:repo:12', path: '/issues', },
      { type: 'commits', url: 'http://commits', qualifier: 'urn:repo:12', path: '/commits', },
      { type: 'events', url: 'http://events', qualifier: 'urn:repo:12', path: '/events', }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue deletion correctly', () => {
    const request = createRequest('repo', 'http://foo/repo/12');
    request.context = { action: 'deleted' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 12,
      owner: { id: 45, url: 'http://user/45' },
      collaborators_url: 'http://collaborators{/collaborator}',
      commits_url: 'http://commits{/sha}',
      contributors_url: 'http://contributors',
      events_url: 'http://events',
      issues_url: 'http://issues{/number}',
      pulls_url: 'http://pulls{/number}',
      stargazers_count: 2,
      stargazers_url: 'http://stargazers',
      subscribers_count: 1,
      subscribers_url: 'http://subscribers',
      teams_url: 'http://teams',
      organization: { id: 24, url: 'http://org/24' },
    };

    const processor = new GitHubProcessor();
    const document = processor.repo(request);

    const links = {
      self: { href: 'urn:repo:12', type: 'resource' },
      siblings: { href: 'urn:user:45:repos', type: 'collection' },
      owner: { href: 'urn:user:45', type: 'resource' },
      organization: { href: 'urn:org:24', type: 'resource' },
      events: { href: 'urn:repo:12:events', type: 'collection' },
      pull_requests: { href: 'urn:repo:12:pull_requests', type: 'collection' },
      teams: { href: 'urn:repo:12:teams:pages:*', type: 'relation' },
      collaborators: { href: 'urn:repo:12:collaborators:pages:*', type: 'relation' },
      contributors: { href: 'urn:repo:12:contributors:pages:*', type: 'relation' },
      stargazers: { href: 'urn:repo:12:stargazers:pages:*', type: 'relation' },
      subscribers: { href: 'urn:repo:12:subscribers:pages:*', type: 'relation' },
      commits: { href: 'urn:repo:12:commits', type: 'collection' },
      issues: { href: 'urn:repo:12:issues', type: 'collection' },
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/45', path: '/owner' },
      { type: 'org', url: 'http://org/24', path: '/organization' },
      { type: 'teams', url: 'http://teams', qualifier: 'urn:repo:12', path: '/teams', relation: { origin: 'repo', qualifier: 'urn:repo:12:teams', type: 'team' } },
      { type: 'collaborators', url: 'http://collaborators?affiliation=direct', qualifier: 'urn:repo:12', path: '/collaborators', relation: { origin: 'repo', qualifier: 'urn:repo:12:collaborators', type: 'user' } },
      { type: 'contributors', url: 'http://contributors', qualifier: 'urn:repo:12', path: '/contributors', relation: { origin: 'repo', qualifier: 'urn:repo:12:contributors', type: 'user' } },
      { type: 'stargazers', url: 'http://stargazers', qualifier: 'urn:repo:12', path: '/stargazers', relation: { origin: 'repo', qualifier: 'urn:repo:12:stargazers', type: 'user' } },
      { type: 'subscribers', url: 'http://subscribers', qualifier: 'urn:repo:12', path: '/subscribers', relation: { origin: 'repo', qualifier: 'urn:repo:12:subscribers', type: 'user' } },
      { type: 'issues', url: 'http://issues?state=all', qualifier: 'urn:repo:12', path: '/issues', },
      { type: 'commits', url: 'http://commits', qualifier: 'urn:repo:12', path: '/commits', },
      { type: 'events', url: 'http://events', qualifier: 'urn:repo:12', path: '/events', }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue CreateEvent', () => {
    const request = createRequest('CreateEvent', 'http://foo');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      repository: { id: 4, url: 'http://repo/4' }
    }
    request.document = createEvent('CreateEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.CreateEvent(request);

    const links = {
      self: { href: 'urn:repo:4:CreateEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:CreateEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      repository: { href: 'urn:repo:4', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue deletion of RepositoryEvent', () => {
    const request = createRequest('RepositoryEvent', 'http://foo');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      action: 'deleted',
      repository: { id: 4, url: 'http://repo/4' }
    }
    request.document = createEvent('RepositoryEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.RepositoryEvent(request);

    const links = {
      self: { href: 'urn:repo:4:RepositoryEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:RepositoryEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      repository: { href: 'urn:repo:4', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/', deletedAt: 'date and time' },
      { type: 'org', url: 'http://org/5', path: '/org' }
    ];
    expectQueued(queue, expected);
  });
});

describe('Commit processing', () => {
  it('should link and queue correctly', () => {
    testCommit('urn:repo:12');
  });

  it('should link and queue correctly if qualifier contains PushEvent', () => {
    testCommit('urn:repo:12:PushEvent:123');
  });

  function testCommit(qualifier) {
    const request = createRequest('commit', 'http://foo/commit');
    request.context = { qualifier: qualifier };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      sha: '6dcb09b5b5',
      url: 'http://repo/12/commits/6dcb09b5b5',
      commit: { comment_count: 1 },
      comments_url: 'http://comments',
      author: { id: 7, url: 'http://user/7' },
      committer: { id: 15, url: 'http://user/15' }
    };
    request.processMode = 'process';

    const processor = new GitHubProcessor();
    const document = processor.commit(request);

    const links = {
      self: { href: 'urn:repo:12:commit:6dcb09b5b5', type: 'resource' },
      siblings: { href: 'urn:repo:12:commits', type: 'collection' },
      commit_comments: { href: 'urn:repo:12:commit:6dcb09b5b5:commit_comments', type: 'collection' },
      author: { href: 'urn:user:7', type: 'resource' },
      committer: { href: 'urn:user:15', type: 'resource' },
      repo: { href: 'urn:repo:12', type: 'resource' },
    };
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/7', path: '/author' },
      { type: 'user', url: 'http://user/15', path: '/committer' },
      { type: 'repo', url: 'http://repo/12', path: '/repo' },
      { type: 'commit_comments', url: 'http://comments', qualifier: 'urn:repo:12:commit:6dcb09b5b5', path: '/commit_comments' }
    ];
    expectQueued(queue, expected);
  }

  it('should link and queue PushEvent with commits', () => {
    const request = createRequest('PushEvent', 'http://foo/events/123');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      commits: [{ sha: 'a1a', url: 'http://commits/a1a' }, { sha: 'b2b', url: 'http://commits/b2b' }]
    };
    request.document = createEvent('PushEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.PushEvent(request);

    const links = {
      self: { href: 'urn:repo:4:PushEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:PushEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      commits: { href: 'urn:repo:4:PushEvent:12345:commits', type: 'collection' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'commit', url: 'http://commits/a1a', qualifier: 'urn:repo:4:PushEvent:12345', path: '/commits' },
      { type: 'commit', url: 'http://commits/b2b', qualifier: 'urn:repo:4:PushEvent:12345', path: '/commits' }
    ];
    expectQueued(queue, expected);
  });
});

describe('Pull request commit processing', () => {
  it('should link and queue pull request commit correctly without comments', () => {
    testPullRequestCommit(false);
  });

  it('should link and queue pull request commit correctly with comments', () => {
    testPullRequestCommit(true);
  });

  function testPullRequestCommit(hasComments = false) {
    request = createRequest('pull_request_commit', 'http://foo/commit');
    request.context = { qualifier: 'urn:repo:12:pull_request:9' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      sha: '77cb09b5b5',
      url: 'http://repo/12/commits/77cb09b5b5',
      commit: { comment_count: 0 },
      author: { id: 7, url: 'http://user/7' },
      committer: { id: 15, url: 'http://user/15' }
    };
    if (hasComments) {
      request.document.commit.comment_count = 1;
      request.document.comments_url = 'http://comments';
    }
    request.processMode = 'process';

    const processor = new GitHubProcessor();
    const document = processor.pull_request_commit(request);
    const links = {
      self: { href: 'urn:repo:12:pull_request:9:pull_request_commit:77cb09b5b5', type: 'resource' },
      siblings: { href: 'urn:repo:12:pull_request:9:pull_request_commits', type: 'collection' },
      author: { href: 'urn:user:7', type: 'resource' },
      committer: { href: 'urn:user:15', type: 'resource' },
      repo: { href: 'urn:repo:12', type: 'resource' },
      pull_request : { href: 'urn:repo:12:pull_request:9', type: 'resource' },
      pull_request_commit_comments: { href: 'urn:repo:12:pull_request:9:pull_request_commit:77cb09b5b5:pull_request_commit_comments', type: 'collection' }
    };
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/7', path: '/author' },
      { type: 'user', url: 'http://user/15', path: '/committer' },
      { type: 'repo', url: 'http://repo/12', path: '/repo' }
    ];
    if (hasComments) {
      expected.push({ type: 'pull_request_commit_comments', url: 'http://comments', qualifier: 'urn:repo:12:pull_request:9:pull_request_commit:77cb09b5b5', path: '/pull_request_commit_comments' });
    }
    expectQueued(queue, expected);
  }
});

describe('Commit comment processing', () => {
  it('should link and queue correctly', () => {
    const request = createRequest('commit_comment', 'http://repo/commit/comment');
    request.context = { qualifier: 'urn:repo:12:commit:a1b1' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 37,
      user: { id: 7, url: 'http://user/7' }
    };
    const processor = new GitHubProcessor();
    const document = processor.commit_comment(request);

    const links = {
      self: { href: 'urn:repo:12:commit:a1b1:commit_comment:37', type: 'resource' },
      siblings: { href: 'urn:repo:12:commit:a1b1:commit_comments', type: 'collection' },
      commit: { href: 'urn:repo:12:commit:a1b1', type: 'resource' },
      user: { href: 'urn:user:7', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/7', path: '/user' },
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue CommitCommentEvent', () => {
    const request = createRequest('CommitCommentEvent', 'http://foo/pull');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      comment: { id: 7, url: 'http://commit_comment/7', commit_id: 'a1b1' }
    }
    request.document = createEvent('PullRequestReviewCommentEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.CommitCommentEvent(request);

    const links = {
      self: { href: 'urn:repo:4:CommitCommentEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:CommitCommentEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      commit_comment: { href: 'urn:repo:4:commit:a1b1:commit_comment:7', type: 'resource' },
      commit: { href: 'urn:repo:4:commit:a1b1', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'commit', url: 'http://repo/4/commits/a1b1', qualifier: 'urn:repo:4', path: '/commit' },
      { type: 'commit_comment', url: 'http://commit_comment/7', qualifier: 'urn:repo:4:commit:a1b1', path: '/commit_comment' }
    ];
    expectQueued(queue, expected);
  });
});

describe('Pull request commit comment processing', () => {
  it('should link and queue pull request commit comment correctly', () => {
    const request = createRequest('pull_request_commit_comment', 'http://repo/commit/comment');
    request.context = { qualifier: 'urn:repo:12:pull_request:7:pull_request_commit:a1b1' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 37,
      user: { id: 7, url: 'http://user/7' }
    };
    const processor = new GitHubProcessor();
    const document = processor.pull_request_commit_comment(request);

    const links = {
      self: { href: 'urn:repo:12:pull_request:7:pull_request_commit:a1b1:pull_request_commit_comment:37', type: 'resource' },
      siblings: { href: 'urn:repo:12:pull_request:7:pull_request_commit:a1b1:pull_request_commit_comments', type: 'collection' },
      pull_request_commit: { href: 'urn:repo:12:pull_request:7:pull_request_commit:a1b1', type: 'resource' },
      user: { href: 'urn:user:7', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/7', path: '/user' },
    ];
    expectQueued(queue, expected);
  });
});

describe('Deployment processing', () => {
  it('should link and queue correctly', () => {
    const request = createRequest('deployment', 'http://foo');
    request.context = { qualifier: 'urn:repo:12' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 3,
      sha: '6dcb09b5b5',
      creator: { id: 7, url: 'http://user/7' }
    };
    const processor = new GitHubProcessor();
    const document = processor.deployment(request);

    const links = {
      self: { href: 'urn:repo:12:deployment:3', type: 'resource' },
      siblings: { href: 'urn:repo:12:deployments', type: 'collection' },
      creator: { href: 'urn:user:7', type: 'resource' },
      commit: { href: 'urn:repo:12:commit:6dcb09b5b5', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/7', path: '/creator' }
    ];
    expectQueued(queue, expected);
  });
});

describe('Pull Request processing', () => {
  it('should link and queue correctly', () => {
    const request = createRequest('pull_request', 'http://foo/pull');
    request.context = { qualifier: 'urn:repo:12' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 13,
      comments: 1,
      commits: 1,
      assignee: { id: 1, url: 'http://user/1' },
      milestone: { id: 26 },
      head: { repo: { id: 45, url: 'http://repo/45' } },
      base: { repo: { id: 17, url: 'http://repo/17' } },
      _links: {
        self: { href: 'http://pull_request/13' },
        issue: { href: 'http://issue/13' },
        review_comments: { href: 'http://review_comments' },
        commits: { href: 'http://commits' },
        statuses: { href: 'http://statuses/funkySHA' }
      },
      user: { id: 7, url: 'http://user/7' },
      merged_by: { id: 15, url: 'http://user/15' }
    };
    const processor = new GitHubProcessor();
    const document = processor.pull_request(request);

    const links = {
      self: { href: 'urn:repo:12:pull_request:13', type: 'resource' },
      siblings: { href: 'urn:repo:12:pull_requests', type: 'collection' },
      user: { href: 'urn:user:7', type: 'resource' },
      merged_by: { href: 'urn:user:15', type: 'resource' },
      assignee: { href: 'urn:user:1', type: 'resource' },
      head: { href: 'urn:repo:45', type: 'resource' },
      base: { href: 'urn:repo:17', type: 'resource' },
      repo: { href: 'urn:repo:17', type: 'resource' },
      reviews: { href: 'urn:repo:12:pull_request:13:reviews', type: 'collection' },
      review_comments: { href: 'urn:repo:12:pull_request:13:review_comments', type: 'collection' },
      pull_request_commits: { href: 'urn:repo:12:pull_request:13:pull_request_commits', type: 'collection' },
      statuses: { href: 'urn:repo:12:commit:funkySHA:statuses', type: 'collection' },
      issue: { href: 'urn:repo:12:issue:13', type: 'resource' },
      issue_comments: { href: 'urn:repo:12:issue:13:issue_comments', type: 'collection' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/7', path: '/user' },
      { type: 'user', url: 'http://user/15', path: '/merged_by' },
      { type: 'user', url: 'http://user/1', path: '/assignee' },
      { type: 'repo', url: 'http://repo/45', path: '/head' },
      { type: 'repo', url: 'http://repo/17', path: '/base' },
      { type: 'reviews', url: 'http://pull_request/13/reviews', qualifier: 'urn:repo:12:pull_request:13', path: '/reviews' },
      { type: 'review_comments', url: 'http://review_comments', qualifier: 'urn:repo:12:pull_request:13', path: '/review_comments' },
      { type: 'statuses', url: 'http://statuses/funkySHA', qualifier: 'urn:repo:12:pull_request:13', path: '/statuses' },
      { type: 'pull_request_commits', url: 'http://commits', qualifier: 'urn:repo:12:pull_request:13', path: '/pull_request_commits' }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue PullRequestEvent', () => {
    const request = createRequest('PullRequestEvent', 'http://foo/pull');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      pull_request: { id: 1, url: 'http://pull_request/1', issue_url: 'http://issue/9' }
    }
    request.document = createEvent('PullRequestEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.PullRequestEvent(request);

    const links = {
      self: { href: 'urn:repo:4:PullRequestEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:PullRequestEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      pull_request: { href: 'urn:repo:4:pull_request:1', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'pull_request', url: 'http://pull_request/1', qualifier: 'urn:repo:4', path: '/pull_request' },
      { type: 'issue', url: 'http://issue/9', qualifier: 'urn:repo:4', path: '/issue' }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue PullRequestReviewEvent', () => {
    const request = createRequest('PullRequestReviewEvent', 'http://foo/pull');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      pull_request: { id: 1, url: 'http://pull_request/1' }
    }
    request.document = createEvent('PullRequestReviewEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.PullRequestReviewEvent(request);

    const links = {
      self: { href: 'urn:repo:4:PullRequestReviewEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:PullRequestReviewEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      pull_request: { href: 'urn:repo:4:pull_request:1', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'pull_request', url: 'http://pull_request/1', qualifier: 'urn:repo:4', path: '/pull_request' }
    ];
    expectQueued(queue, expected);
  });
});

describe('Pull request/review comment processing', () => {
  it('should link and queue correctly', () => {
    const request = createRequest('review_comment', 'http://repo/pull_request/comment');
    request.context = { qualifier: 'urn:repo:12:pull_request:27' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 37,
      user: { id: 7, url: 'http://user/7' }
    };
    const processor = new GitHubProcessor();
    const document = processor.review_comment(request);

    const links = {
      self: { href: 'urn:repo:12:pull_request:27:review_comment:37', type: 'resource' },
      siblings: { href: 'urn:repo:12:pull_request:27:review_comments', type: 'collection' },
      pull_request: { href: 'urn:repo:12:pull_request:27', type: 'resource' },
      user: { href: 'urn:user:7', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/7', path: '/user' },
    ];
    expectQueued(queue, expected);
  });

  function testPullRequestReviewCommentEvent(request, method, isDeletion) {
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const processor = new GitHubProcessor();
    const document = processor[method](request);

    const links = {
      self: { href: 'urn:repo:4:PullRequestReviewCommentEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:PullRequestReviewCommentEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      comment: { href: 'urn:repo:4:pull_request:1:review_comment:7', type: 'resource' }
    }
    if (!isDeletion) {
      links.pull_request = { href: 'urn:repo:4:pull_request:1', type: 'resource' };
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'review_comment', url: 'http://review_comment/7', qualifier: 'urn:repo:4:pull_request:1', path: isDeletion ? '/' : '/comment', deletedAt: isDeletion ? 'date and time' : undefined }
    ];
    if (!isDeletion) {
      expected.push({ type: 'pull_request', url: 'http://pull_request/1', qualifier: 'urn:repo:4', path: '/pull_request' });
    }
    expectQueued(queue, expected);
  }

  it('should link and queue PullRequestReviewCommentEvent', () => {
    const request = createRequest('PullRequestReviewCommentEvent', 'http://foo/pull');
    const payload = {
      comment: { id: 7, url: 'http://review_comment/7' },
      pull_request: { id: 1, url: 'http://pull_request/1' }
    }
    request.document = createEvent('PullRequestReviewCommentEvent', payload);
    testPullRequestReviewCommentEvent(request, 'PullRequestReviewCommentEvent', false);
  });

  it('should link and queue deletion of PullRequestReviewCommentEvent', () => {
    const request = createRequest('PullRequestReviewCommentEvent', 'http://foo/pull');
    const payload = {
      action: 'deleted',
      comment: { id: 7, url: 'http://review_comment/7' },
      pull_request: { id: 1, url: 'http://pull_request/1' }
    }
    request.document = createEvent('PullRequestReviewCommentEvent', payload);
    testPullRequestReviewCommentEvent(request, 'PullRequestReviewCommentEvent', true);
  });

  it('should link and queue LegacyPullRequestReviewCommentEvent', () => {
    const request = createRequest('PullRequestReviewCommentEvent', 'http://foo/pull');
    const payload = {
      comment: { id: 7, url: 'http://review_comment/7', pull_request_url: 'http://pull_request/1' }
    }
    request.document = createEvent('PullRequestReviewCommentEvent', payload);

    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const processor = new GitHubProcessor();
    const document = processor.PullRequestReviewCommentEvent(request);

    // test that the new request got queued and that the doc has the right stuff

    const newRequest = queue.pop();
    newRequest.document = { id: 1, url: 'http://pull_request/1' }
    testPullRequestReviewCommentEvent(newRequest, 'LegacyPullRequestReviewCommentEvent', false);
  });
});

describe('Issue processing', () => {
  it('should link and queue correctly', () => {
    const request = createRequest('issue', 'http://repo/issue');
    request.context = { qualifier: 'urn:repo:12' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 27,
      comments: 1,
      assignee: { id: 1, url: 'http://user/1' },
      assignees: [{ id: 50 }, { id: 51 }],
      milestone: { id: 26 },
      labels: [{ id: 88 }, { id: 99 }],
      repo: { id: 45, url: 'http://repo/45' },
      comments_url: 'http://issue/27/comments',
      pull_request: { url: 'http://pull_request/27' },
      user: { id: 7, url: 'http://user/7' },
      closed_by: { id: 15, url: 'http://user/15' }
    };
    const processor = new GitHubProcessor();
    const document = processor.issue(request);

    const links = {
      self: { href: 'urn:repo:12:issue:27', type: 'resource' },
      siblings: { href: 'urn:repo:12:issues', type: 'collection' },
      user: { href: 'urn:user:7', type: 'resource' },
      labels: { hrefs: ['urn:repo:12:label:88', 'urn:repo:12:label:99'], type: 'resource' },
      closed_by: { href: 'urn:user:15', type: 'resource' },
      assignee: { href: 'urn:user:1', type: 'resource' },
      repo: { href: 'urn:repo:12', type: 'resource' },
      assignees: { hrefs: ['urn:user:50', 'urn:user:51'], type: 'resource' },
      issue_comments: { href: 'urn:repo:12:issue:27:issue_comments', type: 'collection' },
      pull_request: { href: 'urn:repo:12:pull_request:27', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/7', path: '/user' },
      { type: 'user', url: 'http://user/15', path: '/closed_by' },
      { type: 'user', url: 'http://user/1', path: '/assignee' },
      { type: 'repo', url: 'http://repo/45', path: '/repo' },
      { type: 'issue_comments', url: 'http://issue/27/comments', qualifier: 'urn:repo:12:issue:27', path: '/issue_comments' },
      { type: 'pull_request', url: 'http://pull_request/27', qualifier: 'urn:repo:12', path: '/pull_request' }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue IssuesEvent', () => {
    const request = createRequest('IssuesEvent', 'http://foo/pull');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      assignee: { id: 2, url: 'http://user/2' },
      issue: { id: 1, url: 'http://issue/1' },
      label: { id: 8, url: 'http://label/8' }
    }
    request.document = createEvent('IssuesEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.IssuesEvent(request);

    const links = {
      self: { href: 'urn:repo:4:IssuesEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:IssuesEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      assignee: { href: 'urn:user:2', type: 'resource' },
      issue: { href: 'urn:repo:4:issue:1', type: 'resource' },
      label: { href: 'urn:repo:4:label:8', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'user', url: 'http://user/2', path: '/assignee' },
      { type: 'issue', url: 'http://issue/1', qualifier: 'urn:repo:4', path: '/issue' },
      { type: 'label', url: 'http://label/8', qualifier: 'urn:repo:4', path: '/label' }
    ];
    expectQueued(queue, expected);
  });
});

describe('Issue comment processing', () => {
  it('should link and queue correctly', () => {
    const request = createRequest('issue_comment', 'http://repo/issue/comment');
    request.context = { qualifier: 'urn:repo:12:issue:27' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 37,
      user: { id: 7, url: 'http://user/7' }
    };
    const processor = new GitHubProcessor();
    const document = processor.issue_comment(request);

    const links = {
      self: { href: 'urn:repo:12:issue:27:issue_comment:37', type: 'resource' },
      siblings: { href: 'urn:repo:12:issue:27:issue_comments', type: 'collection' },
      issue: { href: 'urn:repo:12:issue:27', type: 'resource' },
      user: { href: 'urn:user:7', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/7', path: '/user' },
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue deletion correctly', () => {
    const request = createRequest('issue_comment', 'http://repo/issue/comment');
    request.context = { qualifier: 'urn:repo:12:issue:27', action: 'deleted' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 37,
      user: { id: 7, url: 'http://user/7' }
    };
    const processor = new GitHubProcessor();
    const document = processor.issue_comment(request);

    const links = {
      self: { href: 'urn:repo:12:issue:27:issue_comment:37', type: 'resource' },
      siblings: { href: 'urn:repo:12:issue:27:issue_comments', type: 'collection' },
      issue: { href: 'urn:repo:12:issue:27', type: 'resource' },
      user: { href: 'urn:user:7', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/7', path: '/user' },
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue IssueCommentEvent', () => {
    const request = createRequest('IssueCommentEvent', 'http://foo/');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      comment: { id: 7, url: 'http://issue_comment/7' },
      issue: { id: 1, url: 'http://issue/1' }
    }
    request.document = createEvent('IssueCommentEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.IssueCommentEvent(request);

    const links = {
      self: { href: 'urn:repo:4:IssueCommentEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:IssueCommentEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      comment: { href: 'urn:repo:4:issue:1:issue_comment:7', type: 'resource' },
      issue: { href: 'urn:repo:4:issue:1', type: 'resource' },
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'issue_comment', url: 'http://issue_comment/7', qualifier: 'urn:repo:4:issue:1', path: '/comment' },
      { type: 'issue', url: 'http://issue/1', qualifier: 'urn:repo:4', path: '/issue' }
    ];
    expectQueued(queue, expected);
  });

    it('should link and queue deletion of IssueCommentEvent', () => {
    const request = createRequest('IssueCommentEvent', 'http://foo/');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      action: 'deleted',
      comment: { id: 7, url: 'http://issue_comment/7' },
      issue: { id: 1, url: 'http://issue/1' }
    }
    request.document = createEvent('IssueCommentEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.IssueCommentEvent(request);

    const links = {
      self: { href: 'urn:repo:4:IssueCommentEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:IssueCommentEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      comment: { href: 'urn:repo:4:issue:1:issue_comment:7', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'issue_comment', url: 'http://issue_comment/7', qualifier: 'urn:repo:4:issue:1', path: '/', deletedAt: 'date and time' }
    ];
    expectQueued(queue, expected);
  });
});

describe('Member processing', () => {
  it('should link and queue added MemberEvent', () => {
    const request = createRequest('MemberEvent', 'http://foo/');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      action: 'added',
      member: { id: 7, url: 'http://member/7' }
    }
    request.document = createEvent('MemberEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.MemberEvent(request);

    const links = {
      self: { href: 'urn:repo:4:MemberEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:MemberEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      repository: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      member: { href: 'urn:user:7', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'user', url: 'http://member/7', path: '/member' }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue deleted/removed MemberEvent', () => {
    const request = createRequest('MemberEvent', 'http://foo/');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      action: 'removed',
      member: { id: 7, url: 'http://member/7' }
    }
    request.document = createEvent('MemberEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.MemberEvent(request);

    const links = {
      self: { href: 'urn:repo:4:MemberEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:MemberEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      repository: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' }
    ];
    expectQueued(queue, expected);
  });
});

describe('Membership processing', () => {
  it('should link and queue added MembershipEvent', () => {
    const request = createRequest('MembershipEvent', 'http://foo/');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      action: 'added',
      member: { id: 7, url: 'http://member/7' },
      team: { id: 14, url: 'http://member/14' }
    }
    request.document = createEvent('MembershipEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.MembershipEvent(request);

    const links = {
      self: { href: 'urn:repo:4:MembershipEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:MembershipEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      member: { href: 'urn:user:7', type: 'resource' },
      team: { href: 'urn:team:14', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'user', url: 'http://member/7', path: '/' },
      { type: 'team', url: 'http://member/14', path: '/' }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue removed MembershipEvent with a removed team', () => {
    const request = createRequest('MembershipEvent', 'http://foo/');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      action: 'removed',
      member: { id: 7, url: 'http://member/7' },
      team: { id: 14, url: 'http://member/14', deleted: true }
    }
    request.document = createEvent('MembershipEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.MembershipEvent(request);

    const links = {
      self: { href: 'urn:repo:4:MembershipEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:MembershipEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      member: { href: 'urn:user:7', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'user', url: 'http://member/7', path: '/' }
    ];
    expectQueued(queue, expected);
  });
});

describe('Status processing', () => {
  it('should link and queue StatusEvent', () => {
    const request = createRequest('StatusEvent', 'http://foo/');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      sha: 'a1b2'
    }
    request.document = createEvent('StatusEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.StatusEvent(request);

    const links = {
      self: { href: 'urn:repo:4:StatusEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:StatusEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      commit: { href: 'urn:repo:4:commit:a1b2', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' }
    ];
    expectQueued(queue, expected);
  });
});

describe('Team processing', () => {
  it('should link and queue correctly', () => {
    const request = createRequest('team', 'http://team/66');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 66,
      members_url: 'http://teams/66/members{/member}',
      repositories_url: 'http://teams/66/repos',
      organization: { id: 9, url: 'http://orgs/9' }
    };
    const processor = new GitHubProcessor();
    const document = processor.team(request);

    const links = {
      self: { href: 'urn:team:66', type: 'resource' },
      siblings: { href: 'urn:org:9:teams', type: 'collection' },
      organization: { href: 'urn:org:9', type: 'resource' },
      members: { href: 'urn:team:66:team_members:pages:*', type: 'relation' },
      repos: { href: 'urn:team:66:repos:pages:*', type: 'relation' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'org', url: 'http://orgs/9', path: '/organization' },
      { type: 'repos', url: 'http://teams/66/repos', qualifier: 'urn:team:66', path: '/repos' },
      { type: 'members', url: 'http://teams/66/members', qualifier: 'urn:team:66', path: '/members' }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue deletion correctly', () => {
    const request = createRequest('team', 'http://team/66');
    request.context = { action: 'deleted' };
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.document = {
      _metadata: { links: {} },
      id: 66,
      members_url: 'http://teams/66/members{/member}',
      repositories_url: 'http://teams/66/repos',
      organization: { id: 9, url: 'http://orgs/9' }
    };
    const processor = new GitHubProcessor();
    const document = processor.team(request);

    const links = {
      self: { href: 'urn:team:66', type: 'resource' },
      siblings: { href: 'urn:org:9:teams', type: 'collection' },
      organization: { href: 'urn:org:9', type: 'resource' },
      members: { href: 'urn:team:66:team_members:pages:*', type: 'relation' },
      repos: { href: 'urn:team:66:repos:pages:*', type: 'relation' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'org', url: 'http://orgs/9', path: '/organization' },
      { type: 'repos', url: 'http://teams/66/repos', qualifier: 'urn:team:66', path: '/repos' },
      { type: 'members', url: 'http://teams/66/members', qualifier: 'urn:team:66', path: '/members' }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue TeamEvent', () => {
    const request = createRequest('TeamEvent', 'http://foo/team');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      team: { id: 7, url: 'http://team/7' },
      organization: { id: 5, url: 'http://org/5' }
    }
    request.document = createOrgEvent('TeamEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.TeamEvent(request);

    const links = {
      self: { href: 'urn:team:7:TeamEvent:12345', type: 'resource' },
      siblings: { href: 'urn:team:7:TeamEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      team: { href: 'urn:team:7', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'team', url: 'http://team/7', path: '/team' }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue deletion of TeamEvent', () => {
    const request = createRequest('TeamEvent', 'http://foo/team');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      action: 'deleted',
      team: { id: 7, url: 'http://team/7' },
      organization: { id: 5, url: 'http://org/5' }
    }
    request.document = createOrgEvent('TeamEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.TeamEvent(request);

    const links = {
      self: { href: 'urn:team:7:TeamEvent:12345', type: 'resource' },
      siblings: { href: 'urn:team:7:TeamEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      team: { href: 'urn:team:7', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'team', url: 'http://team/7', path: '/', deletedAt: 'date and time' }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue TeamEvent with repository', () => {
    const request = createRequest('TeamEvent', 'http://foo/team');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      team: { id: 7, url: 'http://team/7' },
      organization: { id: 5, url: 'http://org/5' },
      repository: { id: 6, url: 'http://repo/6' }
    }
    request.document = createOrgEvent('TeamEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.TeamEvent(request);

    const links = {
      self: { href: 'urn:team:7:TeamEvent:12345', type: 'resource' },
      siblings: { href: 'urn:team:7:TeamEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      repository: { href: 'urn:repo:6', type: 'resource' },
      team: { href: 'urn:team:7', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'repo', url: 'http://repo/6', path: '/repository' },
      { type: 'team', url: 'http://team/7', path: '/team' }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue added_to_repository TeamEvent with repository', () => {
    const request = createRequest('TeamEvent', 'http://foo/team');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      action: 'added_to_repository',
      team: { id: 7, url: 'http://team/7' },
      organization: { id: 5, url: 'http://org/5' },
      repository: { id: 6, url: 'http://repo/6' }
    }
    request.document = createOrgEvent('TeamEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.TeamEvent(request);

    const links = {
      self: { href: 'urn:team:7:TeamEvent:12345', type: 'resource' },
      siblings: { href: 'urn:team:7:TeamEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      repository: { href: 'urn:repo:6', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'repo', url: 'http://repo/6', path: '/repo' }
    ];
    expectQueued(queue, expected);
  });

  it('should link and queue TeamAddEvent', () => {
    const request = createRequest('TeamAddEvent', 'http://foo/team');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      team: { id: 7, url: 'http://team/7' },
      organization: { id: 5, url: 'http://org/5' },
      repository: { id: 6, url: 'http://repo/6' }
    }
    request.document = createOrgEvent('TeamAddEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.TeamAddEvent(request);

    const links = {
      self: { href: 'urn:team:7:TeamAddEvent:12345', type: 'resource' },
      siblings: { href: 'urn:team:7:TeamAddEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      repository: { href: 'urn:repo:6', type: 'resource' },
      team: { href: 'urn:team:7', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'org', url: 'http://org/5', path: '/org' },
      { type: 'repo', url: 'http://repo/6', path: '/repository' },
      { type: 'team', url: 'http://team/7', path: '/team' }
    ];
    expectQueued(queue, expected);
  });
});

describe('Watch processing', () => {
  it('should link and queue WatchEvent', () => {
    const request = createRequest('WatchEvent', 'http://foo/watch');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    const payload = {
      repository: { id: 4, url: 'http://repo/4' }
    }
    request.document = createEvent('WatchEvent', payload);

    const processor = new GitHubProcessor();
    const document = processor.WatchEvent(request);

    const links = {
      self: { href: 'urn:repo:4:WatchEvent:12345', type: 'resource' },
      siblings: { href: 'urn:repo:4:WatchEvents', type: 'collection' },
      actor: { href: 'urn:user:3', type: 'resource' },
      repo: { href: 'urn:repo:4', type: 'resource' },
      org: { href: 'urn:org:5', type: 'resource' },
      repository: { href: 'urn:repo:4', type: 'resource' }
    }
    expectLinks(document._metadata.links, links);

    const expected = [
      { type: 'user', url: 'http://user/3', path: '/actor' },
      { type: 'repo', url: 'http://repo/4', path: '/repo' },
      { type: 'org', url: 'http://org/5', path: '/org' }
    ];
    expectQueued(queue, expected);
  });
});

describe('Event Finder', () => {
  it('will skip duplicates', () => {
    const docs = { 'http://repo1/events/3': '{ id: 3 }', 'http://repo1/events/4': '{ id: 4}' };
    const store = { get: (type, url) => { return Q(docs[url]); } }
    const events = [];
    for (let i = 0; i < 20; i++) {
      events.push({ id: i, repo: { url: 'http://repo1' } })
    }
    const processor = new GitHubProcessor();
    processor.store = store;
    processor._findNew(events).then(newEvents => {
      expect(newEvents.length).to.be.equal(18);
    });
  });
});

describe('Event Trigger', () => {
  it('should queue update_events when on timeline', () => {
    const request = createRequest('event_trigger', 'http://foo/events/4321');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.payload = { type:'issue_comment', body: { action: 'created' } };

    const processor = new GitHubProcessor();
    processor.event_trigger(request);

    const expected = [
      { type: 'update_events', url: 'http://foo/events', path: '/' }
    ];
    expectQueued(queue, expected);
    expect(queue[0].payload).to.undefined;
  });

  it('should queue explict event when not on timeline', () => {
    const request = createRequest('event_trigger', 'http://foo/events/4321');
    const queue = [];
    request.crawler = { queue: sinon.spy(request => { queue.push.apply(queue, request) }) };
    request.payload = { type:'repository', body: { action: 'created' } };

    const processor = new GitHubProcessor();
    processor.event_trigger(request);

    const expected = [
      { type: 'RepositoryEvent', url: request.url, path: '/' }
    ];
    expectQueued(queue, expected);
    expect(queue[0].payload).to.be.deep.equal(request.payload);
  });
});

// =========================== HELPERS =========================


function createRequest(type, url, context = {}) {
  const result = new Request(type, url, context);
  result.policy = TraversalPolicy.default(type);
  result.payload = { fetchedAt: 'date and time' };
  return result;
}

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
  actual.forEach(a => {
    const ar = a.context.relation;
    expect(expected.some(e => {
      const er = e.context ? e.context.relation : null;
      return e.type === a.type
        && e.url === a.url
        && (!e.urn || e.urn === a.context.qualifier)
        && (!e.deletedAt || e.deletedAt === a.context.deletedAt)
        && (!e.path || e.path === a.policy.map.path)
        && (!er || (er.origin === ar.orgin && er.qualifier === ar.qualifier && er.type === ar.type));
    })).to.be.true;
  })
}

function createEvent(type, payload) {
  return {
    _metadata: { links: {} },
    type: type,
    id: 12345,
    payload: payload,
    actor: { id: 3, url: 'http://user/3' },
    repo: { id: 4, url: 'http://repo/4' },
    org: { id: 5, url: 'http://org/5' }
  };
}

function createOrgEvent(type, payload) {
  return {
    _metadata: { links: {} },
    type: type,
    id: 12345,
    payload: payload,
    actor: { id: 3, url: 'http://user/3' },
    org: { id: 5, url: 'http://org/5' }
  };
}

function createLinkHeader(target, previous, next, last) {
  const separator = target.includes('?') ? '&' : '?';
  const firstLink = null; //`<${urlHost}/${target}${separator}page=1>; rel="first"`;
  const prevLink = previous ? `<${target}${separator}page=${previous}>; rel="prev"` : null;
  const nextLink = next ? `<${target}${separator}page=${next}>; rel="next"` : null;
  const lastLink = last ? `<${target}${separator}page=${last}>; rel="last"` : null;
  return [firstLink, prevLink, nextLink, lastLink].filter(value => { return value !== null; }).join(',');
}