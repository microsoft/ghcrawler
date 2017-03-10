// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const expect = require('chai').expect;
const CrawlerFactory = require('../../lib/crawlerFactory');
const Q = require('q');
const Request = require('ghcrawler').request;
const sinon = require('sinon');

let crawler = null;
let spies = {};

describe('Simple processing', () => {
  it('sdfs', () => {
    return createCrawler().then(newCrawler => {
      crawler = newCrawler;
      const request = new Request('org', 'https://api.github.com/orgs/test')
      return crawler.queue(request)
        .then(processOne)
        .then(checkDoc.bind(null, 'org', 'urn:org:1', 4))
        .then(processOne)
        .then(checkDoc.bind(null, 'user', 'urn:user:1', 1))
        .then(processOne)
        .then(checkDoc.bind(null, 'repos', 'urn:org:1:repos:page:1', 0))
        .then(processOne)
        .then(checkDoc.bind(null, 'members', 'urn:org:1:members:page:1', 2))
        .then(processOne)
        .then(checkDoc.bind(null, 'teams', 'urn:org:1:teams:page:1', 1))
        .then(processOne)
        .then(checkDoc.bind(null, 'repos', 'urn:org:1:repos:page:1', 0))
        .then(processOne)
        .then(checkDoc.bind(null, 'user', 'urn:user:2', 1))  // queued as a member of the org
        .then(processOne)
        .then(checkDoc.bind(null, 'team', 'urn:team:20', 2))
        .then(processOne)
        .then(checkDoc.bind(null, 'repos', 'urn:user:2:repos:page:1', 0))
        .then(processOne)
        .then(checkDoc.bind(null, 'members', 'urn:team:20:members:page:1', 0))
        .then(processOne)
        .then(checkDoc.bind(null, 'repos', 'urn:team:20:repos:page:1', 0))
        .then(processOne)
        .then(processOne)
        .then(processOne);
    });
  });
});

function processOne() {
  resetCrawlerSpies(crawler);
  return crawler.processOne({ loopName: 'test' });
}

function checkDoc(type, urn, queuedCount) {
  const doc = crawler.store.collections[type][urn];
  expect(!!doc).to.be.equal(true, urn);
  expect(doc._metadata.links.self.href).to.be.equal(urn, urn);
  const queued = gatherQueued(spies.queueSpy);
  expect(queued.length).to.be.equal(queuedCount, urn);
}

function gatherQueued(spy) {
  let result = [];
  for (let i = 0; i < spy.callCount; i++) {
    result = result.concat(spy.getCall(i).args[0]);
  }
  return result;
}

function createCrawler() {
  const service = CrawlerFactory.createService('InMemory');
  return service.ensureInitialized().then(() => {
    const crawler = service.crawler;
    crawler.options.orgList = null;
    crawler.fetcher = new TestFetcher();
    return spyOnCrawler(crawler);
  });
}

function spyOnCrawler(crawler) {
  spies = {};
  spies.pushSpy = sinon.spy(crawler.queues, 'push');
  spies.queueSpy = sinon.spy(crawler, 'queue');
  return crawler;
}

function resetCrawlerSpies(crawler) {
  for (let spy in spies) {
    spies[spy].reset();
  }
  return crawler;
}

class TestFetcher {
  constructor() {
    this.resources = resources;
  }

  fetch(request) {
    const response = this.resources[request.url];
    if (!response) {
      return Q.reject('Not found');
    }
    response.statusCode = 200;
    request.document = response.body;
    request.contentOrigin = 'origin';
    request.response = response;
    return Q(request);
  }
}

const resources = {
  'https://api.github.com/orgs/test': {
    body: {
      "id": 1,
      "url": "https://api.github.com/orgs/test",
      "repos_url": "https://api.github.com/orgs/test/repos",
      "members_url": "https://api.github.com/orgs/test/members{/member}",
    }
  },
  'https://api.github.com/orgs/test/repos': {
    body: [
      // {
      //   "url": "https://api.github.com/repos/test/repo1",
      // }
    ]
  },
  'https://api.github.com/users/test/repos': {
    body: []
  },
  'https://api.github.com/users/test': {
    body:
    {
      "id": 1,
      "url": "https://api.github.com/users/test",
      "repos_url": "https://api.github.com/users/test/repos",
    }
  },
  'https://api.github.com/users/user2': {
    body:
    {
      "id": 2,
      "url": "https://api.github.com/users/user2",
      "repos_url": "https://api.github.com/users/user2/repos",
    }
  },
  'https://api.github.com/users/user2/repos': {
    body: [
      // {
      //   "url": "https://api.github.com/repos/user2/repo2",
      // }
    ]
  },
  'https://api.github.com/teams/20': {
    body:
    {
      "id": 20,
      "members_url": "https://api.github.com/teams/20/members{/member}",
      "repositories_url": "https://api.github.com/teams/20/repos",
      "members_count": 3,
      "repos_count": 10,
      "organization": {
        "id": 1,
        "url": "https://api.github.com/orgs/test",
      }
    }
  },
  'https://api.github.com/teams/20/repos': {
    body: []
  },
  'https://api.github.com/teams/20/members': {
    body: []
  },
  'https://api.github.com/repos/test/repo1': {
    body: {
      "id": 10,
      "owner": {
        "id": 1,
        "url": "https://api.github.com/users/test",
      },
      "collaborators_url": "http://api.github.com/repos/test/repo1/collaborators{/collaborator}",
      "commits_url": "http://api.github.com/repos/test/repo1/commits{/sha}",
      "contributors_url": "http://api.github.com/repos/test/repo1/contributors",
      "events_url": "http://api.github.com/repos/test/repo1/events",
      "issues_url": "http://api.github.com/repos/test/repo1/issues{/number}",
      "pulls_url": "http://api.github.com/repos/test/repo1/pulls{/number}",
      "subscribers_url": "http://api.github.com/repos/test/repo1/subscribers",
      "teams_url": "http://api.github.com/repos/test/repo1/teams",
      "subscribers_count": 42,
      "organization": {
        "id": 1,
        "url": "https://api.github.com/orgs/test",
      }
    }
  }, 'https://api.github.com/repos/user2/repo2': {
    body: {
      "id": 11,
      "owner": {
        "id": 2,
        "url": "https://api.github.com/users/user2",
      },
      "collaborators_url": "http://api.github.com/repos/user2/repo2/collaborators{/collaborator}",
      "commits_url": "http://api.github.com/repos/user2/repo2/commits{/sha}",
      "contributors_url": "http://api.github.com/repos/user2/repo2/contributors",
      "events_url": "http://api.github.com/repos/user2/repo2/events",
      "issues_url": "http://api.github.com/repos/user2/repo2/issues{/number}",
      "pulls_url": "http://api.github.com/repos/user2/repo2/pulls{/number}",
      "subscribers_url": "http://api.github.com/repos/user2/repo2/subscribers",
      "teams_url": "http://api.github.com/repos/user2/repo2/teams",
    }
  },
  'https://api.github.com/repos/test/repo1/collaborators': {
    body: [
      {
        "url": "https://api.github.com/users/test",
      },
      {
        "url": "https://api.github.com/users/user2",
      }
    ]
  },
  'https://api.github.com/orgs/test/members': {
    body: [
      {
        "url": "https://api.github.com/users/test",
      },
      {
        "url": "https://api.github.com/users/user2",
      }
    ]
  },
  'https://api.github.com/orgs/test/teams': {
    body: [
      {
        "url": "https://api.github.com/teams/20",
      }
    ]
  }
};
