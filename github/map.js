// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

// Map building blocks
const self = {};

function neighbors() {
  return self;
}

function collection(type) {
  // return [type];
  return type;
}

function relation(type) {
  // return [[type]];
  return type;
}

const commit_comment = {
  _type: 'commit_comment',
  user: self
};


const commit = {
  _type: 'commit',
  commit_comments: collection(commit_comment),
  repo: self,
  author: self,
  committer: self
};

const pull_request_commit_comment = {
  _type: 'pull_request_commit_comment',
  user: self
};


const pull_request_commit = {
  _type: 'pull_request_commit',
  pull_request_commit_comments: collection(pull_request_commit_comment),
  repo: self,
  author: self,
  committer: self
};

const status = self;

const issue_comment = {
  _type: 'issue_comment',
  user: self
};

const issue = {
  _type: 'issue',
  //  assignees: collection(user),
  user: self,
  repo: self,
  assignee: self,
  closed_by: self,
  issue_comments: collection(issue_comment)
  //  pull_request: pull_request
};

const review_comment = {
  _type: 'review_comment',
  user: self
};

const review = {
  _type: 'review',
  user: self,
  pull_request: self
};

const pull_request = {
  _type: 'pull_request',
  user: self,
  merged_by: self,
  assignee: self,
  head: self,
  base: self,
  reviews: review,
  review_comments: review_comment,
  statuses: collection(status),
  pull_request_commits: collection(pull_request_commit),
  issue: issue,
  issue_comments: collection(issue_comment)
};

// patch pull_request into issue as it is a cycle.
issue.pull_request = pull_request;

const deployment = {
  _type: 'deployment',
  creator: self
};

const traffic = {
  _type: 'traffic',
  referrers: self,
  paths: self,
  views: self,
  clones: self
};

const team = {
  _type: 'team',
  organization: self,
  members: relation(self),
  repos: relation(self)
};

const repo = {
  _type: 'repo',
  owner: self,
  organization: self,
  teams: relation(self),
  collaborators: relation(self),
  contributors: relation(self),
  stargazers: relation(self),
  subscribers: relation(self),
  issues: collection(issue),
  commits: collection(commit),
  events: collection(event)
};

const user = {
  _type: 'user',
  repos: collection(repo)
};

const org = {
  _type: 'org',
  repos: collection(repo),
  user: user,
  members: relation(user),
  teams: relation(team)
};

const orgs = {
  _type: 'orgs',
  orgs: collection(org)
};

function event(additions = {}) {
  const base = {
    actor: self,
    repo: self,
    org: self
  };
  return Object.assign({}, base, additions);
}

const events = {
  CommitCommentEvent: event({
    commit: commit,
    commit_comment: commit_comment
  }),
  CreateEvent: event(),
  DeleteEvent: event(),
  DeploymentEvent: event({
    deployment: deployment
  }),
  DeploymentStatusEvent: event({
    deployment: deployment
  }),
  ForkEvent: event(),
  GollumEvent: event(),
  IssueCommentEvent: event({
    issue: issue,
    comment: issue_comment
  }),
  IssuesEvent: event({
    issue: issue,
    assignee: self,
    label: self
  }),
  LabelEvent: event(),
  MemberEvent: event({
    member: self
  }),
  MembershipEvent: event({
    member: self,
    team: self
  }),
  MilestoneEvent: event(),
  OrganizationEvent: event(),
  PageBuildEvent: event(),
  PublicEvent: event(),
  PullRequestEvent: event({
    pull_request: pull_request,
    issue: issue
  }),
  PullRequestReviewEvent: event({
    pull_request: pull_request
  }),
  PullRequestReviewCommentEvent: event({
    pull_request: pull_request,
    comment: review_comment
  }),
  PushEvent: event({
    commits: commit
  }),
  ReleaseEvent: event(),
  RepositoryEvent: event({
    repository: self
  }),
  StatusEvent: event(),
  TeamEvent: event({
    repository: self,
    team: self
  }),
  TeamAddEvent: event({
    repository: self,
    team: self
  }),
  WatchEvent: event()
};

const entities = {
  self: self,
  neighbors: neighbors,
  orgs: orgs,
  org: org,
  repo: repo,
  user: user,
  team: team,
  commit: commit,
  commit_comment: commit_comment,
  pull_request_commit: pull_request_commit,
  pull_request_commit_comment: pull_request_commit_comment,
  deployment: deployment,
  issue: issue,
  issue_comment: issue_comment,
  pull_request: pull_request,
  review: review,
  review_comment: review_comment,
  traffic: traffic,
  update_events: events
};

// join the entities and events then copy. That way the objects are shared and cross linked correctly
const initializeMap = Object.assign(VisitorMap.copy(Object.assign({}, entities, events)));
// for initialize assume that the repo issues are being walked explicitly.  That will bring in the pull_requests so
// no need to walk them explicitly.  Just get the PR collection.  also break the PR/issue cycle
initializeMap.repo.pull_requests = [];
initializeMap.pull_request.issue = self;
initializeMap.user.repos = [];

const relationOnlyMap = {
  MemberEvent: {
    repo: {
      collaborators: relation(self)
    }
  },
  TeamEvent: {
    repo: {
      teams: relation(self)
    }
  }
}

const mapList = {
  initialize: VisitorMap.copy(initializeMap),
  default: VisitorMap.copy(initializeMap),
  relationOnly: VisitorMap.copy(relationOnlyMap)
};

// console.dir(mapList.default);
