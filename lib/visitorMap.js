// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

class VisitorMap {

  static getCopy(name) {
    return VisitorMap.copy(VisitorMap._getMap(name));
  }

  static copy(node, seen = new Map()) {
    if (typeof node === 'string' || typeof node === 'function') {
      return node;
    }
    if (seen.get(node)) {
      return seen.get(node);
    }
    // if (Array.isArray(node)) {
    //   if (Array.isArray(node[0])) {
    //     return [[VisitorMap.copy(node[0][0], seen)]];
    //   }
    //   return [VisitorMap.copy(node[0], seen)];
    // }
    const result = {};
    seen.set(node, result);
    for (let key in node) {
      const value = node[key];
      if (typeof value === 'function') {
        result[key] = value;
      }
      result[key] = VisitorMap.copy(value, seen);
    }
    return result;
  }

  static resolve(step, segment) {
    return typeof step === 'function' ? step(segment) : step[segment];
  }

  static _getMap(name) {
    return mapList[name] || mapList.event[name];
  }

  static getMap(name, path = '/') {
    return name ? new VisitorMap(name, path) : null;
  }

  constructor(name, path = '/') {
    this.name = name;
    this.path = path;
  }

  getNextMap(next) {
    const separator = this.path.endsWith('/') ? '' : '/';
    return this.hasNextStep(next) ? new VisitorMap(this.name, this.path + `${separator}${next}`) : null;
  }

  getNextStep(next) {
    const current = this.getCurrentStep();
    return this.navigate(current, next);
  }

  hasNextStep(next = null) {
    const current = this.getCurrentStep();
    // arrays trigger the traversal of a collection/relation but not their contents.  Terminal nodes only
    if (Array.isArray(current)) {
      return false;
    }
    const props = Object.getOwnPropertyNames(current);
    if (props.length === 0) {
      return false;
    }
    return next ? true : props.includes(next);
  }

  getCurrentStep() {
    const map = this.getMap();
    if (!map) {
      throw new Error(`VisitorMap in an invalid state.  Unknown map: ${this.name}`);
    }
    return this.navigate(this.getMap(), this.getPath());
  }

  navigate(map, path) {
    if (!map) {
      throw new Error('VisitorMap in an invalid state.  Unknown map.')
    }
    path = this._resolvePath(path);
    let current = map;
    let currentPath = [];
    for (let i = 0; i < path.length; i++) {
      const segment = path[i];
      currentPath.push(segment);
      current = VisitorMap.resolve(current, segment);
      if (!current) {
        return current;
      }
    }
    return current;
  }

  getMap() {
    return VisitorMap._getMap(this.name);
  }

  getPath() {
    return this._resolvePath(this.path);
  }

  _resolvePath(spec) {
    if (Array.isArray(spec)) {
      return spec;
    }
    if (spec === '/') {
      return [];
    }
    return spec.split('/').slice(spec.startsWith('/') ? 1 : 0);
  }
}

module.exports = VisitorMap;


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

// /org/repos/collaborators = [], {}
//

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
}

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
  review: review,
  review_comments: review_comment,
  statuses: collection(status),
  commits: collection(commit),
  issue: issue,
  issue_comments: collection(issue_comment)
}
// patch pull_request into issue as it is a cycle.
issue.pull_request = pull_request;

const deployment = {
  _type: 'deployment',
  created_by: self
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
}

const repo = {
  _type: 'repo',
  owner: self,
  organization: self,
  teams: relation(team),
  collaborators: relation(self),
  contributors: relation(self),
  subscribers: relation(self),
  issues: collection(issue),
  commits: collection(commit),
  events: collection(event)
}

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


function event(additions = {}) {
  const base = {
    actor: self,
    repo: self,
    org: self
  };
  return Object.assign({}, base, additions);
}

const eventList = {
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
    issue_comment: issue_comment
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
  PageBuildEvent: event(),
  PublicEvent: event(),
  PullRequestEvent: event({
    pull_request: pull_request
  }),
  PullRequestReviewEvent: event({
    pull_request: pull_request
  }),
  PullRequestReviewCommentEvent: event({
    pull_request: pull_request,
    comment: review_comment
  }),
  PushEvent: event(),
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

const mapList = VisitorMap.copy({
  self: self,
  neighbors: neighbors,
  event: eventList,
  org: org,
  repo: repo,
  user: user,
  team: team,
  commit: commit,
  commit_comment: commit_comment,
  deployment: deployment,
  issue: issue,
  issue_comment: issue_comment,
  pull_request: pull_request,
  review: review,
  review_comment: review_comment,
  traffic: traffic
});
