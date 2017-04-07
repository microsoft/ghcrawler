// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/**
Fetch behavior -- Defines the authoritative source for content.  The first word of the name identifies the authority.
  * storageOnly - Only use stored content.  Skip this resource if we don't already have it
  * originStorage - Origin rules. Consider storage first and use it if it matches origin. Otherwise, get content from origin
  * storageOriginIfMissing - Storage rules.  Only if content is missing from storage, get content from origin
  * mutables - Use originStorage if the resource is deemed mutable, storageOriginIfMissing if immutable
  * originOnly - Always get content from original source

Freshness --  How age of the resource, relative what we have seen/done before, factors into whether or not process the resource.
  * always - process the resource no matter what
  * match - process the resource if origin and stored docs do NOT match
  * N - process the resource if newer or if the stored copy is N days old
  * version - process the resource if the current stored doc's processing version is behind current
  * matchOrVersion - process the resource if stored and origin do not match or the stored processed version is out of date

=============== Scenarios

Initialization -- Traverse a subgraph ensuring everything is fetched. If something has already been processed, great, assume it is up to date
* fetch = originStorage
* freshness = match

Update -- Ensure a subgraph up to date. If something has already been processed, get it again
* fetch = originStorage
* freshness = always

Events -- Given an event, traverse its subgraph until encountering something previously seen. This ensures the event is recorded and the related resources are present.  They may not be completely up to date.
* fetch = originStorage
* freshness = match

Events and update -- Traverse a subgraph until encountering something previously seen.  If that
resource is older than N days, ensure the it is updated
// TODO, what is N's relation to match?
* fetch = originStorage
* freshness = N

Just Reprocess -- Reprocess just the exact resources we have already fetched
* fetch = storageOnly
* freshness = version

Reprocess and Rediscover -- Reprocess the resources we have and traverse to new/missing resources discovered during reprocessing.  Process those as desired.
* fetch = storageOriginIfMissing
* freshness = version

Reprocess and Update -- Reprocess anything that is EITHER older version or out of date.
* fetch = originStorage
* freshness = matchOrVersion

A policy spec is of the form
  <policyName>[:mapSpec]
  mapSpec :: [scenario/]mapName[@p/a/t/h]
where
  * policyName identifies one of the well-known, canonical policies
  * mapSpec optionally identifies the traversal map to use. If omitted, the request.type-based map from the
    default traversal scenario is used
  * if supplied, the mapSpec identifies the map (within an optional scenario) and a path-based starting point in the map.

This arrangement allows one to apply an overall policy (e.g., freshness and fetching) to a traversal of the object graph.
The traversal is driven by the scenario such as Initialization, or Update where the graph is cut differently to suit the need.
 */

const moment = require('moment');
const VisitorMap = require('./visitorMap');

class TraversalPolicy {

  static adopt(object) {
    if (object && object.__proto__ !== TraversalPolicy.prototype) {
      object.__proto__ = TraversalPolicy.prototype;
    }
    if (object.map && object.map.__proto__ !== VisitorMap.prototype) {
      object.map.__proto__ = VisitorMap.prototype;
    }
    return object;
  }

  static _resolveMapSpec(spec) {
    if (!spec) {
      return null;
    }
    if (typeof spec !== 'string') {
      return spec;
    }
    const [mapName, path] = spec.split('@');
    return VisitorMap.getMap(mapName, path);
  }

  /**
   * A policy spec has the following form:  <policyName>[:<[scenario/]mapName[@path]].  That means a spec can be just
   * a policy name (e.g., default, reprocess, ...) in which case the map is selected from the default scenario,
   * the type of the current request is used as the mapName and the path is /.  You can also supply these values
   * and do things like 'default:self' which is a policy that only processes the referenced entity itself and
   * none of the entities it references.
   */
  static getPolicy(policySpec) {
    const [policyName, mapSpec] = policySpec.split(':');
    const map = TraversalPolicy._resolveMapSpec(mapSpec);
    if (!map) {
      return null;
    }

    const definition = TraversalPolicy[policyName];
    return definition ? definition(map) : null;
  }

  static default(map) {
    return new TraversalPolicy('mutables', 'match', TraversalPolicy._resolveMapSpec(map));
  }

  static event(map) {
    return new TraversalPolicy('mutables', 'match', TraversalPolicy._resolveMapSpec(map));
  }

  static refresh(map) {
    return new TraversalPolicy('mutables', 'match', TraversalPolicy._resolveMapSpec(map));
  }

  static reload(map) {
    return new TraversalPolicy('originStorage', 'match', TraversalPolicy._resolveMapSpec(map));
  }

  static reprocess(map) {
    return new TraversalPolicy('storageOnly', 'version', TraversalPolicy._resolveMapSpec(map));
  }

  static reprocessAndDiscover(map) {
    return new TraversalPolicy('storageOriginIfMissing', 'version', TraversalPolicy._resolveMapSpec(map));
  }

  static reprocessAndUpdate(map) {
    return new TraversalPolicy('mutables', 'matchOrVersion', TraversalPolicy._resolveMapSpec(map));
  }

  static always(map) {
    return new TraversalPolicy('originOnly', 'always', TraversalPolicy._resolveMapSpec(map));
  }

  static reprocessAlways(map) {
    return new TraversalPolicy('storageOnly', 'always', TraversalPolicy._resolveMapSpec(map));
  }

  static clone(policy) {
    return new TraversalPolicy(policy.fetch, policy.freshness, policy.map);
  }

  constructor(fetch, freshness, map) {
    this.fetch = fetch;
    this.freshness = freshness;
    this.map = typeof map === 'string' ? new VisitorMap(map) : map;
  }

  getNextPolicy(name, map = null) {
    const newMap = (map || this.map).getNextMap(name);
    if (!newMap) {
      return null;
    }
    return new TraversalPolicy(this.fetch, this.freshness, newMap);
  }

  getCurrentStep() {
    return this.map.getCurrentStep();
  }

  /**
   * Given a request for which the requisite content has been fetched, determine whether or not it needs to be
   * processed.
   */
  shouldProcess(request, version) {
    if (this.freshness === 'always') {
      return true;
    }
    if (this.freshness === 'match') {
      // process if the content came from origin then either we did not have it cached or it did not match.  Process
      return request.contentOrigin === 'origin';
    }
    if (typeof this.freshness === 'number') {
      // TODO this is not quite right. To tell time freshness we need to get the cached version but if we need to process
      // we need the content from origin. Essentially we need to read the processed time with the etag (at that point)
      // determine if the content is stale.  Testing here is too late.
      return moment.diff(request.document._metadata.processedAt, 'hours') > this.freshness * 24;
    }
    if (this.freshness === 'version' || this.freshness === 'matchOrVersion') {
      return !request.document._metadata.version || (request.document._metadata.version < version);
    }
    throw new Error('Invalid freshness in traversal policy');
  }

  /**
   * Given a request that would not otherwise be processed, answer whether or not its document should be
   * traversed to discover additional resources to process.
   */
  shouldTraverse() {
    return this.map.hasNextStep();
  }

  isImmutable(type) {
    return ['commit'].includes(type);
  }

  /**
   * Return the source from which to perform the initial fetch for the given request's resource.
   */
  initialFetch(request) {
    const mutablesValue = this.isImmutable(request.type) ? 'storage' : 'etag';
    const result = { storageOnly: 'storage', originStorage: 'etag', originMutable: 'storage', storageOriginIfMissing: 'storage', mutables: mutablesValue, originOnly: 'origin' }[this.fetch];
    if (!result) {
      throw new Error(`Fetch policy misconfigured ${this.fetch}`);
    }
    return result;
  }

  /**
   * Return the source from which to fetch if the original fetch did not find any content
   */
  shouldFetchMissing(request) {
    const result = { storageOnly: null, originStorage: 'origin', storageOriginIfMissing: 'origin', mutables: 'origin', originOnly: null }[this.fetch];
    if (result === undefined) {
      throw new Error(`Fetch policy misconfigured ${this.fetch}`);
    }
    return result;
  }

  /**
   * Return a symbolic short form to uniquely identify this policy.
   */
  getShortForm() {
    const fetch = { storageOnly: 'S', storageOriginIfMissing: 's', originOnly: 'O', originStorage: 'o', mutables: 'm' }[this.fetch];
    let freshness = { always: 'A', match: 'M', version: 'V', matchOrVersion: 'm' }[this.freshness];
    if (!freshness) {
      if (typeof this.policy.freshness === 'number') {
        freshness = 'N';
      }
    }
    return fetch + freshness;
  }

}

module.exports = TraversalPolicy;
