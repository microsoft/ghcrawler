// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/**






Fetch behavior
  * storageOnly - Only use stored content.  Skip this resource if we don't already have it
  * originStorage - Use stored content if it is up to date.  Otherwise, get content from original source
  * storageOriginIfMissing - Use stored content.  If missing, get content from original source
  * originOnly - Always get content from original source

Freshness --  How age of the resource, relative what we have seen/done before, factors into whether or not process the resource.
  * always - process the resource no matter what
  * match - process the resource if origin and stored docs do NOT match
  * N - process the resource if newer or if the stored copy is N days old
  * version - process the resource if the current stored doc's processing version is behind current
  * matchOrVersion - process the resource if stored and origin do not match or the stored processed version is out of date

Processing -- Which processing to do for a given resource.
  * documentAndRelated - generate links etc and queue referenced resources for further processing
  * documentAndChildren - generate links etc and queue referenced child resources (i.e., not roots) for further processing
  * documentOnly - generate links but do not queue any referenced resources

Transitivity -- How related resources should be queued.  We need to define behavior of four different relationships:
Contains, References, Is-a and contained-By (CRIB).  For each we talk about transitivity as a number, 0, 1 or 8 (inifinite).
We assume various traversal cutting techniques such as a list of visted nodes.
  * broad - 8, 1, 8, 8 an aggressive broad traversal that can start at any point in the graph. This explores all
    strong edges and ensure that weak (reference) edges have the referenced node.
  * exact - 0, 0, 0, 0  do not explore any other nodes
  * neighbors -  1,1,1,1  ensure neighbors exist

  Basically, once you are doing deep traversal, carry that through for all children, but still allow transivity
  control when traversing to a root.  A deepDeep traversal to a root will queue that root as deepShallow.  Similarly,
  when traversing with deepShallow, queued roots end up as shallow.  This approach gives you the ability to push deep
  for one level.

=============== Scenarios

Initialization -- Traverse a subgraph ensuring everything is fetched. If something has already been processed, great, assume it is up to date
* fetch = originStorage
* freshness = match
* processing = documentAndRelated
* transitivity = broad

Update -- Ensure a subgraph up to date. If something has already been processed, get it again
* fetch = originStorage
* freshness = always
* processing = documentAndRelated
* transitivity = broad

Events -- Given an event, traverse its subgraph until encountering something previously seen. This ensures the event is recorded and the related resources are present.  They may not be completely up to date.
* fetch = originStorage
* freshness = match
* processing = documentAndRelated
* transitivity = broad

Events and update -- Traverse a subgraph until encountering something previously seen.  If that
resource is older than N days, ensure the it is updated
// TODO, what is N's relation to match?
* fetch = originStorage
* freshness = N
* processing = documentAndRelated
* transitivity = broad

Just Reprocess -- Reprocess just the exact resources we have already fetched
* fetch = storageOnly
* freshness = version
* processing = documentOnly
* transitivity = [broad | exact | neighbors]

Reprocess and Rediscover -- Reprocess the resources we have and traverse to new/missing resources discovered during reprocessing.  Process those as desired.
* fetch = storageOriginIfMissing
* freshness = version
* processing = documentAndRelated
* transitivity = [broad | exact | neighbors]

Reprocess and Update -- Reprocess anything that is EITHER older version or out of date.
* fetch = originStorage
* freshness = matchOrVersion
* processing = documentAndRelated
* transitivity = [broad | exact | neighbors]

 */

const moment = require('moment');

class UpdateTransitivity {

  getNextPolicy(request, relationship) {
    const currentRelationship = request.relationship;
    if (!currentRelationship || ((currentRelationship === 'contains' || currentRelationship === 'belongsTo') && relationship === currentRelationship)) {
      return 'update';
    }
    return 'neighbors';
  }

  getShortForm() {
    return 'U';
  }

  /**
   * Given a request that would not otherwise be processed, answer whether or not its document should be
   * traversed to discover additional resources to process.
   */
  shouldTraverse(request) {
    return true;
  }
}

class BroadTransitivity {

  getNextPolicy(request, relationship) {
    const currentRelationship = request.relationship;
    if (!currentRelationship || ((currentRelationship === 'contains' || currentRelationship === 'belongsTo') && relationship === currentRelationship)) {
      return 'broad';
    }
    return 'only';
  }

  getShortForm() {
    return 'B';
  }

  /**
   * Given a request that would not otherwise be processed, answer whether or not its document should be
   * traversed to discover additional resources to process.
   */
  shouldTraverse(request) {
    return true;
  }
}

class OnlyTransitivity {

  getNextPolicy(request, relationship) {
    return null;
  }

  getShortForm() {
    return 'O';
  }

  shouldTraverse(request) {
    return false;
  }
}

class NeighborsTransitivity {

  getNextPolicy(request, relationship) {
    return 'only';
  }

  getShortForm() {
    return 'N';
  }

  shouldTraverse(request) {
    return true;
  }
}

const transitivitySpecs = {
  update: new UpdateTransitivity(),
  broad: new BroadTransitivity(),
  only: new OnlyTransitivity(),
  neighbors: new NeighborsTransitivity()
};

class TraversalPolicy {

  static getPolicy(name) {
    const definition = TraversalPolicy[name];
    return definition ? definition() : null;
  }

  static default() {
    return new TraversalPolicy('originStorage', 'match', 'broad');
  }

  static refresh() {
    return new TraversalPolicy('originStorage', 'match', 'update');
  }

  static events() {
    return TraversalPolicy.default();
  }

  static reprocess() {
    return new TraversalPolicy('storageOnly', 'version', 'update');
  }

  static reprocessAndDiscover() {
    return new TraversalPolicy('storageOriginIfMissing', 'version', 'update');
  }

  static reprocessAndUpdate() {
    return new TraversalPolicy('originStorage', 'matchOrVersion', 'update');
  }

  static clone(policy) {
    return new TraversalPolicy(policy.fetch, policy.freshness, policy.transitivity);
  }

  constructor(fetch, freshness, transitivity) {
    this.fetch = fetch;
    this.freshness = freshness;
    this.transitivity = transitivity;
  }

  getNextPolicy(request, relationship) {
    const transitivity = this._getTransitivitySpec().getNextPolicy(request, relationship);
    if (transitivity === null) {
      return null;
    }
    return new TraversalPolicy(this.fetch, this.freshness, transitivity);
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
      // process if the content we got did NOT come from the cache (i.e., either is it new or never seen)
      return request.origin !== 'cacheOfOrigin';
    }
    if (typeof this.freshness === 'number') {
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
  shouldTraverse(request) {
    return this._getTransitivitySpec().shouldTraverse(request);
  }

  /**
   * Return the source from which to perform the initial fetch for the given request's resource.
   */
  initialFetch(request) {
    const result = { storageOnly: 'storage', originStorage: 'origin', storageOriginIfMissing: 'storage', originOnly: 'origin' }[this.fetch];
    if (!result) {
      throw new Error(`Fetch policy misconfigured ${this.fetch}`);
    }
    return result;
  }

  /**
   * Return the source from which to fetch if the original fetch did not find any content
   */
  shouldFetchMissing(request) {
    const result = { storageOnly: null, originStorage: 'origin', storageOriginIfMissing: 'origin', originOnly: null }[this.fetch];
    if (result === undefined) {
      throw new Error(`Fetch policy misconfigured ${this.fetch}`);
    }
    return result;
  }

  /**
   * Return a symbolic short form to uniquely identify this policy.
   */
  getShortForm() {
    const fetch = { storageOnly: 'S', storageOriginIfMissing: 's', originOnly: 'O', originStorage: 'o' }[this.fetch];
    let freshness = { always: 'A', match: 'M', version: 'V', matchOrVersion: 'm' }[this.freshness];
    if (!freshness) {
      if (typeof this.policy.freshness === 'number') {
        freshness = 'N';
      }
    }
    const transitivity = this._getTransitivitySpec().getShortForm();
    return fetch + freshness + transitivity;
  }

  _getTransitivitySpec() {
    return transitivitySpecs[this.transitivity];
  }
}

module.exports = TraversalPolicy;