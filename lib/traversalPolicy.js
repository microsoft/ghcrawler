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
  * documentOnly - generate links

Transitivity -- How related resources should be queued.
  * shallow - Queue all non-roots and roots with the current Freshness policy
  * deepShallow - Queue NON-roots as always and roots with the current Freshness policy, Roots will have transitivity set to shallow.
  * deepDeep - Queue all related resources as always. Roots will have transitivity set to deepShallow

  Basically, once you are doind deep traversal, carry that through for all children, but still allow transivity
  control when traversing to a root.  A deepDeep traversal to a root will queue that root as deepShallow.  Similarly,
  when traversing with deepShallow, queued roots end up as shallow.  This approach gives you the ability to push deep
  for one level.

=============== Scenarios

Initialization -- Traverse a subgraph ensuring everything is fetched. If something has already been processed, great, assume it is up to date
* fetch = originStorage
* freshness = match
* processing = documentAndRelated
* transitivity = [deepShallow | deepDeep]

Update -- Ensure a subgraph up to date. If something has already been processed, get it again
* fetch = originStorage
* freshness = always
* processing = documentAndRelated
* transitivity = [deepShallow | deepDeep]

Events -- Given an event, traverse its subgraph until encountering something previously seen. This ensures the event is recorded and the related resources are present.  They may not be completely up to date.
* fetch = originStorage
* freshness = match
* processing = documentAndRelated
* transitivity = shallow

Events and update -- Traverse a subgraph until encountering something previously seen.  If that
resource is older than N days, ensure the it is updated
// TODO, what is N's relation to match?
* fetch = originStorage
* freshness = N
* processing = documentAndRelated
* transitivity = shallow

Just Reprocess -- Reprocess just the exact resources we have already fetched
* fetch = storageOnly
* freshness = version
* processing = documentOnly
* transitivity = NA

Reprocess and Rediscover -- Reprocess the resources we have and traverse to new/missing resources discovered during reprocessing.  Process those as desired.
* fetch = storageOriginIfMissing
* freshness = version
* processing = documentAndRelated
* transitivity = <any>

Reprocess and Update -- Reprocess anything that is EITHER older version or out of date.
* fetch = originStorage
* freshness = matchOrVersion
* processing = documentAndRelated
* transitivity = <any>

 */

const moment = require('moment');

class TraversalPolicy {

  static default() {
    return new TraversalPolicy('originStorage', 'match', 'documentAndRelated', 'shallow');
  }

  static update() {
    return new TraversalPolicy('originStorage', 'always', 'documentAndRelated', 'deepDeep');
  }

  static events() {
    return TraversalPolicy.default();
  }

  static clone(policy) {
    return new TraversalPolicy(policy.fetch, policy.freshness, policy.processing, policy.transitivity);
  }

  constructor(fetch, freshness, processing, transitivity) {
    this.fetch = fetch;
    this.freshness = freshness;
    this.processing = processing;
    this.transitivity = transitivity;
  }

  /**
   * Create and return a new policy suitable for use a root object reached from processing
   * a resource with this resource.
   */
  createPolicyForRoot() {
    if (this.processing === 'documentOnly' || this.processing === 'documentAndChildren') {
      return null;
    }
    const transitivity = { shallow: 'shallow', deepShallow: 'shallow', deepDeep: 'deepShallow' }[this.transitivity];
    return new TraversalPolicy(this.fetch, this.freshness, this.processing, transitivity);
  }

  /**
   * Create and return a new policy suitable for use by a child object reached from processing
   * a resource with this resource.
   */
  createPolicyForChild() {
    if (this.processing === 'documentOnly') {
      return null;
    }
    const transitivity = { shallow: 'shallow', deepShallow: 'deepShallow', deepDeep: 'deepShallow' }[this.transitivity];
    const freshness = { shallow: this.freshness, deepShallow: this.freshness, deepDeep: 'always' }[this.transitivity];
    return new TraversalPolicy(this.fetch, freshness, this.processing, transitivity);
  }

  /**
   * Given a request for which the requisite content has been fetched, determine whether or not processing
   * should happen.
   */
  shouldProcess(request, version) {
    // Note: if the freshness is match, we only got here if the content should be processed.
    if (this.freshness === 'always' || this.freshness === 'match') {
      return true;
    }
    if (typeof this.freshness === 'number') {
      return moment.diff(request.document._metadata.processedAt, 'hours') > this.freshness * 24;
    }
    if (this.freshness === 'version' || this.freshness === 'matchOrVersion') {
      return !request.document._metadata.version || (version > request.document._metadata.version);
    }
    throw new Error('Invalid freshness in traversal policy');
  }

  /**
   * Given a request for which we have found existing content in our doc store, say whether or not that existing
   * content should be fetched. The only case where we do NOT fetch is match.  All others require the actual cached
   * content to determine whether or not processing is required.
   */
  fetchExisting(request) {
    return this.freshness !== 'match';
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
  missingFetch(request) {
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
    const processing = { documentOnly: 'D', documentAndRelated: 'r', documentAndChildren: 'c' }[this.processing];
    const transitivity = { shallow: 'S', deepShallow: 'd', deepDeep: 'D' }[this.transitivity];
    return fetch + freshness + processing + transitivity;
  }
}

module.exports = TraversalPolicy;