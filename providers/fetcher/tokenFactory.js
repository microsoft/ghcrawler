// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

class TokenFactory {

  static createToken(spec) {
    const parts = spec.split('#');
    const value = parts[0];
    const traits = parts[1].split(',');
    return { value: value, traits: traits };
  }

  constructor(tokens, options) {
    this.setTokens(tokens);
    this.options = options;
  }

  setTokens(spec) {
    if (!spec) {
      this.tokens = [];
      return;
    }
    if (Array.isArray(spec)) {
      this.tokens = spec;
      return;
    }
    const tokenSpecs = spec.split(';');
    this.tokens = tokenSpecs.map(spec => TokenFactory.createToken(spec));
  }

  /**
   * Given a collection of trait sets, find the first set that has any number of matching tokens in the
   * factory.  From that set return a random one that is not on the bench. If all candidates are benched,
   * return either the soonest time one will come off the bench. If no matching tokens are found for a given
   * set, move on to the next set. If no tokens match any of the sets, return null.
   */
  getToken(desiredTraitSets) {
    desiredTraitSets = (!desiredTraitSets || desiredTraitSets.length === 0) ? [[]] : desiredTraitSets;
    for (let i = 0; i < desiredTraitSets.length; i++) {
      const token = this._getToken(desiredTraitSets[i]);
      if (token) {
        return token;
      }
    }
    return null;
  }

  _getToken(desiredTraits) {
    let minBench = Number.MAX_SAFE_INTEGER;
    const now = Date.now();
    const candidates = this.tokens.filter(token => {
      if (this._traitsMatch(token.traits, desiredTraits)) {
        if (!token.benchUntil || now > token.benchUntil) {
          return true;
        }
        minBench = Math.min(token.benchUntil, minBench);
        return false;
      }
      return false;
    });

    if (candidates.length === 0) {
      return minBench === Number.MAX_SAFE_INTEGER ? null : minBench;
    }
    const index = Math.floor(Math.random() * candidates.length);
    return candidates[index].value;
  }

  /**
   * Mark the given token as exhausted until the given time and return the time at which it will be restored.
   * If the token is already on the bench, it's restore time is unaffected. Null is returned if the token
   * could not be found.
   **/
  exhaust(value, until) {
    const now = Date.now();
    let result = null;
    this.tokens.filter(token => token.value === value).forEach(token => {
      // If the token is not benched or the bench time is passed, update the bench time. Otherwise, leave it as is.
      if (!token.benchUntil || now > token.benchUntil) {
        result = token.benchUntil = until;
      } else {
        result = token.benchUntil;
      }
    });
    return result;
  }

  // desired can be an array of traits or an array of arrays of traits if there are fall backs
  _traitsMatch(available, desired) {
    if (desired.length === 0) {
      return true;
    }
    // just a single trait.  See that it is available
    if (typeof desired === 'string') {
      return available.includes(desired);
    }
    // An array of traits. Make sure available includes them all
    if (typeof desired[0] === 'string') {
      return desired.every(trait => { return available.includes(trait); });
    }
    return false;
  }
}

module.exports = TokenFactory;