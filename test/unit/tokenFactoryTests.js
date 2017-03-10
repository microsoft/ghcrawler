// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const expect = require('chai').expect;
const TokenFactory = require('../../providers/fetcher/tokenFactory');

describe('Token Factory', () => {
  it('should find a token with multiple desired traits', () => {
    const factory = new TokenFactory('1111#admin,private,push;2222#public', null);
    let token = null;

    token = factory.getToken([]);
    expect(token).to.be.not.null;

    token = factory.getToken();
    expect(token).to.be.not.null;

    token = factory.getToken([['admin'], ['admin'], ['public']]);
    expect(token).to.be.equal('1111');

    token = factory.getToken(['public']);
    expect(token).to.be.equal('2222');
  });
});
