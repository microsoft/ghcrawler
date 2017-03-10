// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const assert = require('chai').assert;
const expect = require('chai').expect;
const sinon = require('sinon');
const VisitorMap = require('../../lib/visitorMap');

describe('Visitor Map', () => {

  it('will get next', () => {
    const map = new VisitorMap('org');
    const node = map.getNextStep('repos');
    expect(node._type).to.be.equal('repo');
  });

  it('will get next giving a self', () => {
    const map = new VisitorMap('org', '/repos');
    const node = map.getNextStep('owner');
    expect(Object.getOwnPropertyNames(node).length).to.be.equal(0);
  });

  it('will return undefined for next of self', () => {
    const map = new VisitorMap('org', '/repos');
    const node = map.getNextStep('foo');
    expect(node).to.be.undefined;
  });

  it('will return undefined for random next', () => {
    const map = new VisitorMap('org');
    const node = map.getNextStep('boo');
    expect(node).to.be.undefined;
  });


  // it('will get next for collection', () => {
  //   const map = new VisitorMap('org');
  //   const node = map.getNextPolicy('repos');
  //   expect(node[0]._type).to.be.equal('repo');
  // });

  // it('will resolve collections', () => {
  //   const map = new VisitorMap('org');
  //   const node = map.getNextPolicy('repos');
  //   expect(VisitorMap.resolve(node)._type).to.be.equal('repo');
  // });
});
