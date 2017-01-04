const expect = require('chai').expect;
module.exports.expectLinks = function expectLinks(actual, expected) {
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

module.exports.expectQueued = function expectQueued(actual, expected) {
  expect(actual.length).to.be.equal(expected.length);
  actual.forEach(element => {
    expect(expected.some(r => r.type === element.type && r.url === element.url)).to.be.true;
  })
}