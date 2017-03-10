// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const Q = require('q');

function wrap(genFn) {
  var cr = Q.async(genFn);
  return function (req, res, next) {
    cr(req, res, next).catch(next);
  };
}

module.exports = wrap;