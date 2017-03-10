// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

let force = false;
let token = null;

function initialize(tokenValue, forceValue = false) {
  force = forceValue;
  token = tokenValue;
}
exports.initialize = initialize;

function validate(request, response, next) {
  // if running on localhost, don't bother to validate
  if ((!token || process.env.NODE_ENV === 'localhost') && !force) {
    return next();
  }

  // TODO temporary poor man's token management
  if (request.header('X-token') === token) {
    return next();
  }
  response.status(403).send('Authentication required');
}
exports.validate = validate;
