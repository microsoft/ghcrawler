// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const express = require('express');
const htmlencode = require('htmlencode').htmlEncode;

function create() {
  return function (request, response, next) {
    response.helpers = response.helpers || {};
    response.helpers.send = {
      context: {
        request: request,
        response: response
      },
      noContent: noContent,
      partialHtml: partialHtml
    };
    next();
  };
}
module.exports = create;

function noContent() {
  this.context.response.sendStatus(204).end();
}

function partialHtml(title, html) {
  this.context.response.type('html').status(200).end('<html><head><title>' + htmlencode(title) + '</title></head><body>' + html + '</body></html>');
}