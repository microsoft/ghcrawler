// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

const VisitorMap = require('../').visitorMap;

// Map building blocks
const self = {};

function neighbors() {
  return self;
}

const scancode = self;

const source = {
  _type: 'source',
  scancode
}

const npm = {
  _type: 'npm',
  source
};

const maven = {
  _type: 'maven',
  source
};

const entities = {
  self,
  neighbors,
  orgs,
  source,
  npm,
  maven
};

VisitorMap.register('initialize', VisitorMap.copy(entities));
VisitorMap.register('default', VisitorMap.copy(entities));
