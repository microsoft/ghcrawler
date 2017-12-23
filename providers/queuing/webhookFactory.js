
// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

module.exports = (manager, options) => {
  return manager.createQueueChain('events', null, options);
}