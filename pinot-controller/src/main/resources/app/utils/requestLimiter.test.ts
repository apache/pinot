/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import assert from 'node:assert/strict';
import test from 'node:test';
import { runWithConcurrencyLimit } from './requestLimiter';

const nextTick = () => new Promise((resolve) => setTimeout(resolve, 0));

test('runWithConcurrencyLimit caps in-flight tasks', async () => {
  let active = 0;
  let maxActive = 0;
  const releaseTasks: (() => void)[] = [];

  const tasks = Array.from({ length: 10 }, (_, index) => async () => {
    active++;
    maxActive = Math.max(maxActive, active);
    await new Promise<void>((resolve) => {
      releaseTasks.push(resolve);
    });
    active--;
    return index;
  });

  const resultPromise = runWithConcurrencyLimit(tasks, 3);

  await nextTick();
  assert.equal(maxActive, 3);

  while (releaseTasks.length > 0) {
    const release = releaseTasks.shift();
    release?.();
    await nextTick();
  }

  assert.deepEqual(await resultPromise, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
});
