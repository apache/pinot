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
package org.apache.pinot.core.query.scheduler.tokenbucket;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class TokenSchedulerGroupTest {

  private int _timeMillis = 100;

  class TestTokenSchedulerGroup extends TokenSchedulerGroup {
    private static final int NUM_TOKENS_PER_MS = 100;
    private static final int TOKEN_LIFETIME_MS = 100;

    TestTokenSchedulerGroup() {
      super("testGroup", NUM_TOKENS_PER_MS, TOKEN_LIFETIME_MS);
    }

    @Override
    public long currentTimeMillis() {
      return _timeMillis;
    }
  }

  @Test
  public void testIncrementThreads()
      throws Exception {
    // set test time first
    _timeMillis = 100;
    TestTokenSchedulerGroup group = new TestTokenSchedulerGroup();

    int availableTokens = group.getAvailableTokens();
    // verify token count is correctly set
    assertEquals(availableTokens,
        TestTokenSchedulerGroup.NUM_TOKENS_PER_MS * TestTokenSchedulerGroup.TOKEN_LIFETIME_MS);

    // no threads in use...incrementing time has no effect
    _timeMillis += 2 * TestTokenSchedulerGroup.TOKEN_LIFETIME_MS;
    availableTokens = group.getAvailableTokens();
    int startTime = _timeMillis;
    assertEquals(availableTokens,
        TestTokenSchedulerGroup.NUM_TOKENS_PER_MS * TestTokenSchedulerGroup.TOKEN_LIFETIME_MS);

    int nThreads = 1;
    incrementThreads(group, nThreads);
    assertEquals(group.getThreadsInUse(), nThreads);
    assertEquals(group.getAvailableTokens(), availableTokens);

    // advance time
    int timeIncrement = 20;
    _timeMillis += timeIncrement;
    group.decrementThreads();
    assertEquals(group.getThreadsInUse(), 0);
    assertEquals(group.getAvailableTokens(), availableTokens - timeIncrement * nThreads);

    // more threads
    availableTokens = group.getAvailableTokens();
    nThreads = 5;
    incrementThreads(group, nThreads);
    assertEquals(group.getThreadsInUse(), nThreads);
    // advance time now
    _timeMillis += timeIncrement;
    assertEquals(group.getAvailableTokens(), availableTokens - timeIncrement * nThreads);

    // simple getAvailableTokens() updates tokens and reservedThreads has no effect
    group.addReservedThreads(2 * nThreads);
    availableTokens = group.getAvailableTokens();
    timeIncrement = 10;
    _timeMillis += timeIncrement;
    assertEquals(group.getAvailableTokens(), availableTokens - timeIncrement * nThreads);
    availableTokens = group.getAvailableTokens();

    // decrement some threads
    decrementThreads(group, 2);
    nThreads -= 2;
    _timeMillis += timeIncrement;
    assertEquals(group.getAvailableTokens(), availableTokens - timeIncrement * nThreads);

    // 3 threads still in use. Advance time beyond time quantum
    availableTokens = group.getAvailableTokens();
    int pendingTimeInQuantum = startTime + TestTokenSchedulerGroup.TOKEN_LIFETIME_MS - _timeMillis;
    _timeMillis = startTime + TestTokenSchedulerGroup.TOKEN_LIFETIME_MS + timeIncrement;
    int timeAdvance = pendingTimeInQuantum + timeIncrement;
    // these are "roughly" the tokens in use since we apply decay. So we don't test for exact value
    int expectedTokens =
        TestTokenSchedulerGroup.NUM_TOKENS_PER_MS * TestTokenSchedulerGroup.TOKEN_LIFETIME_MS - timeAdvance * nThreads;
    assertTrue(group.getAvailableTokens() < expectedTokens);
    availableTokens = group.getAvailableTokens();

    // increment by multiple quantums
    _timeMillis = startTime + 3 * TestTokenSchedulerGroup.TOKEN_LIFETIME_MS + timeIncrement;
    expectedTokens = TestTokenSchedulerGroup.NUM_TOKENS_PER_MS * TestTokenSchedulerGroup.TOKEN_LIFETIME_MS
        - timeIncrement * nThreads;
    assertTrue(group.getAvailableTokens() < expectedTokens);
  }

  @Test
  public void testStartStopQuery() {
    _timeMillis = 100;
    TestTokenSchedulerGroup group = new TestTokenSchedulerGroup();
    assertEquals(group.numRunning(), 0);
    assertEquals(group.numPending(), 0);
    assertEquals(group.getThreadsInUse(), 0);
    group.startQuery();
    assertEquals(group.numRunning(), 1);
    assertEquals(group.getThreadsInUse(), 1);

    group.endQuery();
    assertEquals(group.numRunning(), 0);
    assertEquals(group.getThreadsInUse(), 0);
  }

  private void incrementThreads(TokenSchedulerGroup group, int nThreads) {
    for (int i = 0; i < nThreads; i++) {
      group.incrementThreads();
    }
  }

  private void decrementThreads(TokenSchedulerGroup group, int nThreads) {
    for (int i = 0; i < nThreads; i++) {
      group.decrementThreads();
    }
  }
}
