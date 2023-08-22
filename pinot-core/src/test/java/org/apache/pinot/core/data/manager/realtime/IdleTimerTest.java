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

package org.apache.pinot.core.data.manager.realtime;

import org.testng.Assert;
import org.testng.annotations.Test;


public class IdleTimerTest {

  private static class StaticIdleTimer extends IdleTimer {

    private long _nowTimeMs = 0;

    public StaticIdleTimer() {
      super();
    }

    @Override
    protected long now() {
      return _nowTimeMs;
    }

    public void setNowTimeMs(long nowTimeMs) {
      _nowTimeMs = nowTimeMs;
    }
  }

  @Test
  public void testIdleTimerResetNoIdle() {
    StaticIdleTimer timer = new StaticIdleTimer();
    // idle times are all 0 before init
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 0);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 0);
    // start times are all 1000L
    timer.setNowTimeMs(1000L);
    timer.init();
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 0);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 0);
    // new now time should affect idle time
    timer.setNowTimeMs(2000L);
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 1000);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 1000);
    // everything resets to 2000
    timer.init();
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 0);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 0);
  }

  @Test
  public void testOnlyResetStreamIdleTime() {
    StaticIdleTimer timer = new StaticIdleTimer();
    timer.setNowTimeMs(1000L);
    timer.init();
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 0);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 0);
    // only stream idle time resets
    timer.setNowTimeMs(2000L);
    timer.markStreamCreated();
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 0);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 1000);
    // everything resets to 0
    timer.init();
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 0);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 0);
  }

  @Test
  public void testMultipleIdleResets() {
    StaticIdleTimer timer = new StaticIdleTimer();
    timer.setNowTimeMs(1000L);
    timer.init();
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 0);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 0);
    // new now time should affect idle time
    timer.setNowTimeMs(2000L);
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 1000);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 1000);
    // only stream idle time resets
    timer.markStreamCreated();
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 0);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 1000);
    // everything resets to 0
    timer.setNowTimeMs(3000L);
    timer.markEventConsumed();
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 0);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 0);
    // later now times should affect idle time
    timer.setNowTimeMs(4000L);
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 1000);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 1000);
    // only stream idle time resets
    timer.setNowTimeMs(5000L);
    timer.markStreamCreated();
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 0);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 2000);
    // later now time should only increase both idle times
    timer.setNowTimeMs(6000L);
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 1000);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 3000);
    // everything resets to 0
    timer.init();
    Assert.assertEquals(timer.getTimeSinceStreamLastCreatedOrConsumedMs(), 0);
    Assert.assertEquals(timer.getTimeSinceEventLastConsumedMs(), 0);
  }
}
