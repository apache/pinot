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

  @Test
  public void testIdleTimerResetNoIdle() {
    IdleTimer timer = new IdleTimer();
    timer.markIdle(1000);
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 0);
    // same now time should not affect idle time
    timer.markIdle(1000);
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 0);
    // new now time should affect idle time
    timer.markIdle(2000);
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 1000);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 1000);
    // everything resets to 0
    timer.reset();
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 0);
  }

  @Test
  public void testOnlyResetStreamIdleTime() {
    IdleTimer timer = new IdleTimer();
    timer.markIdle(1000);
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 0);
    // same now time should not affect idle time
    timer.markIdle(1000);
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 0);
    // new now time should affect idle time
    timer.markIdle(2000);
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 1000);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 1000);
    // only stream idle time resets
    timer.markStreamNotIdle();
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 1000);
    // everything resets to 0
    timer.reset();
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 0);
  }

  @Test
  public void testMultipleIdleResets() {
    IdleTimer timer = new IdleTimer();
    timer.markIdle(1000);
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 0);
    // new now time should affect idle time
    timer.markIdle(2000);
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 1000);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 1000);
    // only stream idle time resets
    timer.markStreamNotIdle();
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 1000);
    // everything resets to 0
    timer.reset();
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 0);
    // next now time should not affect idle time
    timer.markIdle(2000);
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 0);
    // later now time should affect idle time
    timer.markIdle(3000);
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 1000);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 1000);
    // only stream idle time resets
    timer.markStreamNotIdle();
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 1000);
    // later now time should only increase consume idle time
    timer.markIdle(4000);
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 2000);
    // later now time should only increase both idle times
    timer.markIdle(5000);
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 1000);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 3000);
    // everything resets to 0
    timer.reset();
    Assert.assertEquals(timer.getStreamIdleTimeMs(), 0);
    Assert.assertEquals(timer.getConsumeIdleTimeMs(), 0);
  }
}
