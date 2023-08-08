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
package org.apache.pinot.minion.event;

import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class MinionProgressObserverTest {
  @Test
  public void testNotifyProgressStatus() {
    MinionProgressObserver observer = new MinionProgressObserver(3);

    observer.notifyTaskStart(null);
    List<MinionProgressObserver.StatusEntry> progress = observer.getProgress();
    assertEquals(progress.size(), 1);

    observer.notifyProgress(null, "preparing input: A");
    observer.notifyProgress(null, "preparing input: B");
    observer.notifyProgress(null, "generating segment");
    progress = observer.getProgress();
    assertEquals(progress.size(), 3);

    observer.notifyProgress(null, "uploading segment");
    observer.notifyTaskError(null, new Exception("bad bug"));
    progress = observer.getProgress();
    assertEquals(progress.size(), 3);
    MinionProgressObserver.StatusEntry entry = progress.get(0);
    assertTrue(entry.getStatus().contains("generating"), entry.getStatus());
    entry = progress.get(2);
    assertTrue(entry.getStatus().contains("bad bug"), entry.getStatus());
  }

  @Test
  public void testGetStartTs() {
    MinionProgressObserver observer = new MinionProgressObserver(3);
    long ts1 = System.currentTimeMillis();
    observer.notifyTaskStart(null);
    long ts = observer.getStartTs();
    long ts2 = System.currentTimeMillis();
    assertTrue(ts1 <= ts);
    assertTrue(ts2 >= ts);
  }

  @Test
  public void testUpdateAndGetTaskState() {
    MinionProgressObserver observer = new MinionProgressObserver(3);
    assertEquals(observer.getTaskState(), MinionTaskState.UNKNOWN);
    observer.notifyTaskStart(null);
    assertEquals(observer.getTaskState(), MinionTaskState.IN_PROGRESS);
    observer.notifyProgress(null, "");
    assertEquals(observer.getTaskState(), MinionTaskState.IN_PROGRESS);
    observer.notifyTaskSuccess(null, "");
    assertEquals(observer.getTaskState(), MinionTaskState.SUCCEEDED);
    observer.notifyTaskCancelled(null);
    assertEquals(observer.getTaskState(), MinionTaskState.CANCELLED);
    observer.notifyTaskError(null, new Exception());
    assertEquals(observer.getTaskState(), MinionTaskState.ERROR);
  }
}
