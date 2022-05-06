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
package org.apache.pinot.broker.broker.helix;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


@SuppressWarnings("UnstableApiUsage")
public class ClusterChangeMediatorTest {
  private final Lock _lock = new ReentrantLock();

  /**
   * Tests the potential deadlock between Helix callback handling thread and ClusterChangeMediator change handling
   * thread when the ClusterChangeMediator is stopped.
   * The deadlock chain is as following:
   * - ClusterChangeMediator.stop() is called and waiting for the change handling thread to stop
   * - Helix callback handling thread acquires a lock, then send a cluster change to the mediator which is blocked
   *   ClusterChangeMediator.stop() (both stop() and enqueueChange() are synchronized)
   * - The change handling thread waits on the lock held by the Helix callback handling thread
   */
  @Test
  public void testDeadLock() {
    ClusterChangeMediator mediator = new ClusterChangeMediator(
        Collections.singletonMap(ChangeType.IDEAL_STATE, Collections.singletonList(new Handler())),
        mock(BrokerMetrics.class));
    mediator.start();

    new Thread(() -> {
      sendClusterChange(mediator);
      Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
      sendClusterChange(mediator);
    }).start();

    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    mediator.stop();
  }

  private void sendClusterChange(ClusterChangeMediator mediator) {
    _lock.lock();
    try {
      mediator.onIdealStateChange(Collections.emptyList(), mock(NotificationContext.class));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      _lock.unlock();
    }
  }

  private class Handler implements ClusterChangeHandler {

    @Override
    public void init(HelixManager helixManager) {
    }

    @Override
    public void processClusterChange(ChangeType changeType) {
      Uninterruptibles.sleepUninterruptibly(300, TimeUnit.MILLISECONDS);
      _lock.lock();
      _lock.unlock();
    }
  }
}
