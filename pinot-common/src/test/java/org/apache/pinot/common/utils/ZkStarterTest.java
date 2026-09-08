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
package org.apache.pinot.common.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;


/// Tests lifecycle properties of [ZkStarter]'s asynchronous client cleanup.
public class ZkStarterTest {
  @Test
  public void testAsyncCloserUsesDaemonThread()
      throws Exception {
    ZkClient client = mock(ZkClient.class);
    CountDownLatch closeCalled = new CountDownLatch(1);
    AtomicBoolean daemonThread = new AtomicBoolean();
    doAnswer(invocation -> {
      daemonThread.set(Thread.currentThread().isDaemon());
      closeCalled.countDown();
      return null;
    }).when(client).close();

    ZkStarter.closeAsync(client);

    assertThat(closeCalled.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(daemonThread.get()).isTrue();
  }
}
