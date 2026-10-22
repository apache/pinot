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
package org.apache.pinot.tools.admin.command;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.coordination.CoordinationBackend;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.tools.service.PinotServiceManager;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Ensures normal bootstrap can form a native voter quorum while preserving controller-before-participant ordering.
public class StartServiceManagerCommandTest {
  @Test(timeOut = 10_000)
  public void testNativeVotersStartTogetherAfterSequentialHelixControllers() throws Exception {
    PinotServiceManager manager = mock(PinotServiceManager.class);
    CountDownLatch nativeVoters = new CountDownLatch(3);
    AtomicInteger helixControllers = new AtomicInteger();
    AtomicInteger completedVoters = new AtomicInteger();
    AtomicInteger participants = new AtomicInteger();
    Thread bootstrapThread = Thread.currentThread();
    when(manager.startRole(any(), any())).thenAnswer(invocation -> {
      ServiceRole role = invocation.getArgument(0);
      Map<String, Object> config = invocation.getArgument(1);
      if (role == ServiceRole.CONTROLLER) {
        if ("RATIS".equals(config.get(CoordinationBackend.CONFIG_KEY))) {
          assertEquals(helixControllers.get(), 2);
          nativeVoters.countDown();
          assertTrue(nativeVoters.await(5, TimeUnit.SECONDS), "Voters must start together to form a quorum");
          completedVoters.incrementAndGet();
        } else {
          assertSame(Thread.currentThread(), bootstrapThread, "Default Helix controllers remain synchronous");
          assertEquals(helixControllers.incrementAndGet(), ((Number) config.get("ordinal")).intValue());
        }
      } else {
        assertEquals(helixControllers.get(), 2);
        assertEquals(completedVoters.get(), 3, "Participants must wait for every native voter startup");
        participants.incrementAndGet();
      }
      return role.toString();
    });
    List<Entry<ServiceRole, Map<String, Object>>> configurations = List.of(
        Map.entry(ServiceRole.BROKER, Map.of()),
        Map.entry(ServiceRole.CONTROLLER, Map.of(CoordinationBackend.CONFIG_KEY, "RATIS")),
        Map.entry(ServiceRole.CONTROLLER, Map.of("ordinal", 1)),
        Map.entry(ServiceRole.SERVER, Map.of()),
        Map.entry(ServiceRole.CONTROLLER, Map.of(CoordinationBackend.CONFIG_KEY, "RATIS")),
        Map.entry(ServiceRole.CONTROLLER, Map.of("ordinal", 2)),
        Map.entry(ServiceRole.CONTROLLER, Map.of(CoordinationBackend.CONFIG_KEY, "RATIS")));
    assertTrue(StartServiceManagerCommand.startBootstrapServices(manager, configurations));
    assertEquals(participants.get(), 2);
  }
}
