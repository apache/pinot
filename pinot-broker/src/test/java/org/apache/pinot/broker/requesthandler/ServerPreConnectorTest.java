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
package org.apache.pinot.broker.requesthandler;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ServerPreConnectorTest {
  private static final long ONE_MINUTE_MS = 60_000L;

  private static List<ServerInstance> mockServers(int count) {
    List<ServerInstance> servers = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      servers.add(mock(ServerInstance.class));
    }
    return servers;
  }

  private static long farDeadline() {
    return System.currentTimeMillis() + ONE_MINUTE_MS;
  }

  @Test
  public void connectsEveryServerForBothTableTypes() {
    List<ServerInstance> servers = mockServers(3);
    Set<TableType> tableTypesSeen = ConcurrentHashMap.newKeySet();
    Set<Integer> serversSeen = ConcurrentHashMap.newKeySet();
    AtomicInteger calls = new AtomicInteger();

    int connected = new ServerPreConnector(() -> servers, (server, tableType) -> {
      calls.incrementAndGet();
      tableTypesSeen.add(tableType);
      serversSeen.add(System.identityHashCode(server));
      return true;
    }).preConnect(farDeadline());

    // 3 servers x 2 table types.
    assertEquals(connected, 6);
    assertEquals(calls.get(), 6);
    assertEquals(serversSeen.size(), 3);
    assertEquals(tableTypesSeen, EnumSet.of(TableType.OFFLINE, TableType.REALTIME));
  }

  @Test
  public void emptyServerListReturnsZeroWithoutConnecting() {
    AtomicInteger calls = new AtomicInteger();
    int connected = new ServerPreConnector(List::of, (server, tableType) -> {
      calls.incrementAndGet();
      return true;
    }).preConnect(farDeadline());

    assertEquals(connected, 0);
    assertEquals(calls.get(), 0);
  }

  @Test
  public void deadlineAlreadyPassedReturnsZeroWithoutConnecting() {
    List<ServerInstance> servers = mockServers(2);
    AtomicInteger calls = new AtomicInteger();
    int connected = new ServerPreConnector(() -> servers, (server, tableType) -> {
      calls.incrementAndGet();
      return true;
    }).preConnect(System.currentTimeMillis() - 1);

    assertEquals(connected, 0);
    assertEquals(calls.get(), 0);
  }

  @Test
  public void countsOnlySuccessfulConnects() {
    List<ServerInstance> servers = mockServers(4);
    // OFFLINE succeeds, REALTIME fails: exactly one successful channel per server.
    int connected = new ServerPreConnector(() -> servers,
        (server, tableType) -> tableType == TableType.OFFLINE).preConnect(farDeadline());

    assertEquals(connected, 4);
  }

  @Test
  public void connectFailureIsSwallowedAndOthersStillConnect() {
    List<ServerInstance> servers = mockServers(5);
    AtomicInteger attempts = new AtomicInteger();
    // Every REALTIME attempt throws; the method must not propagate it and must still connect OFFLINE.
    int connected = new ServerPreConnector(() -> servers, (server, tableType) -> {
      attempts.incrementAndGet();
      if (tableType == TableType.REALTIME) {
        throw new RuntimeException("connect blew up");
      }
      return true;
    }).preConnect(farDeadline());

    assertEquals(connected, 5);        // only the 5 OFFLINE channels
    assertEquals(attempts.get(), 10);  // all 10 were still attempted
  }

  @Test
  public void respectsBudgetAndDoesNotWaitForSlowConnects() {
    List<ServerInstance> servers = mockServers(4);
    // Each connect is far slower than the budget; preConnect must return near the budget, not wait for
    // the connects, and must not throw.
    long budgetMs = 400L;
    long startMs = System.currentTimeMillis();
    int connected = new ServerPreConnector(() -> servers, (server, tableType) -> {
      try {
        Thread.sleep(5_000L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      return true;
    }).preConnect(System.currentTimeMillis() + budgetMs);
    long elapsedMs = System.currentTimeMillis() - startMs;

    assertEquals(connected, 0);
    // Comfortably below the 5s connect: proves the budget bounded the wait rather than blocking on
    // the slow connects.
    assertTrue(elapsedMs < 3_000L, "preConnect took " + elapsedMs + " ms, expected it to honor the budget");
  }
}
