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
package org.apache.pinot.broker.api.resources;

import java.util.List;
import java.util.function.Predicate;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.broker.stats.BrokerTableStatsManager;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class PinotBrokerStatisticsTest {

  private static final String SERVED_TABLE = "served_OFFLINE";
  private static final String DROPPED_TABLE = "dropped_OFFLINE";

  private AutoCloseable _mocks;

  @Mock
  private BrokerTableStatsManager _statsManager;

  @Mock
  private BrokerRoutingManager _routingManager;

  @InjectMocks
  private PinotBrokerStatistics _resource;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testPurgeReportsTheTablesItDropped() {
    when(_statsManager.purgeTablesNoLongerServed(any())).thenReturn(List.of(DROPPED_TABLE));
    assertEquals(_resource.purgeOrphanedStatistics(), List.of(DROPPED_TABLE));
  }

  @Test
  public void testPurgeDecidesLivenessFromTheRoutingTable() {
    when(_statsManager.purgeTablesNoLongerServed(any())).thenReturn(List.of());
    when(_routingManager.routingExists(SERVED_TABLE)).thenReturn(true);
    when(_routingManager.routingExists(DROPPED_TABLE)).thenReturn(false);

    _resource.purgeOrphanedStatistics();

    // The predicate decides what gets deleted, so assert it is actually wired to routing: an
    // inverted or wrongly-sourced predicate would silently purge tables the broker still serves.
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Predicate<String>> captor = ArgumentCaptor.forClass(Predicate.class);
    org.mockito.Mockito.verify(_statsManager).purgeTablesNoLongerServed(captor.capture());
    Predicate<String> stillServed = captor.getValue();
    assertTrue(stillServed.test(SERVED_TABLE));
    assertFalse(stillServed.test(DROPPED_TABLE));
  }

  @Test
  public void testPurgeReports404WhenStatisticsAreDisabled() {
    // A broker with pinot.broker.stats.enabled=false has no manager bound at all, so the field is
    // left unset rather than holding a disabled instance.
    PinotBrokerStatistics disabled = new PinotBrokerStatistics();
    WebApplicationException e = expectThrows(WebApplicationException.class, disabled::purgeOrphanedStatistics);
    assertEquals(e.getResponse().getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    verifyNoInteractions(_statsManager);
  }
}
