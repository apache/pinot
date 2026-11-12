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
package org.apache.pinot.controller.helix;

import java.util.Map;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.utils.NetUtils;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


/// Verifies bounded test-port searches without binding host ports. Tests have no shared mutable state; the static
/// availability mocks are scoped to the executing thread.
public class TestPortRangeTest {
  @Test
  public void testControllerConfigurationsExhaustOnlyControllerRange() {
    ControllerTest fixture = new ControllerTest() {
      @Override
      public String getZkUrl() {
        return "localhost:2181";
      }
    };
    int controllerStart = fixture._controllerPortRange.getStart();
    int brokerStart = fixture._brokerPortRange.getStart();
    assertEquals(brokerStart - controllerStart, 100);

    try (MockedStatic<NetUtils> netUtils = mockStatic(NetUtils.class)) {
      netUtils.when(() -> NetUtils.available(anyInt())).thenReturn(true);
      for (int i = 0; i < 100; i++) {
        Map<String, Object> configuration = fixture.getDefaultControllerConfiguration();
        assertEquals(configuration.get(ControllerConf.CONTROLLER_PORT), controllerStart + i);
      }

      netUtils.clearInvocations();
      expectThrows(IllegalStateException.class, fixture::getDefaultControllerConfiguration);
      netUtils.verifyNoInteractions();
    }
  }

  @Test
  public void testSkipsOccupiedPortsWithinRange() {
    TestPortRange range = new TestPortRange("controller", 21000, 21100);
    try (MockedStatic<NetUtils> netUtils = mockStatic(NetUtils.class)) {
      netUtils.when(() -> NetUtils.available(21002)).thenReturn(true);

      assertEquals(range.findOpenPort(range.getStart()), 21002);

      netUtils.verify(() -> NetUtils.available(21000));
      netUtils.verify(() -> NetUtils.available(21001));
      netUtils.verify(() -> NetUtils.available(21002));
      netUtils.verifyNoMoreInteractions();
    }
  }

  @Test
  public void testExhaustedRangeDoesNotProbeNextComponent() {
    TestPortRange range = new TestPortRange("server", 21400, 21800);
    try (MockedStatic<NetUtils> netUtils = mockStatic(NetUtils.class)) {
      netUtils.when(() -> NetUtils.available(21800)).thenReturn(true);

      IllegalStateException exception = expectThrows(IllegalStateException.class, () -> range.findOpenPort(21799));
      assertEquals(exception.getMessage(), "No available port for server in range [21400, 21800) starting at 21799");

      netUtils.verify(() -> NetUtils.available(21799));
      netUtils.verifyNoMoreInteractions();
    }
  }

  @Test
  public void testExhaustedCursorDoesNotProbe() {
    TestPortRange range = new TestPortRange("minion", 21800, 21900);
    try (MockedStatic<NetUtils> netUtils = mockStatic(NetUtils.class)) {
      for (int startPort : new int[]{21900, 21901, Integer.MAX_VALUE}) {
        IllegalStateException exception =
            expectThrows(IllegalStateException.class, () -> range.findOpenPort(startPort));
        assertEquals(exception.getMessage(),
            "No available port for minion in range [21800, 21900) starting at " + startPort);
      }
      netUtils.verifyNoInteractions();
    }
  }

  @Test
  public void testRejectsCursorBelowRange() {
    TestPortRange range = new TestPortRange("broker", 21100, 21200);
    try (MockedStatic<NetUtils> netUtils = mockStatic(NetUtils.class)) {
      expectThrows(IllegalArgumentException.class, () -> range.findOpenPort(21099));
      netUtils.verifyNoInteractions();
    }
  }

  @Test
  public void testHighestTcpPortIsIncluded() {
    TestPortRange range = new TestPortRange("controller", 65535, 65536);
    try (MockedStatic<NetUtils> netUtils = mockStatic(NetUtils.class)) {
      netUtils.when(() -> NetUtils.available(65535)).thenReturn(true);

      assertEquals(range.findOpenPort(range.getStart()), 65535);

      netUtils.verify(() -> NetUtils.available(65535));
      netUtils.verifyNoMoreInteractions();
    }
  }

  @DataProvider
  public Object[][] invalidRanges() {
    return new Object[][]{{0, 100}, {-1, 100}, {100, 100}, {101, 100}, {65535, 65537}, {65536, 65537}};
  }

  @Test(dataProvider = "invalidRanges")
  public void testRejectsInvalidRange(int start, int end) {
    expectThrows(IllegalArgumentException.class, () -> new TestPortRange("controller", start, end));
  }
}
