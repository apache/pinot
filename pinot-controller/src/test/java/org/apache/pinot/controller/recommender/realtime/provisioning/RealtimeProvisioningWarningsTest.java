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
package org.apache.pinot.controller.recommender.realtime.provisioning;

import java.util.List;
import org.apache.pinot.controller.recommender.realtime.provisioning.RealtimeProvisioningWarnings.Estimate;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class RealtimeProvisioningWarningsTest {
  private static final long MIB = 1024 * 1024;
  private static final long GIB = 1024 * MIB;
  private static final long MAX_HOST_MEMORY = 48 * GIB;

  @Test
  public void testNoWarningsAtThresholds() {
    Estimate[][] estimates = {{new Estimate(500 * MIB, MAX_HOST_MEMORY, 5000)}};

    List<String> warnings = RealtimeProvisioningWarnings.generate(estimates, MAX_HOST_MEMORY, new int[]{2},
        new int[]{3});

    assertTrue(warnings.isEmpty(), warnings.toString());
    assertEquals(RealtimeProvisioningWarnings.formatForDisplay(warnings), "");
  }

  @Test
  public void testWarningsUseUnroundedBytesAndExactSegmentCount() {
    long segmentLimit = 500 * MIB;
    long segmentSize = segmentLimit + 1;
    long activeMemory = MAX_HOST_MEMORY + 1;
    assertEquals(DataSizeUtils.fromBytes(segmentSize), DataSizeUtils.fromBytes(segmentLimit));
    assertEquals(DataSizeUtils.fromBytes(activeMemory), DataSizeUtils.fromBytes(MAX_HOST_MEMORY));

    Estimate[][] estimates = {{new Estimate(segmentSize, activeMemory, 5001)}};
    List<String> warnings = RealtimeProvisioningWarnings.generate(estimates, MAX_HOST_MEMORY, new int[]{2},
        new int[]{3});

    assertEquals(warnings.size(), 3);
    assertTrue(warnings.get(0).contains(segmentSize + " bytes at numHours=2,numHosts=3"), warnings.toString());
    assertTrue(warnings.get(0).contains("threshold " + segmentLimit + " bytes"), warnings.toString());
    assertTrue(warnings.get(1).contains(activeMemory + " bytes at numHours=2,numHosts=3"), warnings.toString());
    assertTrue(warnings.get(1).contains("exceeds max usable host memory (" + MAX_HOST_MEMORY + " bytes)"),
        warnings.toString());
    assertTrue(warnings.get(2).contains("5001 at numHours=2,numHosts=3"), warnings.toString());
  }

  @Test
  public void testMaximaTieOrderAndInapplicableRows() {
    Estimate[][] estimates = {
        {new Estimate(100 * MIB, 50 * GIB, 5010), new Estimate(5 * GIB, 60 * GIB, 100)},
        {new Estimate(5 * GIB, 40 * GIB, 9000), new Estimate(400 * MIB, 30 * GIB, 7000)},
        {null, null}
    };

    List<String> warnings = RealtimeProvisioningWarnings.generate(estimates, MAX_HOST_MEMORY, new int[]{2, 4, 8},
        new int[]{3, 6});

    assertEquals(warnings.size(), 4);
    assertTrue(warnings.get(0).contains("5G at numHours=2,numHosts=6"), warnings.toString());
    assertTrue(warnings.get(1).contains("60G at numHours=2,numHosts=6"), warnings.toString());
    assertTrue(warnings.get(2).contains("9000 at numHours=4,numHosts=3"), warnings.toString());
    assertTrue(warnings.get(3).contains("NA because numHoursToConsume exceeds the retention hours"),
        warnings.toString());

    String display = RealtimeProvisioningWarnings.formatForDisplay(warnings);
    for (int i = 0; i < warnings.size(); i++) {
      assertTrue(display.contains((i + 1) + ") " + warnings.get(i)), display);
    }
    assertTrue(display.contains(RealtimeProvisioningWarnings.REALTIME_TUNING_DOCS_URL), display);
  }

  @Test
  public void testNoHostsProducesNoWarnings() {
    Estimate[][] estimates = {{}};
    assertTrue(RealtimeProvisioningWarnings.generate(estimates, MAX_HOST_MEMORY, new int[]{2}, new int[]{}).isEmpty());
  }
}
