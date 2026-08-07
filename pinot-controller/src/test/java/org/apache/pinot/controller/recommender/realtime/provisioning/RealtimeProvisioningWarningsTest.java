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
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link RealtimeProvisioningWarnings} sanity checks (#8339).
 */
public class RealtimeProvisioningWarningsTest {

  private static final long MAX_HOST_MEMORY = DataSizeUtils.toBytes("48G");

  @Test
  public void testNoWarningsForModestEstimates() {
    String[][] segmentSize = {{"100M", "200M"}, {"150M", "250M"}};
    String[][] activeMemory = {{"10G/12G", "8G/10G"}, {"12G/14G", "9G/11G"}};
    String[][] numSegments = {{"100", "50"}, {"80", "40"}};

    List<String> warnings =
        RealtimeProvisioningWarnings.generate(segmentSize, activeMemory, numSegments, MAX_HOST_MEMORY);

    assertTrue(warnings.isEmpty(), "Expected no warnings, got: " + warnings);
    assertEquals(RealtimeProvisioningWarnings.formatForDisplay(warnings), "");
  }

  @Test
  public void testLargeSegmentSizeWarning() {
    // 5G segment — the motivating case from #8339
    String[][] segmentSize = {{"100M", "5G"}, {"200M", "1G"}};
    String[][] activeMemory = {{"10G/12G", "20G/24G"}, {"12G/14G", "22G/26G"}};
    String[][] numSegments = {{"100", "20"}, {"80", "15"}};
    int[] numHours = {2, 4};
    int[] numHosts = {3, 6};

    List<String> warnings =
        RealtimeProvisioningWarnings.generate(segmentSize, activeMemory, numSegments, MAX_HOST_MEMORY, numHours,
            numHosts);

    assertEquals(warnings.size(), 1);
    assertTrue(warnings.get(0).contains("segment sizes in the matrix are large"));
    assertTrue(warnings.get(0).contains("5G"));
    assertTrue(warnings.get(0).contains("numHours=2,numHosts=6"));
    assertTrue(warnings.get(0).contains(RealtimeProvisioningWarnings.REALTIME_TUNING_DOCS_URL));
  }

  @Test
  public void testSegmentSizeAtThresholdDoesNotWarn() {
    String threshold = DataSizeUtils.fromBytes(RealtimeProvisioningWarnings.LARGE_SEGMENT_SIZE_BYTES);
    String[][] segmentSize = {{threshold}};
    String[][] activeMemory = {{"1G/2G"}};
    String[][] numSegments = {{"10"}};

    List<String> warnings =
        RealtimeProvisioningWarnings.generate(segmentSize, activeMemory, numSegments, MAX_HOST_MEMORY);

    assertTrue(warnings.isEmpty(), "Size exactly at threshold should not warn, got: " + warnings);
  }

  @Test
  public void testMemoryOverHostLimitWarning() {
    String[][] segmentSize = {{"100M", "200M"}};
    // 60G active exceeds 48G max
    String[][] activeMemory = {{"10G/12G", "60G/80G"}};
    String[][] numSegments = {{"100", "50"}};
    int[] numHours = {6};
    int[] numHosts = {2, 4};

    List<String> warnings =
        RealtimeProvisioningWarnings.generate(segmentSize, activeMemory, numSegments, MAX_HOST_MEMORY, numHours,
            numHosts);

    assertEquals(warnings.size(), 1);
    assertTrue(warnings.get(0).contains("exceeds max usable host memory"));
    assertTrue(warnings.get(0).contains("60G"));
    assertTrue(warnings.get(0).contains("48G"));
    assertTrue(warnings.get(0).contains("numHours=6,numHosts=4"));
  }

  @Test
  public void testHighSegmentCountWarning() {
    String[][] segmentSize = {{"50M"}};
    String[][] activeMemory = {{"5G/6G"}};
    String[][] numSegments = {{"6000"}};

    List<String> warnings =
        RealtimeProvisioningWarnings.generate(segmentSize, activeMemory, numSegments, MAX_HOST_MEMORY);

    assertEquals(warnings.size(), 1);
    assertTrue(warnings.get(0).contains("very high number of segments"));
    assertTrue(warnings.get(0).contains("6000"));
  }

  @Test
  public void testNaCellsExplained() {
    String[][] segmentSize = {{"100M", MemoryEstimator.NOT_APPLICABLE}};
    String[][] activeMemory = {{"10G/12G", MemoryEstimator.NOT_APPLICABLE}};
    String[][] numSegments = {{"100", MemoryEstimator.NOT_APPLICABLE}};

    List<String> warnings =
        RealtimeProvisioningWarnings.generate(segmentSize, activeMemory, numSegments, MAX_HOST_MEMORY);

    assertEquals(warnings.size(), 1);
    assertTrue(warnings.get(0).contains("NA"));
    assertTrue(warnings.get(0).contains("retention hours"));
  }

  @Test
  public void testMultipleWarningsAndDisplayFormat() {
    String[][] segmentSize = {{"5G", MemoryEstimator.NOT_APPLICABLE}};
    String[][] activeMemory = {{"100G/120G", MemoryEstimator.NOT_APPLICABLE}};
    String[][] numSegments = {{"9000", MemoryEstimator.NOT_APPLICABLE}};

    List<String> warnings =
        RealtimeProvisioningWarnings.generate(segmentSize, activeMemory, numSegments, MAX_HOST_MEMORY);

    assertEquals(warnings.size(), 4);

    String display = RealtimeProvisioningWarnings.formatForDisplay(warnings);
    assertTrue(display.contains("Warnings"));
    assertTrue(display.contains("1)"));
    assertTrue(display.contains("2)"));
    assertTrue(display.contains("3)"));
    assertTrue(display.contains("4)"));
    assertTrue(display.contains(RealtimeProvisioningWarnings.REALTIME_TUNING_DOCS_URL));
    assertFalse(display.isEmpty());
  }

  @Test
  public void testSkipsMalformedCells() {
    String[][] segmentSize = {{"not-a-size", "300M"}};
    String[][] activeMemory = {{"bogus", "10G/12G"}};
    String[][] numSegments = {{"x", "20"}};

    List<String> warnings =
        RealtimeProvisioningWarnings.generate(segmentSize, activeMemory, numSegments, MAX_HOST_MEMORY);

    assertTrue(warnings.isEmpty(), "Malformed cells should be ignored, got: " + warnings);
  }
}
