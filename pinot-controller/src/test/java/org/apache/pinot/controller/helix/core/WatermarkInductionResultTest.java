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
package org.apache.pinot.controller.helix.core;

import java.util.List;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class WatermarkInductionResultTest {

  @Test
  public void testTopicIdRoundTrip()
      throws Exception {
    WatermarkInductionResult result =
        new WatermarkInductionResult(List.of(new WatermarkInductionResult.Watermark(1, 2, 3, 100L)));
    String json = JsonUtils.objectToString(result);
    WatermarkInductionResult deserialized = JsonUtils.stringToObject(json, WatermarkInductionResult.class);

    assertEquals(deserialized.getWatermarks().size(), 1);
    WatermarkInductionResult.Watermark watermark = deserialized.getWatermarks().get(0);
    assertEquals(watermark.getPartitionGroupId(), 1);
    assertEquals(watermark.getTopicId(), 2);
    assertEquals(watermark.getSequenceNumber(), 3);
    assertEquals(watermark.getOffset(), 100L);
  }

  @Test
  public void testTopicIdDefaultsToZeroWhenAbsentFromJson()
      throws Exception {
    // Simulates a pre-migration payload (e.g. from an older controller) that has no topicId field.
    String legacyJson = "{\"watermarks\":[{\"partitionGroupId\":5,\"sequenceNumber\":6,\"offset\":200}]}";
    WatermarkInductionResult deserialized = JsonUtils.stringToObject(legacyJson, WatermarkInductionResult.class);

    assertEquals(deserialized.getWatermarks().size(), 1);
    WatermarkInductionResult.Watermark watermark = deserialized.getWatermarks().get(0);
    assertEquals(watermark.getPartitionGroupId(), 5);
    assertEquals(watermark.getTopicId(), 0);
    assertEquals(watermark.getSequenceNumber(), 6);
    assertEquals(watermark.getOffset(), 200L);
  }
}
