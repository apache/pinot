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
package org.apache.pinot.common.compression;

import java.util.List;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies column compression contribution aggregation and public DTO conversion.
public class ColumnCompressionStatsAccumulatorTest {
  @Test
  public void testMixedEncodingAggregation() {
    ColumnCompressionStatsAccumulator accumulator = new ColumnCompressionStatsAccumulator();
    accumulator.add(9_000, 2_000, EncodingType.RAW, ChunkCompressionType.LZ4, List.of("forward_index"), 3);
    accumulator.add(1_000, 1_500, EncodingType.DICTIONARY, null,
        List.of("dictionary", "forward_index"), 2);

    ColumnCompressionStatsInfo info = accumulator.toColumnCompressionStatsInfo("mixed");
    assertEquals(info.getUncompressedValueSizeInBytes(), 10_000L);
    assertEquals(info.getForwardIndexAndDictionaryStorageSizeInBytes(), 3_500L);
    assertEquals(info.getNumSegments(), 5);
    assertEquals(info.getEncodingBreakdown().size(), 2);
    assertTrue(info.getObservedIndexes().contains("dictionary"));
  }

  @Test
  public void testUnavailableContributionIsIgnoredButZeroIsRetained() {
    ColumnCompressionStatsAccumulator accumulator = new ColumnCompressionStatsAccumulator();
    accumulator.add(-1, 100, EncodingType.RAW, ChunkCompressionType.LZ4, null, 1);
    assertEquals(accumulator.toColumnCompressionStatsInfo("empty").getNumSegments(), 0);

    accumulator.add(0, 0, EncodingType.RAW, ChunkCompressionType.PASS_THROUGH, null, 1);
    assertEquals(accumulator.toColumnCompressionStatsInfo("empty").getNumSegments(), 1);
  }
}
