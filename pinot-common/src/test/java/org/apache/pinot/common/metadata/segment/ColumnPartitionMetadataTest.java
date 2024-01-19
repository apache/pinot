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
package org.apache.pinot.common.metadata.segment;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.convert.LegacyListDelimiterHandler;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ColumnPartitionMetadataTest {
  private static final String FUNCTION_NAME = "murmur";
  private static final int NUM_PARTITIONS = 10;
  private static final Set<Integer> PARTITIONS = new HashSet<>(Arrays.asList(1, 2, 3, 5));
  private static final String LEGACY_METADATA_JSON =
      "{\"functionName\":\"murmur\",\"numPartitions\":10,\"partitionRanges\":\"[1 3],[5 5]\"}";
  private static final String LEGACY_PARTITION_RANGES_STRING = "[1 3],[5 5]";

  @Test
  public void testSerDe()
      throws Exception {
    ColumnPartitionMetadata expected = new ColumnPartitionMetadata(FUNCTION_NAME, NUM_PARTITIONS, PARTITIONS, null);
    ColumnPartitionMetadata actual =
        JsonUtils.stringToObject(JsonUtils.objectToString(expected), ColumnPartitionMetadata.class);
    assertEquals(actual, expected);
  }

  @Test
  public void testLegacyMetadataDeserialization()
      throws Exception {
    ColumnPartitionMetadata expected = new ColumnPartitionMetadata(FUNCTION_NAME, NUM_PARTITIONS, PARTITIONS, null);
    ColumnPartitionMetadata actual = JsonUtils.stringToObject(LEGACY_METADATA_JSON, ColumnPartitionMetadata.class);
    assertEquals(actual, expected);
  }

  @Test
  public void testPartitionsConfig() {
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setProperty("partitions", PARTITIONS);
    Set<Integer> actual = ColumnPartitionMetadata.extractPartitions(config.getList("partitions"));
    assertEquals(actual, PARTITIONS);
  }

  @Test
  public void testLegacyPartitionRangesConfig() {
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setListDelimiterHandler(new LegacyListDelimiterHandler(','));
    config.setProperty("partitionRanges", LEGACY_PARTITION_RANGES_STRING);
    Set<Integer> actual = ColumnPartitionMetadata.extractPartitions(config.getList("partitionRanges"));
    assertEquals(actual, PARTITIONS);
  }
}
