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
package org.apache.pinot.plugin.stream.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaPartitionSubsetUtilsTest {

  @Test
  public void testGetPartitionIdsFromConfigMissingKey() {
    Map<String, String> config = new HashMap<>();
    config.put("stream.kafka.topic.name", "myTopic");
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
    Assert.assertNull(result);
  }

  @Test
  public void testGetPartitionIdsFromConfigBlankValue() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS), "  ");
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
    Assert.assertNull(result);
  }

  @Test
  public void testGetPartitionIdsFromConfigEmptyAfterTrim() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS), ",");
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
    Assert.assertNull(result);
  }

  @Test
  public void testGetPartitionIdsFromConfigValidSubset() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        "0,2,5");
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, List.of(0, 2, 5));
  }

  @Test
  public void testGetPartitionIdsFromConfigUnsortedReturnsSorted() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        "5, 2 , 0");
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, List.of(0, 2, 5));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetPartitionIdsFromConfigInvalidNumber() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        "0,abc,1");
    KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetPartitionIdsFromConfigInvalidNumberOnly() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        "not_a_number");
    KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetPartitionIdsFromConfigNegativePartitionId() {
    Map<String, String> config = new HashMap<>();
    config.put(
        KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        "-1,0,1");
    KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
  }

  @Test
  public void testGetPartitionIdsFromConfigSinglePartition() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS), "3");
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, List.of(3));
  }

  @Test
  public void testGetPartitionIdsFromConfigEmptyMap() {
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(Collections.emptyMap());
    Assert.assertNull(result);
  }

  @Test
  public void testGetPartitionIdsFromConfigDedupesDuplicates() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        "2,0,2,5,0,5");
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, List.of(0, 2, 5));
  }

  @Test
  public void testGetPartitionIdsFromConfigMultipleCommasWithWhitespace() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        "  ,  ,  0  ,  ,  ");
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, List.of(0));
  }

  @Test
  public void testGetPartitionIdsFromConfigLeadingAndTrailingCommas() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        ",,,0,1,2,,,");
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, List.of(0, 1, 2));
  }

  @Test
  public void testGetPartitionIdsFromConfigAllEmptyCommas() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        " , , , , ");
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
    Assert.assertNull(result);
  }

  @Test
  public void testGetPartitionIdsFromConfigVeryLargePartitionId() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        "999,1000,9999");
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, List.of(999, 1000, 9999));
  }

  @Test
  public void testGetPartitionIdsFromConfigAllPartitionsInSubset() {
    // Simulates a subset config that contains all partitions (e.g., 0,1,2,3 for a 4-partition topic)
    // This is valid config-wise but might not be useful in practice
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        "0,1,2,3");
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, List.of(0, 1, 2, 3));
  }

  @Test
  public void testGetPartitionIdsFromConfigMixedWhitespace() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        " 5 , 2 ,\t0\t,\n3\n");
    List<Integer> result = KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, List.of(0, 2, 3, 5));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetPartitionIdsFromConfigPartiallyInvalid() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        "0,1,2,invalid,3");
    KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetPartitionIdsFromConfigNegativeInMiddle() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS),
        "0,1,-5,2");
    KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(config);
  }
}
