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
package org.apache.pinot.segment.spi.index.startree;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class StarTreeV2MetadataTest {

  @Test
  public void testUniqueAggregationSpecs() {
    TreeMap<AggregationFunctionColumnPair, AggregationSpec> expected = new TreeMap<>();
    expected.put(AggregationFunctionColumnPair.fromColumnName("count__*"), AggregationSpec.DEFAULT);
    expected.put(AggregationFunctionColumnPair.fromColumnName("sum__dimX"), AggregationSpec.DEFAULT);

    Configuration metadataProperties = createMetadata(List.of("dimX"), expected);
    StarTreeV2Metadata starTreeV2Metadata = new StarTreeV2Metadata(metadataProperties);
    TreeMap<AggregationFunctionColumnPair, AggregationSpec> actual = starTreeV2Metadata.getAggregationSpecs();
    assertEquals(actual, expected);
  }

  @Test
  public void testDuplicateAggregationSpecs() {
    AggregationFunctionColumnPair thetaColumnPair =
        AggregationFunctionColumnPair.fromColumnName("distinctCountThetaSketch__dimX");
    AggregationFunctionColumnPair rawThetaColumnPair =
        AggregationFunctionColumnPair.fromColumnName("distinctCountRawThetaSketch__dimX");

    TreeMap<AggregationFunctionColumnPair, AggregationSpec> expected = new TreeMap<>();
    expected.put(thetaColumnPair, AggregationSpec.DEFAULT);
    expected.put(rawThetaColumnPair, AggregationSpec.DEFAULT);

    Configuration metadataProperties = createMetadata(List.of("dimX"), expected);
    StarTreeV2Metadata starTreeV2Metadata = new StarTreeV2Metadata(metadataProperties);
    TreeMap<AggregationFunctionColumnPair, AggregationSpec> actual = starTreeV2Metadata.getAggregationSpecs();
    expected.remove(rawThetaColumnPair);
    assertEquals(actual, expected);
    assertTrue(starTreeV2Metadata.containsFunctionColumnPair(thetaColumnPair));
  }

  @Test
  public void testUniqueFunctionColumnPairs() {
    Set<AggregationFunctionColumnPair> expected = new HashSet<>();
    expected.add(AggregationFunctionColumnPair.fromColumnName("count__*"));
    expected.add(AggregationFunctionColumnPair.fromColumnName("sum__dimX"));

    Configuration metadataProperties = createMetadata(List.of("dimX"), expected);
    StarTreeV2Metadata starTreeV2Metadata = new StarTreeV2Metadata(metadataProperties);
    Set<AggregationFunctionColumnPair> actual = starTreeV2Metadata.getFunctionColumnPairs();
    assertEquals(actual, expected);
  }

  @Test
  public void testDuplicateFunctionColumnPairs() {
    AggregationFunctionColumnPair thetaColumnPair =
        AggregationFunctionColumnPair.fromColumnName("distinctCountThetaSketch__dimX");
    AggregationFunctionColumnPair rawThetaColumnPair =
        AggregationFunctionColumnPair.fromColumnName("distinctCountRawThetaSketch__dimX");

    Set<AggregationFunctionColumnPair> expected = new HashSet<>();
    expected.add(thetaColumnPair);
    expected.add(rawThetaColumnPair);

    Configuration metadataProperties = createMetadata(Collections.singletonList("dimX"), expected);
    StarTreeV2Metadata starTreeV2Metadata = new StarTreeV2Metadata(metadataProperties);
    Set<AggregationFunctionColumnPair> actual = starTreeV2Metadata.getFunctionColumnPairs();

    expected.remove(rawThetaColumnPair);
    assertEquals(actual, expected);
    assertTrue(starTreeV2Metadata.containsFunctionColumnPair(thetaColumnPair));
  }

  private static Configuration createMetadata(List<String> dimensionsSplitOrder,
      TreeMap<AggregationFunctionColumnPair, AggregationSpec> aggregationSpecs) {
    Configuration metadataProperties = new PropertiesConfiguration();
    StarTreeV2Metadata.writeMetadata(metadataProperties, 1, dimensionsSplitOrder, aggregationSpecs, 10000, Set.of());
    return metadataProperties;
  }

  // This is the old star-tree metadata format
  private static Configuration createMetadata(List<String> dimensionsSplitOrder,
      Set<AggregationFunctionColumnPair> functionColumnPairs) {
    Configuration metadataProperties = new PropertiesConfiguration();
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.TOTAL_DOCS, 1);
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.DIMENSIONS_SPLIT_ORDER, dimensionsSplitOrder);
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.FUNCTION_COLUMN_PAIRS, functionColumnPairs);
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.MAX_LEAF_RECORDS, 10000);
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS, Set.of());
    return metadataProperties;
  }
}
