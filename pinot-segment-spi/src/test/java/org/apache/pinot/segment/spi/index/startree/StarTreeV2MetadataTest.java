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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


public class StarTreeV2MetadataTest {

  @Test
  public void testUniqueAggregationSpecs() {
    TreeMap<AggregationFunctionColumn, AggregationSpec> expected = new TreeMap<>();
    expected.put(AggregationFunctionColumn.fromColumnName("count__*"), AggregationSpec.DEFAULT);
    expected.put(AggregationFunctionColumn.fromColumnName("sum__dimX"), AggregationSpec.DEFAULT);

    Configuration metadataProperties = createMetadata(List.of("dimX"), expected);
    StarTreeV2Metadata starTreeV2Metadata = new StarTreeV2Metadata(metadataProperties);
    TreeMap<AggregationFunctionColumn, AggregationSpec> actual = starTreeV2Metadata.getAggregationSpecs();
    assertEquals(expected, actual);
  }

  @Test
  public void testDuplicateAggregationSpecs() {
    AggregationFunctionColumn thetaColumnPair =
        AggregationFunctionColumn.fromColumnName("distinctCountThetaSketch__dimX");
    AggregationFunctionColumn rawThetaColumnPair =
        AggregationFunctionColumn.fromColumnName("distinctCountRawThetaSketch__dimX");

    TreeMap<AggregationFunctionColumn, AggregationSpec> expected = new TreeMap<>();
    expected.put(thetaColumnPair, AggregationSpec.DEFAULT);
    expected.put(rawThetaColumnPair, AggregationSpec.DEFAULT);

    Configuration metadataProperties = createMetadata(List.of("dimX"), expected);
    StarTreeV2Metadata starTreeV2Metadata = new StarTreeV2Metadata(metadataProperties);
    TreeMap<AggregationFunctionColumn, AggregationSpec> actual = starTreeV2Metadata.getAggregationSpecs();
    expected.remove(rawThetaColumnPair);
    assertEquals(expected, actual);
    assertTrue(starTreeV2Metadata.containsFunctionColumn(thetaColumnPair));
  }

  @Test
  public void testUniqueFunctionColumnPairs() {
    Set<AggregationFunctionColumn> expected = new HashSet<>();
    expected.add(AggregationFunctionColumn.fromColumnName("count__*"));
    expected.add(AggregationFunctionColumn.fromColumnName("sum__dimX"));

    Configuration metadataProperties = createMetadata(List.of("dimX"), expected);
    StarTreeV2Metadata starTreeV2Metadata = new StarTreeV2Metadata(metadataProperties);
    Set<AggregationFunctionColumn> actual = starTreeV2Metadata.getFunctionColumns();
    assertEquals(expected, actual);
  }

  @Test
  public void testDuplicateFunctionColumnPairs() {
    AggregationFunctionColumn thetaColumnPair =
        AggregationFunctionColumn.fromColumnName("distinctCountThetaSketch__dimX");
    AggregationFunctionColumn rawThetaColumnPair =
        AggregationFunctionColumn.fromColumnName("distinctCountRawThetaSketch__dimX");

    Set<AggregationFunctionColumn> expected = new HashSet<>();
    expected.add(thetaColumnPair);
    expected.add(rawThetaColumnPair);

    Configuration metadataProperties = createMetadata(Collections.singletonList("dimX"), expected);
    StarTreeV2Metadata starTreeV2Metadata = new StarTreeV2Metadata(metadataProperties);
    Set<AggregationFunctionColumn> actual = starTreeV2Metadata.getFunctionColumns();

    expected.remove(rawThetaColumnPair);
    assertEquals(expected, actual);
    assertTrue(starTreeV2Metadata.containsFunctionColumn(thetaColumnPair));
  }

  private static Configuration createMetadata(List<String> dimensionsSplitOrder,
      TreeMap<AggregationFunctionColumn, AggregationSpec> aggregationSpecs) {
    Configuration metadataProperties = new PropertiesConfiguration();
    StarTreeV2Metadata.writeMetadata(metadataProperties, 1, dimensionsSplitOrder, aggregationSpecs, 10000, Set.of());
    return metadataProperties;
  }

  // This is the old star-tree metadata format
  private static Configuration createMetadata(List<String> dimensionsSplitOrder,
      Set<AggregationFunctionColumn> functionColumns) {
    Configuration metadataProperties = new PropertiesConfiguration();
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.TOTAL_DOCS, 1);
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.DIMENSIONS_SPLIT_ORDER, dimensionsSplitOrder);
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.FUNCTION_COLUMN_PAIRS, functionColumns);
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.MAX_LEAF_RECORDS, 10000);
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS, Set.of());
    return metadataProperties;
  }
}
