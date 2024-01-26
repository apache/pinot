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
import java.util.Map;
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
    TreeMap<AggregationFunctionColumnPair, AggregationSpec> expected = new TreeMap<>();
    expected.put(AggregationFunctionColumnPair.fromColumnName("count__*"), AggregationSpec.DEFAULT);
    expected.put(AggregationFunctionColumnPair.fromColumnName("sum__dimX"), AggregationSpec.DEFAULT);

    Configuration configuration = createConfiguration(Collections.singletonList("dimX"), null, expected);
    StarTreeV2Metadata starTreeV2Metadata = new StarTreeV2Metadata(configuration);
    TreeMap<AggregationFunctionColumnPair, AggregationSpec> actual = starTreeV2Metadata.getAggregationSpecs();
    assertEquals(expected, actual);
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

    Configuration configuration = createConfiguration(Collections.singletonList("dimX"), null, expected);
    StarTreeV2Metadata starTreeV2Metadata = new StarTreeV2Metadata(configuration);
    TreeMap<AggregationFunctionColumnPair, AggregationSpec> actual = starTreeV2Metadata.getAggregationSpecs();
    expected.remove(rawThetaColumnPair);
    assertEquals(expected, actual);
    assertTrue(starTreeV2Metadata.containsFunctionColumnPair(thetaColumnPair));
  }

  @Test
  public void testUniqueFunctionColumnPairs() {
    Set<AggregationFunctionColumnPair> expected = new HashSet<>();
    expected.add(AggregationFunctionColumnPair.fromColumnName("count__*"));
    expected.add(AggregationFunctionColumnPair.fromColumnName("sum__dimX"));

    Configuration configuration = createConfiguration(Collections.singletonList("dimX"), expected, null);
    StarTreeV2Metadata starTreeV2Metadata = new StarTreeV2Metadata(configuration);
    Set<AggregationFunctionColumnPair> actual = starTreeV2Metadata.getFunctionColumnPairs();
    assertEquals(expected, actual);
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

    Configuration configuration = createConfiguration(Collections.singletonList("dimX"), expected, null);
    StarTreeV2Metadata starTreeV2Metadata = new StarTreeV2Metadata(configuration);
    Set<AggregationFunctionColumnPair> actual = starTreeV2Metadata.getFunctionColumnPairs();

    expected.remove(rawThetaColumnPair);
    assertEquals(expected, actual);
    assertTrue(starTreeV2Metadata.containsFunctionColumnPair(thetaColumnPair));
  }

  private static Configuration createConfiguration(List<String> dimensionsSplitOrder,
      Set<AggregationFunctionColumnPair> functionColumnPairs,
      TreeMap<AggregationFunctionColumnPair, AggregationSpec> aggregationSpecs) {
    Configuration metadataProperties = new PropertiesConfiguration();
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.TOTAL_DOCS, 1);
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.DIMENSIONS_SPLIT_ORDER, dimensionsSplitOrder);
    if (functionColumnPairs != null) {
      metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.FUNCTION_COLUMN_PAIRS, functionColumnPairs);
      metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.AGGREGATION_COUNT, 0);
    } else {
      metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.AGGREGATION_COUNT, aggregationSpecs.size());
      int index = 0;
      for (Map.Entry<AggregationFunctionColumnPair, AggregationSpec> entry : aggregationSpecs.entrySet()) {
        AggregationFunctionColumnPair functionColumnPair = entry.getKey();
        AggregationSpec aggregationSpec = entry.getValue();
        String prefix = StarTreeV2Constants.MetadataKey.AGGREGATION_PREFIX + index + '.';
        metadataProperties.setProperty(prefix + StarTreeV2Constants.MetadataKey.FUNCTION_TYPE,
            functionColumnPair.getFunctionType().getName());
        metadataProperties.setProperty(prefix + StarTreeV2Constants.MetadataKey.COLUMN_NAME,
            functionColumnPair.getColumn());
        metadataProperties.setProperty(prefix + StarTreeV2Constants.MetadataKey.COMPRESSION_CODEC,
            aggregationSpec.getCompressionType());
        index++;
      }
    }
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.MAX_LEAF_RECORDS, 10000);
    metadataProperties.setProperty(StarTreeV2Constants.MetadataKey.SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS,
        new HashSet<>());
    return metadataProperties;
  }
}
