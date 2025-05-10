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
package org.apache.pinot.segment.local.startree.v2.builder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.local.aggregator.DistinctCountCPCSketchValueAggregator;
import org.apache.pinot.segment.local.aggregator.DistinctCountHLLPlusValueAggregator;
import org.apache.pinot.segment.local.aggregator.DistinctCountHLLValueAggregator;
import org.apache.pinot.segment.local.aggregator.DistinctCountThetaSketchValueAggregator;
import org.apache.pinot.segment.local.aggregator.DistinctCountULLValueAggregator;
import org.apache.pinot.segment.local.aggregator.IntegerTupleSketchValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregatorFactory;
import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.AggregationSpec;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;


public class StarTreeBuilderUtilsTest {
  @Test
  public void testAreStarTreeBuilderConfigListsEqual() {
    // Create StartTreeIndexConfigs to test for unequal starTree configs.
    StarTreeIndexConfig starTreeIndexConfig1 = new StarTreeIndexConfig(List.of("Carrier", "Distance"), null,
        List.of(AggregationFunctionColumnPair.COUNT_STAR_NAME), null, 100);

    // Different skip star node creation.
    StarTreeIndexConfig starTreeIndexConfig2 =
        new StarTreeIndexConfig(List.of("Carrier", "Distance"), List.of("Distance"),
            List.of(AggregationFunctionColumnPair.COUNT_STAR_NAME), null, 100);

    // Different dimension split order.
    StarTreeIndexConfig starTreeIndexConfig3 = new StarTreeIndexConfig(List.of("Distance", "Carrier"), null,
        List.of(AggregationFunctionColumnPair.COUNT_STAR_NAME), null, 100);

    // Different max leaf records.
    StarTreeIndexConfig starTreeIndexConfig4 = new StarTreeIndexConfig(List.of("Carrier", "Distance"), null,
        List.of(AggregationFunctionColumnPair.COUNT_STAR_NAME), null, 200);

    // Create StartTreeAggregationConfigs with StarTreeAggregationConfig.
    StarTreeAggregationConfig starTreeAggregationConfig1 = new StarTreeAggregationConfig("Distance", "MAX");

    // Different AggregationConfig.
    StarTreeIndexConfig starTreeIndexConfig5 =
        new StarTreeIndexConfig(List.of("Carrier", "Distance"), null, null, List.of(starTreeAggregationConfig1), 100);

    // Create StarTreeIndexConfig for equality check.
    StarTreeIndexConfig starTreeIndexConfig6 = new StarTreeIndexConfig(List.of("Carrier", "Distance"), null,
        List.of(AggregationFunctionColumnPair.COUNT_STAR_NAME), null, 100);

    // test unequal builder config size.
    List<StarTreeV2BuilderConfig> config1 = new ArrayList<>();
    List<StarTreeV2BuilderConfig> config2 = new ArrayList<>();

    // Add two StartTreeV2BuilderConfigs to config1 and one to config2.
    config1.add(StarTreeV2BuilderConfig.fromIndexConfig(starTreeIndexConfig1));
    config1.add(StarTreeV2BuilderConfig.fromIndexConfig(starTreeIndexConfig2));

    config2.add(StarTreeV2BuilderConfig.fromIndexConfig(starTreeIndexConfig3));
    assertFalse(StarTreeBuilderUtils.areStarTreeBuilderConfigListsEqual(config1, config2));

    // Test different dimension split order in StartTreeV2BuilderConfig.
    config1.clear();
    config1.add(StarTreeV2BuilderConfig.fromIndexConfig(starTreeIndexConfig1));

    assertFalse(StarTreeBuilderUtils.areStarTreeBuilderConfigListsEqual(config1, config2));

    // Test different skip star node creation in StartTreeV2BuilderConfig.
    config2.clear();
    config2.add(StarTreeV2BuilderConfig.fromIndexConfig(starTreeIndexConfig2));

    assertFalse(StarTreeBuilderUtils.areStarTreeBuilderConfigListsEqual(config1, config2));

    // Test different max leaf records in StartTreeV2BuilderConfig.
    config2.clear();
    config2.add(StarTreeV2BuilderConfig.fromIndexConfig(starTreeIndexConfig4));

    assertFalse(StarTreeBuilderUtils.areStarTreeBuilderConfigListsEqual(config1, config2));

    // Test different aggregation configs in StartTreeV2BuilderConfig.
    config2.clear();
    config2.add(StarTreeV2BuilderConfig.fromIndexConfig(starTreeIndexConfig5));

    assertFalse(StarTreeBuilderUtils.areStarTreeBuilderConfigListsEqual(config1, config2));

    // Test equal StartTreeV2BuilderConfigs.
    config2.clear();
    config2.add(StarTreeV2BuilderConfig.fromIndexConfig(starTreeIndexConfig6));

    assertTrue(StarTreeBuilderUtils.areStarTreeBuilderConfigListsEqual(config1, config2));
  }

  @Test
  public void testGenerateBuilderConfig() {

    // Create Schema and SegmentMetadata for testing.
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.INT)
        .addSingleValueDimension("d2", FieldSpec.DataType.LONG)
        .addSingleValueDimension("d3", FieldSpec.DataType.FLOAT)
        .addSingleValueDimension("d4", FieldSpec.DataType.DOUBLE)
        .addMultiValueDimension("d5", FieldSpec.DataType.INT)
        .addMetric("m1", FieldSpec.DataType.DOUBLE)
        .addMetric("m2", FieldSpec.DataType.BYTES)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "t"), null)
        .addDateTime("dt", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .build();
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getSchema()).thenReturn(schema);
    // Included
    ColumnMetadata columnMetadata = getColumnMetadata("d1", true, 200);
    when(segmentMetadata.getColumnMetadataFor("d1")).thenReturn(columnMetadata);
    // Included
    columnMetadata = getColumnMetadata("d2", true, 400);
    when(segmentMetadata.getColumnMetadataFor("d2")).thenReturn(columnMetadata);
    // Not included because the cardinality is too high
    columnMetadata = getColumnMetadata("d3", true, 20000);
    when(segmentMetadata.getColumnMetadataFor("d3")).thenReturn(columnMetadata);
    // Not included because it is not dictionary-encoded
    columnMetadata = getColumnMetadata("d4", false, 100);
    when(segmentMetadata.getColumnMetadataFor("d4")).thenReturn(columnMetadata);
    // Not included because it is multi-valued
    columnMetadata = getColumnMetadata("d5", true, 100);
    when(segmentMetadata.getColumnMetadataFor("d5")).thenReturn(columnMetadata);
    // Included (metric does not have to be dictionary-encoded or have valid cardinality)
    columnMetadata = getColumnMetadata("m1", false, Constants.UNKNOWN_CARDINALITY);
    when(segmentMetadata.getColumnMetadataFor("m1")).thenReturn(columnMetadata);
    // Not included because it is not numeric
    columnMetadata = getColumnMetadata("m2", true, 100);
    when(segmentMetadata.getColumnMetadataFor("m2")).thenReturn(columnMetadata);
    // Included (do not check cardinality for time column)
    columnMetadata = getColumnMetadata("t", true, 20000);
    when(segmentMetadata.getColumnMetadataFor("t")).thenReturn(columnMetadata);
    // Included (do not check cardinality for date time column)
    columnMetadata = getColumnMetadata("dt", true, 30000);
    when(segmentMetadata.getColumnMetadataFor("dt")).thenReturn(columnMetadata);

    // // Create a list of string with column name, hasDictionary and cardinality.
    List<List<String>> columnList =
        List.of(List.of("d1", "true", "200"), List.of("d2", "true", "400"), List.of("d3", "true", "20000"),
            List.of("d4", "false", "100"), List.of("d5", "true", "100"), List.of("m1", "false", "-1"),
            List.of("m2", "true", "100"), List.of("t", "true", "20000"), List.of("dt", "true", "30000"));

    // Convert the list of string to JsonNode with appropriate key names and root node.
    JsonNode segmentMetadataAsJsonNode = convertStringListToJsonNode(columnList);

    // Create StartTreeIndexConfig for testing.
    StarTreeIndexConfig starTreeIndexConfig1 = new StarTreeIndexConfig(List.of("Carrier", "Distance"), null,
        List.of(AggregationFunctionColumnPair.COUNT_STAR_NAME), null, 100);

    StarTreeIndexConfig starTreeIndexConfig2 =
        new StarTreeIndexConfig(List.of("Carrier", "Distance"), List.of("Distance"),
            List.of(AggregationFunctionColumnPair.COUNT_STAR_NAME), null, 100);

    // Create StartTreeV2BuilderConfig from segmentMetadataImpl.
    List<StarTreeV2BuilderConfig> builderConfig1 =
        StarTreeBuilderUtils.generateBuilderConfigs(List.of(starTreeIndexConfig1, starTreeIndexConfig2), true,
            segmentMetadata);

    // Create StartTreeV2BuilderConfig from JsonNode.
    List<StarTreeV2BuilderConfig> builderConfig2 =
        StarTreeBuilderUtils.generateBuilderConfigs(List.of(starTreeIndexConfig1, starTreeIndexConfig2), true, schema,
            segmentMetadataAsJsonNode);

    // They should be equal.
    assertEquals(builderConfig1, builderConfig2);
  }

  @Test
  public void testShouldModifyExistingStarTreesDifferentParameters() {
    Configuration metadataProperties = new PropertiesConfiguration();
    TreeMap<AggregationFunctionColumnPair, AggregationSpec> aggregationSpecs = new TreeMap<>();
    aggregationSpecs.put(new AggregationFunctionColumnPair(AggregationFunctionType.DISTINCTCOUNTHLL, "col2"),
        new AggregationSpec(null, null, null, null, null, Map.of(Constants.HLL_LOG2M_KEY, 16)));
    StarTreeV2Metadata.writeMetadata(metadataProperties, 1, List.of("col1"), aggregationSpecs, 100, Set.of());
    StarTreeV2Metadata existingStarTreeMetadata = new StarTreeV2Metadata(metadataProperties);

    StarTreeIndexConfig starTreeIndexConfig = new StarTreeIndexConfig(List.of("col1"), null, null, List.of(
        new StarTreeAggregationConfig("col2", "DISTINCTCOUNTHLL", Map.of(Constants.HLL_LOG2M_KEY, 16), null, null, null,
            null, null)), 100);
    assertFalse(StarTreeBuilderUtils.shouldModifyExistingStarTrees(
        List.of(StarTreeV2BuilderConfig.fromIndexConfig(starTreeIndexConfig)), List.of(existingStarTreeMetadata)));

    // Change log2m value
    starTreeIndexConfig = new StarTreeIndexConfig(List.of("col1"), null, null, List.of(
        new StarTreeAggregationConfig("col2", "DISTINCTCOUNTHLL", Map.of(Constants.HLL_LOG2M_KEY, 8), null, null, null,
            null, null)), 100);
    assertTrue(StarTreeBuilderUtils.shouldModifyExistingStarTrees(
        List.of(StarTreeV2BuilderConfig.fromIndexConfig(starTreeIndexConfig)), List.of(existingStarTreeMetadata)));
  }

  @Test
  public void testExpressionContextFromFunctionParameters() {
    // DISTINCTCOUNTHLL
    DistinctCountHLLValueAggregator hllValueAggregator =
        (DistinctCountHLLValueAggregator) ValueAggregatorFactory.getValueAggregator(
            AggregationFunctionType.DISTINCTCOUNTHLL,
            StarTreeBuilderUtils.expressionContextFromFunctionParameters(AggregationFunctionType.DISTINCTCOUNTHLL,
                Map.of(Constants.HLL_LOG2M_KEY, "10")));
    assertEquals(10, hllValueAggregator.getLog2m());

    hllValueAggregator = (DistinctCountHLLValueAggregator) ValueAggregatorFactory.getValueAggregator(
        AggregationFunctionType.DISTINCTCOUNTHLL,
        StarTreeBuilderUtils.expressionContextFromFunctionParameters(AggregationFunctionType.DISTINCTCOUNTHLL,
            Map.of()));
    // Verify default value used
    assertEquals(8, hllValueAggregator.getLog2m());

    // DISTINCTCOUNTHLLPLUS
    DistinctCountHLLPlusValueAggregator hllPlusValueAggregator =
        (DistinctCountHLLPlusValueAggregator) ValueAggregatorFactory.getValueAggregator(
            AggregationFunctionType.DISTINCTCOUNTHLLPLUS,
            StarTreeBuilderUtils.expressionContextFromFunctionParameters(AggregationFunctionType.DISTINCTCOUNTHLLPLUS,
                Map.of(Constants.HLLPLUS_ULL_P_KEY, "10", Constants.HLLPLUS_SP_KEY, 20)));
    assertEquals(10, hllPlusValueAggregator.getP());
    assertEquals(20, hllPlusValueAggregator.getSp());

    hllPlusValueAggregator = (DistinctCountHLLPlusValueAggregator) ValueAggregatorFactory.getValueAggregator(
        AggregationFunctionType.DISTINCTCOUNTHLLPLUS,
        StarTreeBuilderUtils.expressionContextFromFunctionParameters(AggregationFunctionType.DISTINCTCOUNTHLLPLUS,
            Map.of("garbage", "value")));
    // Verify default values used
    assertEquals(14, hllPlusValueAggregator.getP());
    assertEquals(0, hllPlusValueAggregator.getSp());

    hllPlusValueAggregator = (DistinctCountHLLPlusValueAggregator) ValueAggregatorFactory.getValueAggregator(
        AggregationFunctionType.DISTINCTCOUNTHLLPLUS,
        StarTreeBuilderUtils.expressionContextFromFunctionParameters(AggregationFunctionType.DISTINCTCOUNTHLLPLUS,
            Map.of(Constants.HLLPLUS_ULL_P_KEY, "10")));
    assertEquals(10, hllPlusValueAggregator.getP());
    assertEquals(0, hllPlusValueAggregator.getSp());

    hllPlusValueAggregator = (DistinctCountHLLPlusValueAggregator) ValueAggregatorFactory.getValueAggregator(
        AggregationFunctionType.DISTINCTCOUNTHLLPLUS,
        StarTreeBuilderUtils.expressionContextFromFunctionParameters(AggregationFunctionType.DISTINCTCOUNTHLLPLUS,
            Map.of(Constants.HLLPLUS_SP_KEY, 20)));
    assertEquals(14, hllPlusValueAggregator.getP());
    assertEquals(20, hllPlusValueAggregator.getSp());

    // DISTINCTCOUNTULL
    DistinctCountULLValueAggregator ullValueAggregator =
        (DistinctCountULLValueAggregator) ValueAggregatorFactory.getValueAggregator(
            AggregationFunctionType.DISTINCTCOUNTULL,
            StarTreeBuilderUtils.expressionContextFromFunctionParameters(AggregationFunctionType.DISTINCTCOUNTULL,
                Map.of(Constants.HLLPLUS_ULL_P_KEY, "10")));
    assertEquals(10, ullValueAggregator.getP());

    ullValueAggregator = (DistinctCountULLValueAggregator) ValueAggregatorFactory.getValueAggregator(
        AggregationFunctionType.DISTINCTCOUNTULL,
        StarTreeBuilderUtils.expressionContextFromFunctionParameters(AggregationFunctionType.DISTINCTCOUNTULL,
            Map.of("garbage", "value")));
    // Verify default value used
    assertEquals(12, ullValueAggregator.getP());

    // DISTINCTCOUNTCPCSKETCH
    DistinctCountCPCSketchValueAggregator cpcSketchValueAggregator =
        (DistinctCountCPCSketchValueAggregator) ValueAggregatorFactory.getValueAggregator(
            AggregationFunctionType.DISTINCTCOUNTCPCSKETCH,
            StarTreeBuilderUtils.expressionContextFromFunctionParameters(AggregationFunctionType.DISTINCTCOUNTCPCSKETCH,
                Map.of(Constants.CPCSKETCH_LGK_KEY, 20)));
    assertEquals(20, cpcSketchValueAggregator.getLgK());

    cpcSketchValueAggregator = (DistinctCountCPCSketchValueAggregator) ValueAggregatorFactory.getValueAggregator(
        AggregationFunctionType.DISTINCTCOUNTCPCSKETCH,
        StarTreeBuilderUtils.expressionContextFromFunctionParameters(AggregationFunctionType.DISTINCTCOUNTCPCSKETCH,
            Map.of()));
    // Verify default value used
    assertEquals(12, cpcSketchValueAggregator.getLgK());

    // DISTINCTCOUNTTHETASKETCH
    DistinctCountThetaSketchValueAggregator thetaSketchValueAggregator =
        (DistinctCountThetaSketchValueAggregator) ValueAggregatorFactory.getValueAggregator(
            AggregationFunctionType.DISTINCTCOUNTTHETASKETCH,
            StarTreeBuilderUtils.expressionContextFromFunctionParameters(
                AggregationFunctionType.DISTINCTCOUNTTHETASKETCH,
                Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 4096)));
    assertEquals(4096, thetaSketchValueAggregator.getNominalEntries());

    thetaSketchValueAggregator = (DistinctCountThetaSketchValueAggregator) ValueAggregatorFactory.getValueAggregator(
        AggregationFunctionType.DISTINCTCOUNTTHETASKETCH,
        StarTreeBuilderUtils.expressionContextFromFunctionParameters(AggregationFunctionType.DISTINCTCOUNTTHETASKETCH,
            Map.of()));
    // Verify default value used
    assertEquals(16384, thetaSketchValueAggregator.getNominalEntries());

    // DISTINCTCOUNTUPLESKETCH
    IntegerTupleSketchValueAggregator tupleSketchValueAggregator =
        (IntegerTupleSketchValueAggregator) ValueAggregatorFactory.getValueAggregator(
            AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH,
            StarTreeBuilderUtils.expressionContextFromFunctionParameters(
                AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH,
                Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 4096)));
    assertEquals(4096, tupleSketchValueAggregator.getNominalEntries());

    tupleSketchValueAggregator = (IntegerTupleSketchValueAggregator) ValueAggregatorFactory.getValueAggregator(
        AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH,
        StarTreeBuilderUtils.expressionContextFromFunctionParameters(AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH,
            Map.of()));
    // Verify default value used
    assertEquals(16384, tupleSketchValueAggregator.getNominalEntries());
  }

  private ColumnMetadata getColumnMetadata(String column, boolean hasDictionary, int cardinality) {
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.getColumnName()).thenReturn(column);
    when(columnMetadata.hasDictionary()).thenReturn(hasDictionary);
    when(columnMetadata.getCardinality()).thenReturn(cardinality);
    return columnMetadata;
  }

  private JsonNode convertStringListToJsonNode(List<List<String>> columnList) {
    // Create arrayNode from the list of string
    ArrayNode arrayNode = new ArrayNode(JsonNodeFactory.instance);
    for (List<String> column : columnList) {
      ObjectNode objectNode = new ObjectNode(JsonNodeFactory.instance);
      objectNode.put("columnName", column.get(0));
      objectNode.put("hasDictionary", column.get(1));
      objectNode.put("cardinality", column.get(2));
      arrayNode.add(objectNode);
    }
    ObjectNode rootNode = new ObjectNode(JsonNodeFactory.instance);

    // set the rootNode key as "columns" and value as arrayNode.
    rootNode.set("columns", arrayNode);
    return rootNode;
  }
}
