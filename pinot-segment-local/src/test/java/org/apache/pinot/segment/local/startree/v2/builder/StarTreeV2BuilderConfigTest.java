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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.AggregationSpec;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class StarTreeV2BuilderConfigTest {

  @Test
  public void testDefaultConfig() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("d1", DataType.INT)
        .addSingleValueDimension("d2", DataType.LONG).addSingleValueDimension("d3", DataType.FLOAT)
        .addSingleValueDimension("d4", DataType.DOUBLE).addMultiValueDimension("d5", DataType.INT)
        .addMetric("m1", DataType.DOUBLE).addMetric("m2", DataType.BYTES)
        .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "t"), null)
        .addDateTime("dt", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS").build();
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

    StarTreeV2BuilderConfig defaultConfig = StarTreeV2BuilderConfig.generateDefaultConfig(segmentMetadata);
    // Sorted by cardinality in descending order, followed by the time column
    assertEquals(defaultConfig.getDimensionsSplitOrder(), Arrays.asList("d2", "d1", "dt", "t"));
    // No column should be skipped for star-node creation
    assertTrue(defaultConfig.getSkipStarNodeCreationForDimensions().isEmpty());
    // Should have COUNT(*) and SUM(m1) as the function column pairs
    assertEquals(defaultConfig.getFunctionColumnPairs(), new HashSet<>(
        Arrays.asList(AggregationFunctionColumnPair.COUNT_STAR,
            new AggregationFunctionColumnPair(AggregationFunctionType.SUM, "m1"))));
    assertEquals(defaultConfig.getMaxLeafRecords(), StarTreeV2BuilderConfig.DEFAULT_MAX_LEAF_RECORDS);
  }

  @Test
  public void testBuildFromIndexConfig() {
    List<StarTreeAggregationConfig> aggregationConfigs =
        List.of(new StarTreeAggregationConfig("m1", "SUM", CompressionCodec.LZ4));
    StarTreeIndexConfig starTreeIndexConfig = new StarTreeIndexConfig(List.of("d1"), null, null, aggregationConfigs, 1);
    StarTreeV2BuilderConfig builderConfig = StarTreeV2BuilderConfig.fromIndexConfig(starTreeIndexConfig);
    assertEquals(builderConfig.getMaxLeafRecords(), 1);
    assertEquals(builderConfig.getDimensionsSplitOrder(), List.of("d1"));
    assertEquals(builderConfig.getFunctionColumnPairs(),
        Set.of(new AggregationFunctionColumnPair(AggregationFunctionType.SUM, "m1")));
    assertTrue(builderConfig.getSkipStarNodeCreationForDimensions().isEmpty());
    assertEquals(builderConfig.getAggregationSpecs().values(),
        Collections.singleton(new AggregationSpec(ChunkCompressionType.LZ4)));
  }

  @Test
  public void testAggregationSpecUniqueness() {
    List<StarTreeAggregationConfig> aggregationConfigs =
        List.of(new StarTreeAggregationConfig("m1", "distinctCountThetaSketch", CompressionCodec.LZ4),
            new StarTreeAggregationConfig("m1", "distinctCountRawThetaSketch", CompressionCodec.LZ4));
    StarTreeIndexConfig starTreeIndexConfig = new StarTreeIndexConfig(List.of("d1"), null, null, aggregationConfigs, 1);
    StarTreeV2BuilderConfig builderConfig = StarTreeV2BuilderConfig.fromIndexConfig(starTreeIndexConfig);
    assertEquals(builderConfig.getMaxLeafRecords(), 1);
    assertEquals(builderConfig.getDimensionsSplitOrder(), List.of("d1"));
    assertEquals(builderConfig.getFunctionColumnPairs(),
        Set.of(new AggregationFunctionColumnPair(AggregationFunctionType.DISTINCTCOUNTTHETASKETCH, "m1")));
    assertTrue(builderConfig.getSkipStarNodeCreationForDimensions().isEmpty());
    assertEquals(builderConfig.getAggregationSpecs().values(),
        Collections.singleton(new AggregationSpec(ChunkCompressionType.LZ4)));
  }

  @Test
  public void testFunctionColumnPairUniqueness() {
    List<String> functionColumnPairs = List.of("distinctCountThetaSketch__m1", "distinctCountRawThetaSketch__m1");
    StarTreeIndexConfig starTreeIndexConfig =
        new StarTreeIndexConfig(List.of("d1"), null, functionColumnPairs, null, 1);
    StarTreeV2BuilderConfig builderConfig = StarTreeV2BuilderConfig.fromIndexConfig(starTreeIndexConfig);
    assertEquals(builderConfig.getMaxLeafRecords(), 1);
    assertEquals(builderConfig.getDimensionsSplitOrder(), List.of("d1"));
    assertEquals(builderConfig.getFunctionColumnPairs(),
        Set.of(new AggregationFunctionColumnPair(AggregationFunctionType.DISTINCTCOUNTTHETASKETCH, "m1")));
    assertTrue(builderConfig.getSkipStarNodeCreationForDimensions().isEmpty());
    assertEquals(builderConfig.getAggregationSpecs().values(), Collections.singleton(AggregationSpec.DEFAULT));
  }

  private ColumnMetadata getColumnMetadata(String column, boolean hasDictionary, int cardinality) {
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.getColumnName()).thenReturn(column);
    when(columnMetadata.hasDictionary()).thenReturn(hasDictionary);
    when(columnMetadata.getCardinality()).thenReturn(cardinality);
    return columnMetadata;
  }
}
