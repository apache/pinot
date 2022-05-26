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
package org.apache.pinot.core.query.pruner;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.segment.index.readers.bloom.OnHeapGuavaBloomFilterReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ColumnValueSegmentPrunerTest {
  private static final ColumnValueSegmentPruner PRUNER = new ColumnValueSegmentPruner();

  @Test
  public void testMinMaxValuePruning() {
    Map<String, Object> properties = new HashMap<>();
    //override default value
    properties.put(PRUNER.IN_PREDICATE_THRESHOLD, 5);
    PinotConfiguration configuration = new PinotConfiguration(properties);
    PRUNER.init(configuration);

    IndexSegment indexSegment = mockIndexSegment();

    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource("column")).thenReturn(dataSource);

    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSourceMetadata.getMinValue()).thenReturn(10);
    when(dataSourceMetadata.getMaxValue()).thenReturn(20);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);

    // Equality predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 20"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 30"));
    // Range predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column < 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column <= 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 20"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 20 AND 30"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 30 AND 40"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 10 AND 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 20 AND 20"));
    // Invalid range predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 20 AND 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 30 AND 20"));
    // In Predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0, 5, 8)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (21, 30)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (10)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (5, 10, 15)"));
    //although the segment can be pruned, it will not be pruned as the size of values is greater than threshold
    assertFalse(
        runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)"));
    assertFalse(
        runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)"));
    // AND operator
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 AND column > 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 0 AND column < 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 0 AND column <= 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 20 AND column < 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 20 AND column < 30"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 0 AND column BETWEEN 0 AND 10"));
    // OR operator
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column > 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column < 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 0 OR column <= 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 30 OR column < 10"));
    assertTrue(runPruner(indexSegment,
        "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 0 AND 5 OR column BETWEEN 30 AND 35"));
  }

  @Test
  public void testPartitionPruning() {
    IndexSegment indexSegment = mockIndexSegment();

    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource("column")).thenReturn(dataSource);

    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSourceMetadata.getPartitionFunction()).thenReturn(
        PartitionFunctionFactory.getPartitionFunction("Modulo", 5, null));
    when(dataSourceMetadata.getPartitions()).thenReturn(Collections.singleton(2));
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);

    // Equality predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 2"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 7"));
    // AND operator
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 AND column = 2"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 0 AND column = 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 2 AND column > 0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column <= 10 AND column = 7"));
    // OR operator
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column = 2"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column < 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column = 10"));
  }

  @Test
  public void testBloomFilterPredicatePruning()
      throws IOException {
    Map<String, Object> properties = new HashMap<>();
    // override default value
    properties.put(ColumnValueSegmentPruner.IN_PREDICATE_THRESHOLD, 5);
    PinotConfiguration configuration = new PinotConfiguration(properties);
    PRUNER.init(configuration);

    IndexSegment indexSegment = mockIndexSegment();
    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource("column")).thenReturn(dataSource);
    // Add support for bloom filter
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    BloomFilterReader bloomFilterReader = new BloomFilterReaderBuilder()
        .put("1.0")
        .put("2.0")
        .put("3.0")
        .put("5.0")
        .put("7.0")
        .put("21.0")
        .build();

    when(dataSourceMetadata.getDataType()).thenReturn(DataType.DOUBLE);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    when(dataSource.getBloomFilter()).thenReturn(bloomFilterReader);
    when(dataSourceMetadata.getMinValue()).thenReturn(5.0);
    when(dataSourceMetadata.getMaxValue()).thenReturn(10.0);

    // all out the bloom filter and out of range
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0.0)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0.0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0.0, 3.0, 4.0)"));

    // some in the bloom filter but all out of range
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 1.0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (1.0)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 21.0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (21.0)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (21.0, 30.0)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 21.0 AND column = 30.0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 21.0 OR column = 30.0"));

    // all out the bloom filter but some in range
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 6.0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (6.0)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0.0 OR column = 6.0"));

    // all in the bloom filter and in range
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 5.0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (5.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 5.0 OR column = 7.0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (5.0, 7.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 5.0 AND column = 7.0"));

    // some in the bloom filter and in range
    assertFalse(
        runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0.0, 5.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 5.0 OR column = 0.0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 5.0 AND column = 0.0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (8.0, 10.0)"));
  }

  private IndexSegment mockIndexSegment() {
    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getColumnNames()).thenReturn(ImmutableSet.of("column"));
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(20);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    return indexSegment;
  }

  private boolean runPruner(IndexSegment indexSegment, String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    return PRUNER.prune(Arrays.asList(indexSegment), queryContext).isEmpty();
  }

  private static class BloomFilterReaderBuilder {
    private BloomFilter<String> _bloomfilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 100, 0.01);
    public BloomFilterReaderBuilder put(String value) {
      _bloomfilter.put(value);
      return this;
    }

    public BloomFilterReader build() throws IOException {
      File file = Files.createTempFile("test", ".bloom").toFile();
      try (FileOutputStream fos = new FileOutputStream(file)) {
        _bloomfilter.writeTo(fos);
        try (PinotDataBuffer pinotDataBuffer = PinotDataBuffer.loadBigEndianFile(file)) {
          // on heap filter should never use the buffer, so we can close it and delete the file
          return new OnHeapGuavaBloomFilterReader(pinotDataBuffer);
        }
      } finally {
        file.delete();
      }
    }
  }
}
