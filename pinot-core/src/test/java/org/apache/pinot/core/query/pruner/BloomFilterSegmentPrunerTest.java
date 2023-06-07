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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.segment.index.readers.bloom.OnHeapGuavaBloomFilterReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class BloomFilterSegmentPrunerTest {
  private static final BloomFilterSegmentPruner PRUNER = new BloomFilterSegmentPruner();

  @BeforeClass
  public void setUp() {
    Map<String, Object> properties = new HashMap<>();
    // override default value
    properties.put(ColumnValueSegmentPruner.IN_PREDICATE_THRESHOLD, 5);
    PinotConfiguration configuration = new PinotConfiguration(properties);
    PRUNER.init(configuration);
  }

  @Test
  public void testBloomFilterPruning()
      throws IOException {
    IndexSegment indexSegment = mockIndexSegment(new String[]{"1.0", "2.0", "3.0", "5.0", "7.0", "21.0"});

    // all out the bloom filter
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0.0)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0.0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 6.0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (6.0)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0.0 OR column = 6.0"));

    // all in the bloom filter
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 5.0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (5.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 5.0 OR column = 7.0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (5.0, 7.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 5.0 AND column = 7.0"));

    // some in the bloom filter with IN/OR
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0.0, 3.0, 4.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 1.0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (1.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 21.0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (21.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (21.0, 30.0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 21.0 OR column = 30.0"));
    // 30 out the bloom filter with AND
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 21.0 AND column = 30.0"));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testQueryTimeoutOnPruning()
      throws IOException {
    IndexSegment indexSegment = mockIndexSegment(new String[]{"1.0", "2.0", "3.0", "5.0", "7.0", "21.0"});
    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource("column")).thenReturn(dataSource);
    runPruner(Collections.singletonList(indexSegment),
        "SELECT COUNT(*) FROM testTable WHERE column = 5.0 OR column = 0.0", 1);
  }

  @Test
  public void testParallelPrune()
      throws IOException {
    List<IndexSegment> segments = new ArrayList<>();
    for (int i = 0; i < 35; i++) {
      IndexSegment indexSegment = mockIndexSegment(new String[]{"1.0", "2.0", "3.0", "5.0", "7.0", "21.0"});
      segments.add(indexSegment);
    }
    assertTrue(
        runPruner(segments, "SELECT COUNT(*) FROM testTable WHERE column = 21.0 AND column = 30.0", 5000).isEmpty());

    IndexSegment indexSegment = mockIndexSegment(new String[]{"1.0", "2.0", "3.0", "5.0", "7.0", "21.0", "30.0"});
    segments.add(indexSegment);
    List<IndexSegment> selected =
        runPruner(segments, "SELECT COUNT(*) FROM testTable WHERE column = 21.0 AND column = 30.0", 5000);
    assertEquals(selected.size(), 1);
  }

  @Test
  public void testIsApplicableTo() {
    // EQ and IN (with small number of values) are applicable for bloom filter based pruning.
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column = 1");
    assertTrue(PRUNER.isApplicableTo(queryContext));
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column IN (1, 2)");
    assertTrue(PRUNER.isApplicableTo(queryContext));

    // NOT is not applicable
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE NOT column = 1");
    assertFalse(PRUNER.isApplicableTo(queryContext));
    // Too many values for IN clause
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column IN (1, 2, 3, 4, 5, 6, 7)");
    assertFalse(PRUNER.isApplicableTo(queryContext));
    // Other predicate types are not applicable
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column LIKE 5");
    assertFalse(PRUNER.isApplicableTo(queryContext));

    // AND with one applicable child filter is applicable
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column NOT IN (1, 2) AND column = 3");
    assertTrue(PRUNER.isApplicableTo(queryContext));

    // OR with one child filter that's not applicable is not applicable
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column = 3 OR column NOT IN (1, 2)");
    assertFalse(PRUNER.isApplicableTo(queryContext));

    // Nested with AND/OR
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column = 3 OR (column NOT IN (1, 2) AND column = 4)");
    assertTrue(PRUNER.isApplicableTo(queryContext));
  }

  private IndexSegment mockIndexSegment(String[] values)
      throws IOException {
    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getColumnNames()).thenReturn(ImmutableSet.of("column"));
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(20);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);

    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource("column")).thenReturn(dataSource);
    // Add support for bloom filter
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    BloomFilterReaderBuilder builder = new BloomFilterReaderBuilder();
    for (String v : values) {
      builder.put(v);
    }
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.DOUBLE);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    when(dataSource.getBloomFilter()).thenReturn(builder.build());

    return indexSegment;
  }

  private boolean runPruner(IndexSegment segment, String query) {
    return runPruner(Collections.singletonList(segment), query, 5000).isEmpty();
  }

  private List<IndexSegment> runPruner(List<IndexSegment> segments, String query, long queryTimeout) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setEndTimeMs(System.currentTimeMillis() + queryTimeout);
    return PRUNER.prune(segments, queryContext, Executors.newCachedThreadPool());
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
