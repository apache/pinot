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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
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

    IndexSegment indexSegment = mock(IndexSegment.class);

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
    IndexSegment indexSegment = mock(IndexSegment.class);

    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource("column")).thenReturn(dataSource);

    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSourceMetadata.getPartitionFunction()).thenReturn(
        PartitionFunctionFactory.getPartitionFunction("Modulo", 5));
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
  public void testBloomFilterInPredicatePruning() {
    Map<String, Object> properties = new HashMap<>();
    // override default value
    properties.put(ColumnValueSegmentPruner.IN_PREDICATE_THRESHOLD, 5);
    PinotConfiguration configuration = new PinotConfiguration(properties);
    PRUNER.init(configuration);

    IndexSegment indexSegment = mock(IndexSegment.class);

    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource("column")).thenReturn(dataSource);
    // Add support for bloom filter
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    BloomFilterReader bloomFilterReader = mock(BloomFilterReader.class);

    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    when(dataSource.getBloomFilter()).thenReturn(bloomFilterReader);
    when(bloomFilterReader.mightContain("1")).thenReturn(true);
    when(bloomFilterReader.mightContain("2")).thenReturn(true);
    when(bloomFilterReader.mightContain("3")).thenReturn(true);
    when(dataSourceMetadata.getMinValue()).thenReturn(5);
    when(dataSourceMetadata.getMaxValue()).thenReturn(10);

    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0, 1, 2)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (21, 30)"));
    assertFalse(
        runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)"));
  }

  private boolean runPruner(IndexSegment indexSegment, String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
    return PRUNER.prune(Collections.singletonList(indexSegment), queryContext).isEmpty();
  }
}
