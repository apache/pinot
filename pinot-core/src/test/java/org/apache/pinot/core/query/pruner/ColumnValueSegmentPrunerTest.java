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

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ColumnValueSegmentPrunerTest {
  private static final ColumnValueSegmentPruner PRUNER = new ColumnValueSegmentPruner();

  @BeforeClass
  public void setUp() {
    Map<String, Object> properties = new HashMap<>();
    // override default value
    properties.put(ColumnValueSegmentPruner.IN_PREDICATE_THRESHOLD, 5);
    PinotConfiguration configuration = new PinotConfiguration(properties);
    PRUNER.init(configuration);
  }

  @Test
  public void testMinMaxValuePruning() {
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
  public void testIsApplicableTo() {
    // EQ, RANGE and IN (with small number of values) are applicable for min/max/partitionId based pruning.
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column = 1");
    assertTrue(PRUNER.isApplicableTo(queryContext));
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column IN (1, 2)");
    assertTrue(PRUNER.isApplicableTo(queryContext));
    queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column BETWEEN 1 AND 2");
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
        "SELECT COUNT(*) FROM testTable WHERE column = 3 OR (column NOT IN (1, 2) AND column BETWEEN 4 AND 5)");
    assertTrue(PRUNER.isApplicableTo(queryContext));
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
}
