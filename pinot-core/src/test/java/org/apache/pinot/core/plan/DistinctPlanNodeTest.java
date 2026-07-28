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
package org.apache.pinot.core.plan;

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DistinctPlanNodeTest {
  @Test
  public void rawVariantIsRejectedBeforeDictionaryOptimizations() {
    SegmentContext segmentContext = Mockito.mock(SegmentContext.class);
    IndexSegment indexSegment = Mockito.mock(IndexSegment.class);
    QueryContext queryContext = Mockito.mock(QueryContext.class);
    DataSource dataSource = Mockito.mock(DataSource.class);
    DataSourceMetadata metadata = Mockito.mock(DataSourceMetadata.class);
    Schema schema = new Schema();

    Mockito.when(segmentContext.getIndexSegment()).thenReturn(indexSegment);
    Mockito.when(queryContext.getSelectExpressions())
        .thenReturn(List.of(ExpressionContext.forIdentifier("payload")));
    Mockito.when(queryContext.getSchema()).thenReturn(schema);
    Mockito.when(indexSegment.getDataSource("payload", schema)).thenReturn(dataSource);
    Mockito.when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    Mockito.when(metadata.getDataType()).thenReturn(DataType.VARIANT);

    IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class,
        () -> new DistinctPlanNode(segmentContext, queryContext).run());
    Assert.assertTrue(exception.getMessage().contains("Raw VARIANT values do not support DISTINCT"));
    Assert.assertTrue(exception.getMessage().contains("extract a typed path with variantGet first"));
  }
}
