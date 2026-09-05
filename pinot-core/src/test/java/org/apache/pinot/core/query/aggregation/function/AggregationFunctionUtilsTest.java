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
package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;


/// Unit test for {@link AggregationFunctionUtils#getAggregationResult}, the metadata/dictionary based aggregation
/// result resolver used by the non-scan based and partial metadata based aggregation paths.
@SuppressWarnings("rawtypes")
public class AggregationFunctionUtilsTest {
  private static final ExpressionContext PAYLOAD = ExpressionContext.forIdentifier("payload");

  private static AggregationFunction mockFunction(AggregationFunctionType type) {
    AggregationFunction aggregationFunction = mock(AggregationFunction.class);
    when(aggregationFunction.getType()).thenReturn(type);
    return aggregationFunction;
  }

  @Test
  public void testCountResolvedFromNumTotalDocs() {
    AggregationFunction countFunction = mockFunction(AggregationFunctionType.COUNT);
    // COUNT is resolved directly from numTotalDocs and must not touch the (possibly null) data source.
    Object result = AggregationFunctionUtils.getAggregationResult(countFunction, null, 42, "TEST");
    assertEquals(result, 42L);
  }

  @Test
  public void testMinAndMaxResolvedFromDictionary() {
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getMinVal()).thenReturn(5);
    when(dictionary.getMaxVal()).thenReturn(10);
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getDictionary()).thenReturn(dictionary);

    Object minResult = AggregationFunctionUtils.getAggregationResult(mockFunction(AggregationFunctionType.MIN),
        dataSource, 100, "TEST");
    assertEquals(minResult, 5.0);

    Object maxResult = AggregationFunctionUtils.getAggregationResult(mockFunction(AggregationFunctionType.MAX),
        dataSource, 100, "TEST");
    assertEquals(maxResult, 10.0);
  }

  @Test
  public void testUnsupportedFunctionThrows() {
    // MODE cannot be resolved from dictionary/metadata; the resolver must reject it rather than return a wrong result.
    DataSource dataSource = mock(DataSource.class);
    assertThrows(IllegalStateException.class,
        () -> AggregationFunctionUtils.getAggregationResult(mockFunction(AggregationFunctionType.MODE), dataSource,
            100, "TEST"));
  }

  @Test
  public void testNonCountWithNullDataSourceThrows() {
    // Every non-COUNT function reads from the column dictionary/metadata and therefore requires a non-null data source.
    assertThrows(NullPointerException.class,
          () -> AggregationFunctionUtils.getAggregationResult(mockFunction(AggregationFunctionType.MIN), null, 100,
            "TEST"));
  }

  @Test
  public void testRejectsUnsafeRawVariantAggregationsUsingLogicalType() {
    Assert.assertEquals(FieldSpec.DataType.VARIANT.getStoredType(), FieldSpec.DataType.BYTES);
    BaseProjectOperator<?> projectOperator = projectOperator(FieldSpec.DataType.VARIANT);

    for (AggregationFunctionType functionType
        : List.of(AggregationFunctionType.SUM, AggregationFunctionType.ANYVALUE,
            AggregationFunctionType.DISTINCTCOUNTHLL)) {
      AggregationFunction aggregationFunction = aggregationFunction(functionType);
      IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class,
          () -> AggregationFunctionUtils.validateRawVariantAggregationInputs(
              new AggregationFunction[]{aggregationFunction}, projectOperator));
      Assert.assertTrue(exception.getMessage().contains(functionType.getName()));
      Assert.assertTrue(exception.getMessage().contains("variantGet"));
    }
  }

  @Test
  public void testAllowsCountOfRawVariant() {
    AggregationFunctionUtils.validateRawVariantAggregationInputs(
        new AggregationFunction[]{aggregationFunction(AggregationFunctionType.COUNT)},
        projectOperator(FieldSpec.DataType.VARIANT));
  }

  @Test
  public void testDoesNotRejectNonVariantInputs() {
    for (FieldSpec.DataType dataType
        : List.of(FieldSpec.DataType.STRING, FieldSpec.DataType.MAP, FieldSpec.DataType.LIST)) {
      AggregationFunctionUtils.validateRawVariantAggregationInputs(
          new AggregationFunction[]{aggregationFunction(AggregationFunctionType.MINSTRING)},
          projectOperator(dataType));
    }
  }

  @Test
  public void testNonVariantSchemaSkipsIdentifierDataSourceValidation() {
    QueryContext queryContext = new QueryContext.Builder().build();
    queryContext.setSchema(
        new Schema.SchemaBuilder().addSingleValueDimension("payload", FieldSpec.DataType.STRING).build());
    SegmentContext segmentContext = mock(SegmentContext.class);
    BaseFilterOperator filterOperator = mock(BaseFilterOperator.class);
    when(filterOperator.isResultEmpty()).thenReturn(true);

    AggregationFunction[] aggregationFunctions =
        new AggregationFunction[]{aggregationFunction(AggregationFunctionType.SUM)};
    for (int i = 0; i < 100; i++) {
      assertNull(AggregationFunctionUtils.buildAggregationInfoWithStarTree(segmentContext, queryContext,
          aggregationFunctions, null, filterOperator, List.of()));
    }
    // The query-wide schema gate prevents any per-segment data-source lookup for non-VARIANT tables.
    verifyNoInteractions(segmentContext);
  }

  @Test
  public void testNoSchemaPreservesRawVariantIdentifierValidation() {
    QueryContext queryContext = new QueryContext.Builder().build();
    SegmentContext segmentContext = mock(SegmentContext.class);
    IndexSegment indexSegment = mock(IndexSegment.class);
    DataSource dataSource = mock(DataSource.class);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    BaseFilterOperator filterOperator = mock(BaseFilterOperator.class);
    when(segmentContext.getIndexSegment()).thenReturn(indexSegment);
    when(indexSegment.getDataSource("payload", null)).thenReturn(dataSource);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    when(dataSourceMetadata.getDataType()).thenReturn(FieldSpec.DataType.VARIANT);

    IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class,
        () -> AggregationFunctionUtils.buildAggregationInfoWithStarTree(segmentContext, queryContext,
            new AggregationFunction[]{aggregationFunction(AggregationFunctionType.SUM)}, null, filterOperator,
            List.of()));
    Assert.assertTrue(exception.getMessage().contains("does not support raw VARIANT values"));
  }

  private static AggregationFunction aggregationFunction(AggregationFunctionType functionType) {
    AggregationFunction aggregationFunction = mock(AggregationFunction.class);
    when(aggregationFunction.getType()).thenReturn(functionType);
    when(aggregationFunction.getInputExpressions()).thenReturn(List.of(PAYLOAD));
    return aggregationFunction;
  }

  private static BaseProjectOperator<?> projectOperator(FieldSpec.DataType dataType) {
    BaseProjectOperator<?> projectOperator = mock(BaseProjectOperator.class);
    ColumnContext columnContext = mock(ColumnContext.class);
    when(columnContext.getDataType()).thenReturn(dataType);
    when(projectOperator.getResultColumnContext(PAYLOAD)).thenReturn(columnContext);
    return projectOperator;
  }
}
