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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.Test;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Unit test for {@link AggregationFunctionUtils#getAggregationResult}, the metadata/dictionary based aggregation
/// result resolver used by the non-scan based and partial metadata based aggregation paths.
@SuppressWarnings("rawtypes")
public class AggregationFunctionUtilsTest {
  private static final ExpressionContext INPUT_EXPRESSION = ExpressionContext.forIdentifier("inputCol");

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
  public void testDistinctCountHllMvOffersUuidDictionaryBytesAsRawValues()
      throws IOException {
    DistinctCountHLLMVAggregationFunction function =
        new DistinctCountHLLMVAggregationFunction(List.of(INPUT_EXPRESSION));
    byte[][] bytesValues = {
        UuidUtils.toBytes("550e8400-e29b-41d4-a716-446655440000"),
        UuidUtils.toBytes("550e8400-e29b-41d4-a716-446655440001"),
        UuidUtils.toBytes("550e8400-e29b-41d4-a716-446655440002")
    };
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.length()).thenReturn(bytesValues.length);
    for (int i = 0; i < bytesValues.length; i++) {
      when(dictionary.get(i)).thenReturn(bytesValues[i]);
    }
    DataSource dataSource = mockDataSource(dictionary, DataType.UUID, false);

    HyperLogLog result = (HyperLogLog) AggregationFunctionUtils.getAggregationResult(function, dataSource, 3, "TEST");

    HyperLogLog expected = new HyperLogLog(function.getLog2m());
    for (byte[] value : bytesValues) {
      expected.offer(value);
    }
    assertEquals(result.cardinality(), 3L);
    assertTrue(Arrays.equals(result.getBytes(), expected.getBytes()));
    for (int i = 0; i < bytesValues.length; i++) {
      verify(dictionary).get(i);
    }
    verify(dictionary, never()).getBytesValue(anyInt());
  }

  @Test
  public void testDistinctCountHllMergesLogicalBytesAsSerializedState()
      throws IOException {
    DistinctCountHLLAggregationFunction function =
        new DistinctCountHLLAggregationFunction(List.of(INPUT_EXPRESSION));
    byte[] firstSketch = serializedHll(function, "a", "b");
    byte[] secondSketch = serializedHll(function, "b", "c");
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.length()).thenReturn(2);
    when(dictionary.getBytesValue(0)).thenReturn(firstSketch);
    when(dictionary.getBytesValue(1)).thenReturn(secondSketch);
    // Logical BYTES determines serialized-state handling independently of the cardinality metadata.
    DataSource dataSource = mockDataSource(dictionary, DataType.BYTES, false);

    HyperLogLog result = (HyperLogLog) AggregationFunctionUtils.getAggregationResult(function, dataSource, 2, "TEST");

    HyperLogLog expected = new HyperLogLog(function.getLog2m());
    expected.offer("a");
    expected.offer("b");
    expected.offer("c");
    assertEquals(result.cardinality(), 3L);
    assertTrue(Arrays.equals(result.getBytes(), expected.getBytes()));
    verify(dictionary).getBytesValue(0);
    verify(dictionary).getBytesValue(1);
    verify(dictionary, never()).get(anyInt());
  }

  private static DataSource mockDataSource(Dictionary dictionary, DataType dataType, boolean singleValue) {
    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getDataType()).thenReturn(dataType);
    when(metadata.isSingleValue()).thenReturn(singleValue);
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getDictionary()).thenReturn(dictionary);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    return dataSource;
  }

  private static byte[] serializedHll(DistinctCountHLLAggregationFunction function, String... values) {
    HyperLogLog hll = new HyperLogLog(function.getLog2m());
    for (String value : values) {
      hll.offer(value);
    }
    return ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hll);
  }
}
