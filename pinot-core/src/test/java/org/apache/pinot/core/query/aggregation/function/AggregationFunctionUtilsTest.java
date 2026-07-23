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

import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/**
 * Unit test for {@link AggregationFunctionUtils#getAggregationResult}, the metadata/dictionary based aggregation
 * result resolver used by the non-scan based and partial metadata based aggregation paths.
 */
@SuppressWarnings("rawtypes")
public class AggregationFunctionUtilsTest {

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
}
