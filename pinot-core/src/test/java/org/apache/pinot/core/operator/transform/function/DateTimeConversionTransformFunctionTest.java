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
package org.apache.pinot.core.operator.transform.function;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class DateTimeConversionTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testDateTimeConversionTransformFunction() {
    // NOTE: functionality of DateTimeConverter is covered in DateTimeConverterTest
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("dateTimeConvert(%s,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH','1:MINUTES')", TIME_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof DateTimeConversionTransformFunction);
    assertEquals(transformFunction.getName(), DateTimeConversionTransformFunction.FUNCTION_NAME);
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertTrue(resultMetadata.isSingleValue());
    assertEquals(resultMetadata.getDataType(), DataType.LONG);
    long[] expectedValues = new long[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = TimeUnit.MILLISECONDS.toMinutes(_timeValues[i]);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{
        new Object[]{
            String.format("dateTimeConvert(%s,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH')", TIME_COLUMN)
        }, new Object[]{"dateTimeConvert(5,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH','1:MINUTES')"}, new Object[]{
        String.format("dateTimeConvert(%s,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH','1:MINUTES')", INT_MV_COLUMN)
    }, new Object[]{
        String.format("dateTimeConvert(%s,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH','MINUTES:1')", TIME_COLUMN)
    }, new Object[]{
        String.format("dateTimeConvert(%s,%s,'1:MINUTES:EPOCH','1:MINUTES')", TIME_COLUMN, INT_SV_COLUMN)
    }
    };
  }

  @Test
  public void testDateTimeConversionTransformFunctionNullColumn() {
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("dateTimeConvert(%s,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH','1:MINUTES')",
            TIMESTAMP_COLUMN_NULL));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof DateTimeConversionTransformFunction);
    assertEquals(transformFunction.getName(), DateTimeConversionTransformFunction.FUNCTION_NAME);
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertTrue(resultMetadata.isSingleValue());
    assertEquals(resultMetadata.getDataType(), DataType.LONG);
    long[] expectedValues = new long[NUM_ROWS];
    RoaringBitmap expectedNulls = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i % 2 == 0) {
        expectedValues[i] = TimeUnit.MILLISECONDS.toMinutes(_timeValues[i]);
      } else {
        expectedNulls.add(i);
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, expectedNulls);
  }
}
