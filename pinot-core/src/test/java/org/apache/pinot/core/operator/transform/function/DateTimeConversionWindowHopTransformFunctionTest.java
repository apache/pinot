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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class DateTimeConversionWindowHopTransformFunctionTest extends BaseTransformFunctionTest {
  @Test
  public void testDateTimeConversionWindowHopEpochTransformFunction() {
    // NOTE: functionality of DateTimeConverterWindowHop is covered in DateTimeConverterWindowHop
    ExpressionContext expression = RequestContextUtils.getExpression(String.format(
        "dateTimeConvertWindowHop(%s,'1:MILLISECONDS:EPOCH'," + "'1:MINUTES:EPOCH','1:MINUTES', '2:MINUTES')",
        TIME_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);

    assertTrue(transformFunction instanceof DateTimeConversionHopTransformFunction);
    assertEquals(transformFunction.getName(), DateTimeConversionHopTransformFunction.FUNCTION_NAME);

    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertFalse(resultMetadata.isSingleValue());
    assertEquals(resultMetadata.getDataType(), FieldSpec.DataType.LONG);
    long[][] expectedValues = new long[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = new long[2];
      expectedValues[i][0] = TimeUnit.MILLISECONDS.toMinutes(_timeValues[i]);
      expectedValues[i][1] = TimeUnit.MILLISECONDS.toMinutes(_timeValues[i]) - 1;
    }
    testTransformFunctionMV(transformFunction, expectedValues);
  }

  @Test
  public void testDateTimeWindowHopSDFTransformFunction() {
    // NOTE: functionality of DateTimeConverterWindowHop is covered in DateTimeConverterWindowHop
    ExpressionContext expression = RequestContextUtils.getExpression(String.format(
        "dateTimeConvertWindowHop(%s,'1:MILLISECONDS:EPOCH',"
            + "'1:MINUTES:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm tz(GMT)','1:MINUTES', '2:MINUTES')", TIME_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);

    assertTrue(transformFunction instanceof DateTimeConversionHopTransformFunction);
    assertEquals(transformFunction.getName(), DateTimeConversionHopTransformFunction.FUNCTION_NAME);

    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertFalse(resultMetadata.isSingleValue());
    assertEquals(resultMetadata.getDataType(), FieldSpec.DataType.STRING);

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm").withZone(DateTimeZone.UTC);

    String[][] expectedValues = new String[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = new String[2];
      expectedValues[i][0] = formatter.print(_timeValues[i]);
      expectedValues[i][1] = formatter.print(_timeValues[i] - 60 * 1000);
    }
    testTransformFunctionMV(transformFunction, expectedValues);
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
            String.format("dateTimeConvertWindowHop(%s,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH', '1:MINUTE')",
                TIME_COLUMN)
        }, new Object[]{
        "dateTimeConvertWindowHop(5,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH','1:MINUTES', '2:MINUTES')"
    }, new Object[]{
        String.format(
            "dateTimeConvertWindowHop(%s,'1:MILLISECONDS:EPOCH'," + "'1:MINUTES:EPOCH','1:MINUTES', '2:MINUTES')",
            LONG_MV_COLUMN)
    }, new Object[]{
        String.format(
            "dateTimeConvertWindowHop(%s,'1:MILLISECONDS:EPOCH'," + "'1:MINUTES:EPOCH','MINUTES:1', '2:MINUTES')",
            TIME_COLUMN)
    }, new Object[]{
        String.format("dateTimeConvertWindowHop(%s, %s,'1:MINUTES:EPOCH'," + "'1:MINUTES', '2:MINUTES')", TIME_COLUMN,
            INT_SV_COLUMN)
    }
    };
  }

  @Test
  public void testDateTimeConversionTransformFunctionNullColumn() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format(
        "dateTimeConvertWindowHop(%s,'1:MILLISECONDS:EPOCH','1:MINUTES:EPOCH'," + "'1:MINUTES', '2:MINUTES')",
        TIMESTAMP_COLUMN_NULL));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof DateTimeConversionHopTransformFunction);
    assertEquals(transformFunction.getName(), DateTimeConversionHopTransformFunction.FUNCTION_NAME);
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertEquals(resultMetadata.getDataType(), FieldSpec.DataType.LONG);
    RoaringBitmap expectedNulls = new RoaringBitmap();
    long[][] expectedValues = new long[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedNulls.add(i);
      } else {
        expectedValues[i] = new long[2];
        expectedValues[i][0] = TimeUnit.MILLISECONDS.toMinutes(_timeValues[i]);
        expectedValues[i][1] = TimeUnit.MILLISECONDS.toMinutes(_timeValues[i]) - 1;
      }
    }
    testTransformFunctionMVWithNull(transformFunction, expectedValues, expectedNulls);
  }
}
