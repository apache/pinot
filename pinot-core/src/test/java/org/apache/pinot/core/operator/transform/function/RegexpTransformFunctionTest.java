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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class RegexpTransformFunctionTest extends BaseTransformFunctionTest {
  private static final String REGEXP = "(.*)([\\d]+)";
  private static final String MALFORMED_REGEXP = ".*([\\d]+";
  private static final Pattern PATTERN = Pattern.compile(REGEXP);

  @Test(dataProvider = "testLegalArguments")
  public void testLegalArguments(String expressionStr, int position, int occurance) {
    ExpressionContext expression = RequestContextUtils.getExpressionFromSQL(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    String[] actualValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Matcher matcher = PATTERN.matcher(_stringSVValues[i]);
      Assert.assertEquals(matcher.find(position - 1) ? matcher.group(occurance - 1) : null, actualValues[i]);
    }
  }

  @DataProvider(name = "testLegalArguments")
  public Object[][] testLegalArguments() {
    return new Object[][]{
        new Object[]{String.format("REGEXP_EXTRACT(%s,'%s')", STRING_SV_COLUMN, REGEXP), 1, 1},
        new Object[]{String.format("REGEXP_EXTRACT(%s, '%s', 2)", STRING_SV_COLUMN, REGEXP), 2, 1},
        new Object[]{String.format("REGEXP_EXTRACT(%s, '%s', 2, 2)", STRING_SV_COLUMN, REGEXP), 2, 2}
    };
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpressionFromSQL(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{
        new Object[]{String.format("REGEXP_EXTRACT(%s)", STRING_SV_COLUMN)},
        new Object[]{String.format("REGEXP_EXTRACT(%s, '%s')", STRING_SV_COLUMN, MALFORMED_REGEXP)},
        new Object[]{String.format("REGEXP_EXTRACT(%s, '%s', 0)", STRING_SV_COLUMN, REGEXP)},
        new Object[]{String.format("REGEXP_EXTRACT(%s, '%s', 1, 0)", STRING_SV_COLUMN, REGEXP)}
    };
  }
}
