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
package org.apache.pinot.core.query.optimizer.filter;

import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TimePredicateFilterOptimizerTest {
  private static final TimePredicateFilterOptimizer OPTIMIZER = new TimePredicateFilterOptimizer();

  @Test
  public void testTimeConvert() {
    // Same input/output format
    testNoOpTimeConvert("timeConvert(col, 'MILLISECONDS', 'MILLISECONDS') > 1620830760000");
    testNoOpTimeConvert("TIME_CONVERT(col, 'MILLISECONDS', 'MILLISECONDS') < 1620917160000");
    testNoOpTimeConvert("timeconvert(col, 'MILLISECONDS', 'MILLISECONDS') BETWEEN 1620830760000 AND 1620917160000");
    testNoOpTimeConvert("TIMECONVERT(col, 'MILLISECONDS', 'MILLISECONDS') = 1620830760000");

    // Other output format
    testTimeConvert("timeConvert(col, 'MILLISECONDS', 'SECONDS') > 1620830760",
        new Range(1620830761000L, true, null, false));
    testTimeConvert("timeConvert(col, 'MILLISECONDS', 'MINUTES') < 27015286",
        new Range(null, false, 1620917160000L, false));
    testTimeConvert("timeConvert(col, 'MILLISECONDS', 'HOURS') BETWEEN 450230 AND 450254",
        new Range(1620828000000L, true, 1620918000000L, false));
    testTimeConvert("timeConvert(col, 'MILLISECONDS', 'DAYS') = 18759",
        new Range(1620777600000L, true, 1620864000000L, false));

    // Other input format
    testTimeConvert("timeConvert(col, 'MINUTES', 'SECONDS') > 1620830760", new Range(27013846L, false, null, false));
    testTimeConvert("timeConvert(col, 'HOURS', 'MINUTES') < 27015286", new Range(null, false, 450254L, true));
    testTimeConvert("timeConvert(col, 'DAYS', 'HOURS') BETWEEN 450230 AND 450254",
        new Range(18759L, false, 18760L, true));
    testTimeConvert("timeConvert(col, 'SECONDS', 'DAYS') = 18759", new Range(1620777600L, true, 1620864000L, false));

    // Invalid time
    testInvalidTimeConvert("timeConvert(col, 'MINUTES', 'SECONDS') > 1620830760.5");
    testInvalidTimeConvert("timeConvert(col, 'HOURS', 'MINUTES') > 1620830760");
  }

  @Test
  public void testEpochToEpochDateTimeConvert() {
    // Value not on granularity boundary
    testTimeConvert(
        "dateTimeConvert(col, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES') > 1620830760000",
        new Range(1620831600000L, true, null, false));
    testTimeConvert(
        "DATE_TIME_CONVERT(col, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES') < 1620917160000",
        new Range(null, false, 1620918000000L, false));
    testTimeConvert(
        "datetimeconvert(col, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES') BETWEEN 1620830760000 AND 1620917160000",
        new Range(1620831600000L, true, 1620918000000L, false));
    testTimeConvert(
        "DATETIMECONVERT(col, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES') = 1620830760000",
        new Range(1620831600000L, true, 1620831600000L, false));

    // Value on granularity boundary
    testTimeConvert(
        "dateTimeConvert(col, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES') > 1620831600000",
        new Range(1620833400000L, true, null, false));
    testTimeConvert(
        "dateTimeConvert(col, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES') < 1620918000000",
        new Range(null, false, 1620918000000L, false));
    testTimeConvert(
        "dateTimeConvert(col, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES') BETWEEN 1620831600000 AND 1620918000000",
        new Range(1620831600000L, true, 1620919800000L, false));
    testTimeConvert(
        "dateTimeConvert(col, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES') = 1620831600000",
        new Range(1620831600000L, true, 1620833400000L, false));

    // Other output format
    testTimeConvert("dateTimeConvert(col, '1:MILLISECONDS:EPOCH', '1:MINUTES:EPOCH', '30:MINUTES') > 27013846",
        new Range(1620831600000L, true, null, false));
    testTimeConvert("dateTimeConvert(col, '1:MILLISECONDS:EPOCH', '10:MINUTES:EPOCH', '30:MINUTES') < 2701528",
        new Range(null, false, 1620918000000L, false));
    testTimeConvert(
        "dateTimeConvert(col, '1:MILLISECONDS:EPOCH', '1:SECONDS:EPOCH', '30:MINUTES') BETWEEN 1620830760 AND 1620917160",
        new Range(1620831600000L, true, 1620918000000L, false));
    testTimeConvert("dateTimeConvert(col, '1:MILLISECONDS:EPOCH', '30:MINUTES:EPOCH', '30:MINUTES') > 900462",
        new Range(1620833400000L, true, null, false));
    testTimeConvert("dateTimeConvert(col, '1:MILLISECONDS:EPOCH', '1:HOURS:EPOCH', '30:MINUTES') < 450255",
        new Range(null, false, 1620918000000L, false));
    testTimeConvert(
        "dateTimeConvert(col, '1:MILLISECONDS:EPOCH', '1:DAYS:EPOCH', '30:MINUTES') BETWEEN 18759 AND 18760",
        new Range(1620777600000L, true, 1620950400000L, false));
    testTimeConvert("dateTimeConvert(col, '1:MILLISECONDS:EPOCH', '1:DAYS:EPOCH', '30:MINUTES') = 18759",
        new Range(1620777600000L, true, 1620864000000L, false));

    // Other input format
    testTimeConvert("dateTimeConvert(col, '1:SECONDS:EPOCH', '1:MINUTES:EPOCH', '30:MINUTES') > 27013846",
        new Range(1620831600L, true, null, false));
    testTimeConvert("dateTimeConvert(col, '1:MINUTES:EPOCH', '10:MINUTES:EPOCH', '30:MINUTES') < 2701528",
        new Range(null, false, 27015300L, false));
    testTimeConvert(
        "dateTimeConvert(col, '1:DAYS:EPOCH', '1:SECONDS:EPOCH', '30:MINUTES') BETWEEN 1620830760 AND 1620917160",
        new Range(18759L, false, 18760L, true));
    testTimeConvert("dateTimeConvert(col, '1:SECONDS:EPOCH', '30:MINUTES:EPOCH', '30:MINUTES') > 900462",
        new Range(1620833400L, true, null, false));
    testTimeConvert("dateTimeConvert(col, '1:MINUTES:EPOCH', '1:HOURS:EPOCH', '30:MINUTES') < 450255",
        new Range(null, false, 27015300L, false));
    testTimeConvert("dateTimeConvert(col, '1:DAYS:EPOCH', '1:DAYS:EPOCH', '30:MINUTES') BETWEEN 18759 AND 18760",
        new Range(18759L, true, 18761L, false));
    testTimeConvert("dateTimeConvert(col, '1:DAYS:EPOCH', '1:DAYS:EPOCH', '30:MINUTES') = 18759",
        new Range(18759L, true, 18760L, false));

    // Invalid time
    testInvalidTimeConvert("dateTimeConvert(col, '1:SECONDS:EPOCH', '1:MINUTES:EPOCH', '30:MINUTES') > 27013846.5");
    testInvalidTimeConvert("dateTimeConvert(col, '1:SECONDS:EPOCH', '30:MINUTES:EPOCH', '30:MINUTES') > 27013846");
  }

  @Test
  public void testSDFToEpochDateTimeConvert() {
    testTimeConvert(
        "dateTimeConvert(col, '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:MILLISECONDS:EPOCH', '30:MINUTES') > 1620830760000",
        new Range("2021-05-12 15:00:00.000", true, null, false));
    testTimeConvert(
        "dateTimeConvert(col, '1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss', '1:MILLISECONDS:EPOCH', '30:MINUTES') < 1620917160000",
        new Range(null, false, "2021-05-13 15:00:00", false));
    testTimeConvert(
        "dateTimeConvert(col, '1:MINUTES:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm', '1:MILLISECONDS:EPOCH', '30:MINUTES') BETWEEN 1620830760000 AND 1620917160000",
        new Range("2021-05-12 15:00", true, "2021-05-13 15:00", false));
    testTimeConvert(
        "dateTimeConvert(col, '1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd', '1:MILLISECONDS:EPOCH', '30:MINUTES') = 1620830760000",
        new Range("2021-05-12", false, "2021-05-12", true));

    // Invalid time
    testInvalidTimeConvert(
        "dateTimeConvert(col, '1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss.SSS', '1:MILLISECONDS:EPOCH', '30:MINUTES') > 1620830760000.5");
    testInvalidTimeConvert(
        "dateTimeConvert(col, '1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss', '1:MILLISECONDS:EPOCH', '30:MINUTES') < 1620917160");
  }

  /**
   * Helper method to test no-op TIME_CONVERT filter (same input and output time unit).
   */
  private void testNoOpTimeConvert(String filterString) {
    Expression originalExpression = CalciteSqlParser.compileToExpression(filterString);
    Function originalFunction = originalExpression.getFunctionCall();
    List<Expression> originalOperands = originalFunction.getOperands();
    Expression optimizedFilterExpression = OPTIMIZER.optimize(CalciteSqlParser.compileToExpression(filterString));
    Function optimizedFunction = optimizedFilterExpression.getFunctionCall();
    List<Expression> optimizedOperands = optimizedFunction.getOperands();
    assertEquals(optimizedFunction.getOperator(), originalFunction.getOperator());
    assertEquals(optimizedOperands.size(), originalOperands.size());
    // TIME_CONVERT transform should be removed
    assertEquals(optimizedOperands.get(0), originalOperands.get(0).getFunctionCall().getOperands().get(0));
    int numOperands = optimizedOperands.size();
    for (int i = 1; i < numOperands; i++) {
      assertEquals(optimizedOperands.get(i), originalOperands.get(i));
    }
  }

  /**
   * Helper method to test optimizing TIME_CONVERT/DATE_TIME_CONVERT on the given filter.
   */
  private void testTimeConvert(String filterString, Range expectedRange) {
    Expression originalExpression = CalciteSqlParser.compileToExpression(filterString);
    Expression optimizedFilterExpression = OPTIMIZER.optimize(CalciteSqlParser.compileToExpression(filterString));
    Function function = optimizedFilterExpression.getFunctionCall();
    assertEquals(function.getOperator(), FilterKind.RANGE.name());
    List<Expression> operands = function.getOperands();
    assertEquals(operands.size(), 2);
    assertEquals(operands.get(0),
        originalExpression.getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0));
    String rangeString = operands.get(1).getLiteral().getStringValue();
    assertEquals(rangeString, expectedRange.getRangeString());
  }

  /**
   * Helper method to test optimizing TIME_CONVERT/DATE_TIME_CONVERT with invalid time in filter.
   */
  private void testInvalidTimeConvert(String filterString) {
    Expression originalExpression = CalciteSqlParser.compileToExpression(filterString);
    Expression optimizedFilterExpression = OPTIMIZER.optimize(CalciteSqlParser.compileToExpression(filterString));
    assertEquals(optimizedFilterExpression, originalExpression);
  }
}
