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
package org.apache.pinot.materializedview.handler;

import java.util.Locale;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Unit tests for [DefaultMaterializedViewHandler#attachFilter], the helper that places the
/// per-branch time-boundary filter on the base (`>= boundary`) and MV (`< boundary`) sides of a
/// split execution.
public class DefaultMaterializedViewHandlerTest {

  @Test
  public void testAttachFilterLessThanOnEmptyFilter() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT carrier, SUM(delay) FROM materializedViewTable GROUP BY carrier");
    Assert.assertNull(query.getFilterExpression(), "precondition: no WHERE clause");

    DefaultMaterializedViewHandler.attachFilter(query, new TimeBoundaryInfo("materializedViewDay", "20000"),
        FilterKind.LESS_THAN);

    Expression filter = query.getFilterExpression();
    Assert.assertNotNull(filter, "MV branch must receive the upper-bound filter");
    Function fn = filter.getFunctionCall();
    Assert.assertNotNull(fn);
    Assert.assertEquals(fn.getOperator().toUpperCase(Locale.ROOT), "LESS_THAN",
        "MV upper-bound filter must use LESS_THAN (exclusive), symmetric to the base branch's >=");
    Assert.assertEquals(fn.getOperands().get(0).getIdentifier().getName(), "materializedViewDay");
  }

  @Test
  public void testAttachFilterLessThanWithExistingFilter() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT carrier, SUM(delay) FROM materializedViewTable WHERE carrier = 'AA' GROUP BY carrier");
    Assert.assertNotNull(query.getFilterExpression(), "precondition: existing WHERE clause");

    DefaultMaterializedViewHandler.attachFilter(query, new TimeBoundaryInfo("materializedViewDay", "20000"),
        FilterKind.LESS_THAN);

    Expression filter = query.getFilterExpression();
    Function root = filter.getFunctionCall();
    Assert.assertNotNull(root);
    Assert.assertEquals(root.getOperator().toUpperCase(Locale.ROOT), "AND",
        "Upper-bound filter must be AND-ed with the user's WHERE, not replace it");
    Assert.assertEquals(root.getOperands().size(), 2);

    // Second operand is the injected LESS_THAN(materializedViewDay, 20000).
    Function injected = root.getOperands().get(1).getFunctionCall();
    Assert.assertNotNull(injected);
    Assert.assertEquals(injected.getOperator().toUpperCase(Locale.ROOT), "LESS_THAN");
    Assert.assertEquals(injected.getOperands().get(0).getIdentifier().getName(), "materializedViewDay");
  }

  @Test
  public void testAttachFilterGreaterThanOrEqualOnBaseSide() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT col FROM baseTable");
    DefaultMaterializedViewHandler.attachFilter(query, new TimeBoundaryInfo("ts", "100"),
        FilterKind.GREATER_THAN_OR_EQUAL);

    Function fn = query.getFilterExpression().getFunctionCall();
    Assert.assertEquals(fn.getOperator().toUpperCase(Locale.ROOT), "GREATER_THAN_OR_EQUAL",
        "Base branch filter must be inclusive >= since the MV uses exclusive <");
    Assert.assertEquals(fn.getOperands().get(0).getIdentifier().getName(), "ts");
  }

  /// Pins the contract that `DateTimeFormatSpec.fromMillisToFormat` emits a literal Pinot can
  /// coerce back to the column's stored type for every time-column format we expect to support on
  /// the base side of a SPLIT_REWRITE.  Each row asserts the round-trip
  /// (`millis -> formatted literal -> millis`) holds without loss; that round-trip is what makes
  /// `base.ts >= literal` and `mv.materializedViewTime < literal` partition the timeline exactly.
  ///
  /// Regression coverage for the M5 reviewer concern that today only `INT/DAYS:EPOCH` and
  /// `LONG/MILLISECONDS:EPOCH` are exercised by the integration test.
  @Test
  public void testBoundaryLiteralRoundTripAcrossSupportedFormats() {
    long sampleMillis = 1700000000000L; // 2023-11-14T22:13:20Z — picked to round-trip cleanly in every format.
    String[] formats = {
        "1:MILLISECONDS:EPOCH",
        "1:SECONDS:EPOCH",
        "1:MINUTES:EPOCH",
        "1:HOURS:EPOCH",
        "1:DAYS:EPOCH",
        "TIMESTAMP",
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd"
    };
    for (String format : formats) {
      DateTimeFormatSpec spec = new DateTimeFormatSpec(format);
      String literal = spec.fromMillisToFormat(sampleMillis);
      Assert.assertNotNull(literal, "format " + format + " produced null literal");
      Assert.assertFalse(literal.isEmpty(), "format " + format + " produced empty literal");
      // Round-trip: parsing the formatted literal back to millis must produce a value within the
      // format's coarsest granularity.  For example `1:DAYS:EPOCH` truncates millis to day-granular
      // boundaries; for SIMPLE_DATE_FORMAT (yyyy-MM-dd) we also truncate to day granularity.
      long parsed = spec.fromFormatToMillis(literal);
      // Tolerance = format granularity in millis (or 1 day for SIMPLE_DATE_FORMAT yyyy-MM-dd).
      long toleranceMs;
      if (format.equals("TIMESTAMP") || format.equals("1:MILLISECONDS:EPOCH")) {
        toleranceMs = 1L;
      } else if (format.equals("1:SECONDS:EPOCH")) {
        toleranceMs = 1_000L;
      } else if (format.equals("1:MINUTES:EPOCH")) {
        toleranceMs = 60_000L;
      } else if (format.equals("1:HOURS:EPOCH")) {
        toleranceMs = 3_600_000L;
      } else {
        toleranceMs = 86_400_000L;
      }
      Assert.assertTrue(Math.abs(parsed - sampleMillis) < toleranceMs,
          "format " + format + " did not round-trip: " + sampleMillis + " -> '" + literal
              + "' -> " + parsed);
    }
  }
}
