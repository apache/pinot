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

import java.util.HashMap;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class MergeRegexFilterOptimizerTest {
  private static final MergeRegexFilterOptimizer OPTIMIZER = new MergeRegexFilterOptimizer();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName("t1")
          .addSingleValueDimension("col1", FieldSpec.DataType.STRING)
          .addSingleValueDimension("col2", FieldSpec.DataType.STRING)
          .build();


  @Test
  public void notDoesNotOptimizeIndexedBackreferences() {
    assertNotOptimized("not REGEXP_LIKE(col1, '(r1)\\1')");
  }

  @Test
  public void notDoesNotOptimizeNamedBackreferences() {
    assertNotOptimized("not REGEXP_LIKE(col1, '(?<x>r1)\\k<x>')");
  }

  @Test
  public void notDoesOptimizeRegex() {
    assertOptimizedTo("not REGEXP_LIKE(col1, 'r1')", "REGEXP_LIKE(col1, '(?!r1)')");
  }

  @Test
  public void notWithNot() {
    assertOptimizedTo("not not REGEXP_LIKE(col1, 'r1')", "REGEXP_LIKE(col1, '(?!(?!r1))')");
  }

  @Test
  public void notWithAnd() {
    assertOptimizedTo("not (REGEXP_LIKE(col1, 'r1') and REGEXP_LIKE(col1, 'r2'))",
        "REGEXP_LIKE(col1, '(?!(?=r1)(?=r2))')");
  }

  @Test
  public void notWithOr() {
    assertOptimizedTo("not (REGEXP_LIKE(col1, 'r1') or REGEXP_LIKE(col1, 'r2'))",
        "REGEXP_LIKE(col1, '(?!(?:r1)|(?:r2))')");
  }

  @Test
  public void andDoesNotOptimizeIndexedBackreferences() {
    assertNotOptimized("REGEXP_LIKE(col1, '(r1)\\1') and REGEXP_LIKE(col1, 'r2')");
    assertNotOptimized("REGEXP_LIKE(col1, 'r1') and REGEXP_LIKE(col1, '(r2)\\1')");
  }

  @Test
  public void andDoesNotOptimizeNamedBackreferences() {
    assertNotOptimized("REGEXP_LIKE(col1, '(?<x>r1)\\k<x>') and REGEXP_LIKE(col1, 'r2')");
    assertNotOptimized("REGEXP_LIKE(col1, 'r1') and REGEXP_LIKE(col1, '(?<x>r2)\\k<x>')");
  }

  @Test
  public void andDoesNotOptimizedWhenSingleTextMatchPerColumn() {
    // Same column, only one REGEXP_LIKE
    assertNotOptimized("REGEXP_LIKE(col1, 'r1') and col1 is not null");
    // two REGEXP_LIKE with different columns
    assertNotOptimized("REGEXP_LIKE(col1, 'r1') and REGEXP_LIKE(col2, 'r1')");
    // two REGEXP_LIKE with same columns, only one is optimizable regex
    assertNotOptimized("REGEXP_LIKE(col1, 'r1') and REGEXP_LIKE(col2, '(r1)\\1')");
  }

  @Test
  public void andDoesOptimizeRegex() {
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') and REGEXP_LIKE(col1, 'r2')", "REGEXP_LIKE(col1, '(?=r1)(?=r2)')");
  }

  @Test
  void andConservesOtherPredicates() {
    // CAUTION: order in the optimized version depends on the actual implementation
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') and REGEXP_LIKE(col1, 'r2') and col1 is not null",
        "col1 is not null and REGEXP_LIKE(col1, '(?=r1)(?=r2)')");
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') and col1 is not null and REGEXP_LIKE(col1, 'r2')",
        "col1 is not null and REGEXP_LIKE(col1, '(?=r1)(?=r2)')");
    assertOptimizedTo("col1 is not null and REGEXP_LIKE(col1, 'r1') and REGEXP_LIKE(col1, 'r2')",
        "col1 is not null and REGEXP_LIKE(col1, '(?=r1)(?=r2)')");

    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') and REGEXP_LIKE(col1, 'r2') and col2 is not null",
        "col2 is not null and REGEXP_LIKE(col1, '(?=r1)(?=r2)')");
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') and col2 is not null and REGEXP_LIKE(col1, 'r2')",
        "col2 is not null and REGEXP_LIKE(col1, '(?=r1)(?=r2)')");
    assertOptimizedTo("col2 is not null and REGEXP_LIKE(col1, 'r1') and REGEXP_LIKE(col1, 'r2')",
        "col2 is not null and REGEXP_LIKE(col1, '(?=r1)(?=r2)')");
  }

  @Test
  public void andMultipleColumns() {
    // CAUTION: order in the optimized version depends on the actual implementation
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') and REGEXP_LIKE(col1, 'r2') "
            + "and REGEXP_LIKE(col2, 'r1') and REGEXP_LIKE(col2, 'r2')",
        "REGEXP_LIKE(col2, '(?=r1)(?=r2)') and REGEXP_LIKE(col1, '(?=r1)(?=r2)')");
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') and REGEXP_LIKE(col2, 'r1') "
            + "and REGEXP_LIKE(col1, 'r2') and REGEXP_LIKE(col2, 'r2')",
        "REGEXP_LIKE(col2, '(?=r1)(?=r2)') and REGEXP_LIKE(col1, '(?=r1)(?=r2)')");
  }

  @Test
  public void andWithNot() {
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') and not REGEXP_LIKE(col1, 'r2')",
        "REGEXP_LIKE(col1, '(?=r1)(?=(?!r2))')");
    assertOptimizedTo("not REGEXP_LIKE(col1, 'r2') and REGEXP_LIKE(col1, 'r1')",
        "REGEXP_LIKE(col1, '(?=(?!r2))(?=r1)')");
  }

  @Test
  public void andWithAnd() {
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') and REGEXP_LIKE(col1, 'r2') and REGEXP_LIKE(col1, 'r3')",
        "REGEXP_LIKE(col1, '(?=r1)(?=r2)(?=r3)')");
  }

  @Test
  public void andWithOr() {
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') and (REGEXP_LIKE(col1, 'r2') or REGEXP_LIKE(col1, 'r3'))",
        "REGEXP_LIKE(col1, '(?=r1)(?=(?:r2)|(?:r3))')");
    assertOptimizedTo("(REGEXP_LIKE(col1, 'r2') or REGEXP_LIKE(col1, 'r3')) and REGEXP_LIKE(col1, 'r1')",
        "REGEXP_LIKE(col1, '(?=(?:r2)|(?:r3))(?=r1)')");
  }

  @Test
  public void orDoesNotOptimizeIndexedBackreferences() {
    assertNotOptimized("REGEXP_LIKE(col1, '(r1)\\1') or REGEXP_LIKE(col1, 'r2')");
    assertNotOptimized("REGEXP_LIKE(col1, 'r1') or REGEXP_LIKE(col1, '(r2)\\1')");
  }

  @Test
  public void orDoesNotOptimizeNamedBackreferences() {
    assertNotOptimized("REGEXP_LIKE(col1, '(?<x>r1)\\k<x>') or REGEXP_LIKE(col1, 'r2')");
    assertNotOptimized("REGEXP_LIKE(col1, 'r1') or REGEXP_LIKE(col1, '(?<x>r2)\\k<x>')");
  }

  @Test
  public void orDoesNotOptimizedWhenSingleTextMatchPerColumn() {
    // Same column, only one REGEXP_LIKE
    assertNotOptimized("REGEXP_LIKE(col1, 'r1') or col1 is not null");
    // two REGEXP_LIKE with different columns
    assertNotOptimized("REGEXP_LIKE(col1, 'r1') or REGEXP_LIKE(col2, 'r1')");
    // two REGEXP_LIKE with same columns, only one is optimizable regex
    assertNotOptimized("REGEXP_LIKE(col1, 'r1') or REGEXP_LIKE(col2, '(r1)\\1')");
  }

  @Test
  public void orDoesOptimizeRegex() {
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') or REGEXP_LIKE(col1, 'r2')", "REGEXP_LIKE(col1, '(?:r1)|(?:r2)')");
  }

  @Test
  void orConservesOtherPredicates() {
    // CAUTION: order in the optimized version depends on the actual implementation
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') or REGEXP_LIKE(col1, 'r2') or col1 is not null",
        "col1 is not null or REGEXP_LIKE(col1, '(?:r1)|(?:r2)')");
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') or col1 is not null or REGEXP_LIKE(col1, 'r2')",
        "col1 is not null or REGEXP_LIKE(col1, '(?:r1)|(?:r2)')");
    assertOptimizedTo("col1 is not null or REGEXP_LIKE(col1, 'r1') or REGEXP_LIKE(col1, 'r2')",
        "col1 is not null or REGEXP_LIKE(col1, '(?:r1)|(?:r2)')");


    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') or REGEXP_LIKE(col1, 'r2') or col2 is not null",
        "col2 is not null or REGEXP_LIKE(col1, '(?:r1)|(?:r2)')");
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') or col2 is not null or REGEXP_LIKE(col1, 'r2')",
        "col2 is not null or REGEXP_LIKE(col1, '(?:r1)|(?:r2)')");
    assertOptimizedTo("col2 is not null or REGEXP_LIKE(col1, 'r1') or REGEXP_LIKE(col1, 'r2')",
        "col2 is not null or REGEXP_LIKE(col1, '(?:r1)|(?:r2)')");
  }

  @Test
  public void orMultipleColumns() {
    // CAUTION: order in the optimized version depends on the actual implementation
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') or REGEXP_LIKE(col1, 'r2') "
            + "or REGEXP_LIKE(col2, 'r1') or REGEXP_LIKE(col2, 'r2')",
        "REGEXP_LIKE(col2, '(?:r1)|(?:r2)') or REGEXP_LIKE(col1, '(?:r1)|(?:r2)')");
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') or REGEXP_LIKE(col2, 'r1') "
            + "or REGEXP_LIKE(col1, 'r2') or REGEXP_LIKE(col2, 'r2')",
        "REGEXP_LIKE(col2, '(?:r1)|(?:r2)') or REGEXP_LIKE(col1, '(?:r1)|(?:r2)')");
  }

  @Test
  public void orWithNot() {
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') or not REGEXP_LIKE(col1, 'r2')",
        "REGEXP_LIKE(col1, '(?:r1)|(?:(?!r2))')");
    assertOptimizedTo("not REGEXP_LIKE(col1, 'r2') or REGEXP_LIKE(col1, 'r1')",
        "REGEXP_LIKE(col1, '(?:(?!r2))|(?:r1)')");
  }

  @Test
  public void orWithAnd() {
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') or (REGEXP_LIKE(col1, 'r2') and REGEXP_LIKE(col1, 'r3'))",
        "REGEXP_LIKE(col1, '(?:r1)|(?:(?=r2)(?=r3))')");
    assertOptimizedTo("(REGEXP_LIKE(col1, 'r2') and REGEXP_LIKE(col1, 'r3') or REGEXP_LIKE(col1, 'r1'))",
        "REGEXP_LIKE(col1, '(?:(?=r2)(?=r3))|(?:r1)')");
  }

  @Test
  public void orWithOr() {
    assertOptimizedTo("REGEXP_LIKE(col1, 'r1') or REGEXP_LIKE(col1, 'r2') or REGEXP_LIKE(col1, 'r3')",
        "REGEXP_LIKE(col1, '(?:r1)|(?:r2)|(?:r3)')");
  }


  private static void assertNotOptimized(String unoptimized) {
    assertOptimizedTo(unoptimized, unoptimized);
  }

  private static void assertOptimizedTo(String unoptimized, String optimized) {
    String prefix = "select * from t1 where ";

    PinotQuery unoptimizedQuery = CalciteSqlParser.compileToPinotQuery(prefix + unoptimized);
    Expression unoptimizedExpression = unoptimizedQuery.getFilterExpression();

    HashMap<String, String> queryOptions = new HashMap<>();
    queryOptions.put(CommonConstants.Query.Request.Optimization.FUSE_REGEX, "true");

    Expression actualExpression = OPTIMIZER.optimize(unoptimizedExpression, SCHEMA, queryOptions);

    PinotQuery expectedQuery = CalciteSqlParser.compileToPinotQuery(prefix + optimized);
    Expression expectedExpression = expectedQuery.getFilterExpression();


    assertEquals(actualExpression.toString(), expectedExpression.toString());
  }
}
