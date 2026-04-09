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
package org.apache.pinot.common.request.context;

import java.util.List;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests filter conversion for literal-only expressions on the right-hand side.
 */
public class RequestContextUtilsTest {
  private static final String UUID_1 = "550e8400-e29b-41d4-a716-446655440000";
  private static final String UUID_2 = "550e8400-e29b-41d4-a716-446655440001";

  @Test
  public void testGetFilterWithUuidCastLiteralRhs() {
    FilterContext filter =
        RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression("uuidCol = CAST('" + UUID_1 + "' AS UUID)"));

    Assert.assertEquals(filter.getType(), FilterContext.Type.PREDICATE);
    EqPredicate predicate = (EqPredicate) filter.getPredicate();
    Assert.assertEquals(predicate.getLhs().getIdentifier(), "uuidCol");
    Assert.assertEquals(predicate.getValue(), UUID_1);
  }

  @Test
  public void testGetFilterExpressionContextWithUuidCastLiteralIn() {
    FilterContext filter = RequestContextUtils.getFilter(
        RequestContextUtils.getExpression("uuidCol IN (CAST('" + UUID_1 + "' AS UUID), CAST('" + UUID_2 + "' AS UUID))"));

    Assert.assertEquals(filter.getType(), FilterContext.Type.PREDICATE);
    InPredicate predicate = (InPredicate) filter.getPredicate();
    Assert.assertEquals(predicate.getLhs().getIdentifier(), "uuidCol");
    Assert.assertEquals(predicate.getValues(), List.of(UUID_1, UUID_2));
  }
}
