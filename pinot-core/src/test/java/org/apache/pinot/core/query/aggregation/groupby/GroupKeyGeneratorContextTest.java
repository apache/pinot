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
package org.apache.pinot.core.query.aggregation.groupby;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies the immutable metadata contract passed to custom group-key providers.
public class GroupKeyGeneratorContextTest {
  @Test
  public void testDefensiveCopies() {
    ExpressionContext expression = ExpressionContext.forIdentifier("intColumn");
    List<GroupKeyGeneratorContext.GroupKeySpec> groupKeySpecs = new ArrayList<>();
    groupKeySpecs.add(new GroupKeyGeneratorContext.GroupKeySpec(expression, DataType.INT, true, false,
        Optional.of(new GroupKeyGeneratorContext.IntegralDomain(1, 10)), OptionalInt.of(10)));
    Map<ExpressionContext, Integer> hints = new HashMap<>(Map.of(expression, 3));

    GroupKeyGeneratorContext context =
        new GroupKeyGeneratorContext(groupKeySpecs, hints, 100, 20, true);
    groupKeySpecs.clear();
    hints.clear();

    assertEquals(context.getGroupKeySpecs().size(), 1);
    assertEquals(context.getPredicateCardinalityHints(), Map.of(expression, 3));
    assertEquals(context.getNumGroupsLimit(), 100);
    assertEquals(context.getMaxInitialResultHolderCapacity(), 20);
    assertTrue(context.isNullHandlingEnabled());
    expectThrows(UnsupportedOperationException.class, () -> context.getGroupKeySpecs().clear());
  }

  @Test
  public void testRejectsInvalidIntegralDomain() {
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> new GroupKeyGeneratorContext.IntegralDomain(10, 1));
    assertEquals(exception.getMessage(), "minInclusive (10) must not exceed maxInclusive (1)");
  }
}
