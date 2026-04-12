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
package org.apache.pinot.common.filter;

import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class FilterPredicateRegistryTest {

  @AfterMethod
  public void tearDown() {
    FilterPredicateRegistry.clear();
  }

  @Test
  public void testCanonicalizedLookup() {
    FilterPredicatePlugin plugin = new TestFilterPredicatePlugin("LIKE_ANY");

    FilterPredicateRegistry.register(plugin);

    Assert.assertSame(FilterPredicateRegistry.get("LIKE_ANY"), plugin);
    Assert.assertSame(FilterPredicateRegistry.get("like_any"), plugin);
    Assert.assertSame(FilterPredicateRegistry.get("likeany"), plugin);
  }

  @Test
  public void testRejectsBuiltInFilterName() {
    IllegalStateException exception = Assert.expectThrows(IllegalStateException.class,
        () -> FilterPredicateRegistry.register(new TestFilterPredicatePlugin("TEXT_MATCH")));

    Assert.assertTrue(exception.getMessage().contains("collides with a built-in Pinot predicate or function"));
  }

  @Test
  public void testRejectsBuiltInFunctionName() {
    IllegalStateException exception = Assert.expectThrows(IllegalStateException.class,
        () -> FilterPredicateRegistry.register(new TestFilterPredicatePlugin("startsWith")));

    Assert.assertTrue(exception.getMessage().contains("collides with a built-in Pinot predicate or function"));
  }

  private static final class TestFilterPredicatePlugin implements FilterPredicatePlugin {
    private final String _name;

    private TestFilterPredicatePlugin(String name) {
      _name = name;
    }

    @Override
    public String name() {
      return _name;
    }

    @Override
    public void validateFilterExpression(List<Expression> operands) {
    }

    @Override
    public List<OperandType> getOperandTypes() {
      return List.of(OperandType.STRING, OperandType.STRING);
    }

    @Override
    public Predicate createPredicate(List<ExpressionContext> operands) {
      return new EqPredicate(ExpressionContext.forIdentifier("col"), "value");
    }
  }
}
