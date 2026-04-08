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
package org.apache.pinot.plugin.filter.example;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.pinot.common.filter.FilterPredicatePlugin;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.RegexpPatternConverterUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class LikeAnyPluginTest {

  @Test
  public void testPluginName() {
    LikeAnyPlugin plugin = new LikeAnyPlugin();
    Assert.assertEquals(plugin.name(), "LIKE_ANY");
  }

  @Test
  public void testOperandTypes() {
    LikeAnyPlugin plugin = new LikeAnyPlugin();
    List<FilterPredicatePlugin.OperandType> types = plugin.getOperandTypes();
    Assert.assertEquals(types.size(), 2);
    Assert.assertEquals(types.get(0), FilterPredicatePlugin.OperandType.STRING);
    Assert.assertEquals(types.get(1), FilterPredicatePlugin.OperandType.STRING);
  }

  @Test
  public void testCreatePredicate() {
    LikeAnyPlugin plugin = new LikeAnyPlugin();
    List<ExpressionContext> operands = List.of(
        ExpressionContext.forIdentifier("name"),
        ExpressionContext.forLiteral(new LiteralContext(DataType.STRING, "John%")),
        ExpressionContext.forLiteral(new LiteralContext(DataType.STRING, "%Smith"))
    );
    Predicate predicate = plugin.createPredicate(operands);
    Assert.assertTrue(predicate instanceof LikeAnyPredicate);
    LikeAnyPredicate likeAny = (LikeAnyPredicate) predicate;
    Assert.assertEquals(likeAny.getPatterns(), List.of("John%", "%Smith"));
    Assert.assertEquals(likeAny.getType(), Predicate.Type.CUSTOM);
    Assert.assertEquals(likeAny.getCustomTypeName(), "LIKE_ANY");
    Assert.assertEquals(likeAny.getLhs().getIdentifier(), "name");
  }

  @Test
  public void testRawEvaluator() {
    LikeAnyPredicate predicate = new LikeAnyPredicate(
        ExpressionContext.forIdentifier("name"),
        List.of("John%", "%Smith")
    );
    // Build combined regex
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < predicate.getPatterns().size(); i++) {
      if (i > 0) {
        sb.append('|');
      }
      sb.append('(').append(RegexpPatternConverterUtils.likeToRegexpLike(predicate.getPatterns().get(i))).append(')');
    }
    Pattern pattern = Pattern.compile(sb.toString());
    LikeAnyRawEvaluator evaluator = new LikeAnyRawEvaluator(predicate, pattern);

    Assert.assertTrue(evaluator.applySV("John Doe"));
    Assert.assertTrue(evaluator.applySV("Jane Smith"));
    Assert.assertTrue(evaluator.applySV("Johnny"));
    Assert.assertFalse(evaluator.applySV("Alice"));
    Assert.assertFalse(evaluator.applySV("Bob Jones"));
    Assert.assertEquals(evaluator.getDataType(), DataType.STRING);
    Assert.assertFalse(evaluator.isExclusive());
    Assert.assertFalse(evaluator.isDictionaryBased());
  }

  @Test
  public void testPredicateToString() {
    LikeAnyPredicate predicate = new LikeAnyPredicate(
        ExpressionContext.forIdentifier("col"),
        List.of("a%", "%b")
    );
    Assert.assertEquals(predicate.toString(), "like_any(col, a%, %b)");
  }
}
