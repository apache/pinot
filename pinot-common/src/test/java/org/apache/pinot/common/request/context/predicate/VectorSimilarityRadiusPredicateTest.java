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
package org.apache.pinot.common.request.context.predicate;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link VectorSimilarityRadiusPredicate}.
 */
public class VectorSimilarityRadiusPredicateTest {

  @Test
  public void testConstructionAndGetters() {
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    float[] value = {1.0f, 2.0f, 3.0f};
    float threshold = 0.75f;

    VectorSimilarityRadiusPredicate predicate = new VectorSimilarityRadiusPredicate(lhs, value, threshold);

    Assert.assertEquals(predicate.getType(), Predicate.Type.VECTOR_SIMILARITY_RADIUS);
    Assert.assertEquals(predicate.getLhs(), lhs);
    Assert.assertEquals(predicate.getValue(), value);
    Assert.assertEquals(predicate.getThreshold(), threshold);
  }

  @Test
  public void testDefaultThreshold() {
    Assert.assertEquals(VectorSimilarityRadiusPredicate.DEFAULT_THRESHOLD, 0.5f);
  }

  @Test
  public void testDefaultInternalLimit() {
    Assert.assertEquals(VectorSimilarityRadiusPredicate.DEFAULT_INTERNAL_LIMIT, 100_000);
  }

  @Test
  public void testEquality() {
    ExpressionContext lhs1 = ExpressionContext.forIdentifier("embedding");
    ExpressionContext lhs2 = ExpressionContext.forIdentifier("embedding");
    float[] value1 = {1.0f, 2.0f};
    float[] value2 = {1.0f, 2.0f};

    VectorSimilarityRadiusPredicate p1 = new VectorSimilarityRadiusPredicate(lhs1, value1, 0.5f);
    VectorSimilarityRadiusPredicate p2 = new VectorSimilarityRadiusPredicate(lhs2, value2, 0.5f);

    Assert.assertEquals(p1, p2);
    Assert.assertEquals(p1.hashCode(), p2.hashCode());
  }

  @Test
  public void testInequalityDifferentThreshold() {
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    float[] value = {1.0f, 2.0f};

    VectorSimilarityRadiusPredicate p1 = new VectorSimilarityRadiusPredicate(lhs, value, 0.5f);
    VectorSimilarityRadiusPredicate p2 = new VectorSimilarityRadiusPredicate(lhs, value, 1.0f);

    Assert.assertNotEquals(p1, p2);
  }

  @Test
  public void testInequalityDifferentVector() {
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    float[] value1 = {1.0f, 2.0f};
    float[] value2 = {3.0f, 4.0f};

    VectorSimilarityRadiusPredicate p1 = new VectorSimilarityRadiusPredicate(lhs, value1, 0.5f);
    VectorSimilarityRadiusPredicate p2 = new VectorSimilarityRadiusPredicate(lhs, value2, 0.5f);

    Assert.assertNotEquals(p1, p2);
  }

  @Test
  public void testInequalityDifferentColumn() {
    ExpressionContext lhs1 = ExpressionContext.forIdentifier("embedding1");
    ExpressionContext lhs2 = ExpressionContext.forIdentifier("embedding2");
    float[] value = {1.0f, 2.0f};

    VectorSimilarityRadiusPredicate p1 = new VectorSimilarityRadiusPredicate(lhs1, value, 0.5f);
    VectorSimilarityRadiusPredicate p2 = new VectorSimilarityRadiusPredicate(lhs2, value, 0.5f);

    Assert.assertNotEquals(p1, p2);
  }

  @Test
  public void testNotEqualToVectorSimilarityPredicate() {
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    float[] value = {1.0f, 2.0f};

    VectorSimilarityRadiusPredicate radiusPredicate = new VectorSimilarityRadiusPredicate(lhs, value, 0.5f);
    VectorSimilarityPredicate topKPredicate = new VectorSimilarityPredicate(lhs, value, 10);

    Assert.assertNotEquals(radiusPredicate, topKPredicate);
  }

  @Test
  public void testToString() {
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    float[] value = {1.0f, 2.0f};

    VectorSimilarityRadiusPredicate predicate = new VectorSimilarityRadiusPredicate(lhs, value, 0.5f);
    String str = predicate.toString();

    Assert.assertTrue(str.contains("vector_similarity_radius"));
    Assert.assertTrue(str.contains("embedding"));
    Assert.assertTrue(str.contains("0.5"));
  }
}
