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
package org.apache.pinot.sql.parsers;

import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityRadiusPredicate;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for parsing VECTOR_SIMILARITY_RADIUS SQL and ensuring backward compatibility
 * of the existing VECTOR_SIMILARITY function.
 */
public class CalciteSqlParserVectorRadiusTest {

  @Test
  public void testParseVectorSimilarityRadius() {
    String sql = "SELECT * FROM t WHERE VECTOR_SIMILARITY_RADIUS(col, ARRAY[1.0, 2.0], 0.5)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertNotNull(pinotQuery.getFilterExpression());

    FilterContext filter = RequestContextUtils.getFilter(pinotQuery.getFilterExpression());
    Assert.assertNotNull(filter.getPredicate());
    Assert.assertEquals(filter.getPredicate().getType(), Predicate.Type.VECTOR_SIMILARITY_RADIUS);

    VectorSimilarityRadiusPredicate predicate = (VectorSimilarityRadiusPredicate) filter.getPredicate();
    Assert.assertEquals(predicate.getLhs().getIdentifier(), "col");
    float[] expectedVector = {1.0f, 2.0f};
    Assert.assertEquals(predicate.getValue().length, expectedVector.length);
    Assert.assertEquals(predicate.getValue()[0], expectedVector[0], 0.001f);
    Assert.assertEquals(predicate.getValue()[1], expectedVector[1], 0.001f);
    Assert.assertEquals(predicate.getThreshold(), 0.5f, 0.001f);
  }

  @Test
  public void testParseVectorSimilarityRadiusWithDefaultThreshold() {
    String sql = "SELECT * FROM t WHERE VECTOR_SIMILARITY_RADIUS(col, ARRAY[1.0, 2.0])";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertNotNull(pinotQuery.getFilterExpression());

    FilterContext filter = RequestContextUtils.getFilter(pinotQuery.getFilterExpression());
    VectorSimilarityRadiusPredicate predicate = (VectorSimilarityRadiusPredicate) filter.getPredicate();
    Assert.assertEquals(predicate.getThreshold(), VectorSimilarityRadiusPredicate.DEFAULT_THRESHOLD, 0.001f);
  }

  @Test
  public void testParseVectorSimilarityRadiusWithIntegerThreshold() {
    String sql = "SELECT * FROM t WHERE VECTOR_SIMILARITY_RADIUS(col, ARRAY[1.0, 2.0], 1)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    FilterContext filter = RequestContextUtils.getFilter(pinotQuery.getFilterExpression());
    VectorSimilarityRadiusPredicate predicate = (VectorSimilarityRadiusPredicate) filter.getPredicate();
    Assert.assertEquals(predicate.getThreshold(), 1.0f, 0.001f);
  }

  @Test
  public void testBackwardCompatibilityVectorSimilarity() {
    // Existing VECTOR_SIMILARITY should still parse correctly
    String sql = "SELECT * FROM t WHERE VECTOR_SIMILARITY(col, ARRAY[1.0, 2.0, 3.0], 5)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertNotNull(pinotQuery.getFilterExpression());

    FilterContext filter = RequestContextUtils.getFilter(pinotQuery.getFilterExpression());
    Assert.assertNotNull(filter.getPredicate());
    Assert.assertEquals(filter.getPredicate().getType(), Predicate.Type.VECTOR_SIMILARITY);

    VectorSimilarityPredicate predicate = (VectorSimilarityPredicate) filter.getPredicate();
    Assert.assertEquals(predicate.getLhs().getIdentifier(), "col");
    Assert.assertEquals(predicate.getTopK(), 5);
  }

  @Test
  public void testBackwardCompatibilityVectorSimilarityDefaultTopK() {
    String sql = "SELECT * FROM t WHERE VECTOR_SIMILARITY(col, ARRAY[1.0, 2.0])";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    FilterContext filter = RequestContextUtils.getFilter(pinotQuery.getFilterExpression());
    VectorSimilarityPredicate predicate = (VectorSimilarityPredicate) filter.getPredicate();
    Assert.assertEquals(predicate.getTopK(), VectorSimilarityPredicate.DEFAULT_TOP_K);
  }

  @Test
  public void testCompoundVectorQueryAndCombination() {
    // Compound query: top-K AND radius
    String sql = "SELECT * FROM t WHERE VECTOR_SIMILARITY(col, ARRAY[1.0, 2.0], 10) "
        + "AND VECTOR_SIMILARITY_RADIUS(col, ARRAY[1.0, 2.0], 0.5)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertNotNull(pinotQuery.getFilterExpression());

    FilterContext filter = RequestContextUtils.getFilter(pinotQuery.getFilterExpression());
    Assert.assertEquals(filter.getType(), FilterContext.Type.AND);
    Assert.assertEquals(filter.getChildren().size(), 2);

    FilterContext child0 = filter.getChildren().get(0);
    FilterContext child1 = filter.getChildren().get(1);
    Assert.assertEquals(child0.getPredicate().getType(), Predicate.Type.VECTOR_SIMILARITY);
    Assert.assertEquals(child1.getPredicate().getType(), Predicate.Type.VECTOR_SIMILARITY_RADIUS);
  }
}
