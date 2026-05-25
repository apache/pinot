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
package org.apache.pinot.core.operator.filter.predicate;

import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.math.BigDecimal;
import java.util.Set;
import java.util.SortedSet;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.core.operator.filter.predicate.InPredicateEvaluatorFactory.InRawPredicateEvaluator;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InPredicateEvaluatorFactoryTest {

  InRawPredicateEvaluator.Visitor<Integer> createValueSizeVisitor() {
    return new InRawPredicateEvaluator.Visitor<>() {
      @Override
      public Integer visitInt(IntSet matchingValues) {
        return matchingValues.size();
      }

      @Override
      public Integer visitLong(LongSet matchingValues) {
        return matchingValues.size();
      }

      @Override
      public Integer visitFloat(FloatSet matchingValues) {
        return matchingValues.size();
      }

      @Override
      public Integer visitDouble(DoubleSet matchingValues) {
        return matchingValues.size();
      }

      @Override
      public Integer visitBigDecimal(SortedSet<BigDecimal> matchingValues) {
        return matchingValues.size();
      }

      @Override
      public Integer visitString(Set<String> matchingValues) {
        return matchingValues.size();
      }

      @Override
      public Integer visitBytes(Set<ByteArray> matchingValues) {
        return matchingValues.size();
      }
    };
  }

  @Test
  void canBeVisited() {
    // Given a visitor
    InRawPredicateEvaluator.Visitor<Integer> valueSizeVisitor = Mockito.spy(createValueSizeVisitor());

    // When int predicate is used
    InPredicate predicate = new InPredicate(ExpressionContext.forIdentifier("ident"), Lists.newArrayList("1", "2"));

    InPredicateEvaluatorFactory.InRawPredicateEvaluator intEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, FieldSpec.DataType.INT);

    // Only the IntSet method is called
    int size = intEvaluator.accept(valueSizeVisitor);
    Assert.assertEquals(size, 2);
    Mockito.verify(valueSizeVisitor).visitInt(new IntOpenHashSet(new int[] {1, 2}));
    Mockito.verifyNoMoreInteractions(valueSizeVisitor);

    // And given a string predicate
    InPredicateEvaluatorFactory.InRawPredicateEvaluator strEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, FieldSpec.DataType.STRING);

    // Only the Set<String> method is called
    size = strEvaluator.accept(valueSizeVisitor);
    Assert.assertEquals(size, 2);
    Mockito.verify(valueSizeVisitor).visitString(Set.of("1", "2"));
    Mockito.verifyNoMoreInteractions(valueSizeVisitor);
  }
}
