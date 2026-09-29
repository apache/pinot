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
package org.apache.pinot.segment.local.aggregator;

import java.util.List;
import org.apache.datasketches.tuple.TupleSketch;
import org.apache.datasketches.tuple.TupleUnion;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerTupleSketch;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class IntegerTupleSketchValueAggregatorTest {

  private byte[] sketchContaining(String key, int value) {
    IntegerTupleSketch is = new IntegerTupleSketch(16, IntegerSummary.Mode.Sum);
    is.update(key, value);
    return is.compact().toByteArray();
  }

  @Test
  public void initialShouldParseASketch() {
    IntegerTupleSketchValueAggregator agg =
        new IntegerTupleSketchValueAggregator(List.of(), IntegerSummary.Mode.Sum);
    assertEquals(toSketch(agg.getInitialAggregatedValue(sketchContaining("hello world", 1))).getEstimate(), 1.0);
  }

  @Test
  public void nullInitialShouldReturnEmptySketch() {
    IntegerTupleSketchValueAggregator agg =
        new IntegerTupleSketchValueAggregator(List.of(), IntegerSummary.Mode.Sum);
    assertEquals(toSketch(agg.getInitialAggregatedValue(null)).getEstimate(), 0.0);
  }

  @Test
  public void applyAggregatedValueShouldUnion() {
    IntegerTupleSketch s1 = new IntegerTupleSketch(16, IntegerSummary.Mode.Sum);
    IntegerTupleSketch s2 = new IntegerTupleSketch(16, IntegerSummary.Mode.Sum);
    s1.update("a", 1);
    s2.update("b", 1);
    IntegerTupleSketchValueAggregator agg =
        new IntegerTupleSketchValueAggregator(List.of(), IntegerSummary.Mode.Sum);
    TupleSketch<IntegerSummary> merged = toSketch(agg.applyAggregatedValue(s1, s2));
    assertEquals(merged.getEstimate(), 2.0);
    assertEquals(agg.getMaxAggregatedValueByteSize(), 196632);
  }

  @Test
  public void applyRawValueShouldUnion() {
    IntegerTupleSketch s1 = new IntegerTupleSketch(16, IntegerSummary.Mode.Sum);
    IntegerTupleSketch s2 = new IntegerTupleSketch(16, IntegerSummary.Mode.Sum);
    s1.update("a", 1);
    s2.update("b", 1);
    IntegerTupleSketchValueAggregator agg =
        new IntegerTupleSketchValueAggregator(List.of(), IntegerSummary.Mode.Sum);
    TupleSketch<IntegerSummary> merged = toSketch(agg.applyRawValue(s1, agg.serializeAggregatedValue(s2)));
    assertEquals(merged.getEstimate(), 2.0);
    assertEquals(agg.getMaxAggregatedValueByteSize(), 196632);
  }

  @SuppressWarnings("unchecked")
  private TupleSketch<IntegerSummary> toSketch(Object value) {
    if (value instanceof TupleUnion) {
      return ((TupleUnion) value).getResult();
    } else if (value instanceof TupleSketch) {
      return ((TupleSketch) value);
    } else {
      throw new IllegalStateException(
          "Unsupported data type for Integer Tuple Sketch aggregation: " + value.getClass().getSimpleName());
    }
  }
}
